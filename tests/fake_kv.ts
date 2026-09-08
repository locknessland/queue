/**
 * @fileoverview An in-memory `Deno.Kv` double for the queue driver tests.
 *
 * The real `Deno.Kv` exposes no way to read back the `expireIn` a write
 * declared, so the dead-letter retention TTL (#247, AC-2) cannot be asserted
 * against it. This double implements only the surface
 * {@link DenoKvQueueDriver} touches — `set` (recording `expireIn`), `get`,
 * `list` (prefix scan), `delete`, `atomic().check().delete().set().commit()`
 * and `close` — and records each write's `expireInMs`.
 *
 * It is never imported by production code; it is stubbed in via
 * `Object.defineProperty(Deno, 'openKv', …)`, the supported way to replace the
 * getter-only `Deno.openKv`.
 *
 * @module @lockness/queue/tests/fake_kv
 */

/** One stored value plus the `expireIn` (ms) the write carried, if any. */
export interface FakeKvEntry {
    /** The stored value. */
    value: unknown
    /** The `expireIn` in milliseconds the write declared, or `undefined`. */
    expireInMs?: number
    /** `Date.now()` at write time — used to honour expiry on read. */
    writtenAt: number
}

/** An op queued on a {@link FakeAtomic} until `commit()`. */
type AtomicOp =
    | { kind: 'set'; key: string; value: unknown; expireInMs?: number }
    | { kind: 'delete'; key: string }

/**
 * The atomic builder returned by {@link FakeKv.atomic}. Mirrors the fluent
 * `Deno.AtomicOperation` shape the queue driver uses: `check` is a no-op in this
 * double (version conflicts are not modelled here), `set` and `delete` queue
 * ops, and `commit` applies them all-or-nothing.
 */
export class FakeAtomic {
    readonly #kv: FakeKv
    readonly #ops: AtomicOp[] = []

    constructor(kv: FakeKv) {
        this.#kv = kv
    }

    /**
     * Record an optimistic check. Not modelled — returns this for chaining.
     * @returns This builder.
     */
    check(): this {
        return this
    }

    /**
     * Queue a set, recording its `expireIn`.
     * @param key - The KV key parts.
     * @param value - The value to store.
     * @param options - Optional `{ expireIn }` in milliseconds.
     * @returns This builder, for chaining.
     */
    set(
        key: readonly unknown[],
        value: unknown,
        options?: { expireIn?: number },
    ): this {
        this.#ops.push({
            kind: 'set',
            key: JSON.stringify(key),
            value,
            expireInMs: options?.expireIn,
        })
        return this
    }

    /**
     * Queue a delete.
     * @param key - The KV key parts.
     * @returns This builder, for chaining.
     */
    delete(key: readonly unknown[]): this {
        this.#ops.push({ kind: 'delete', key: JSON.stringify(key) })
        return this
    }

    /**
     * Apply every queued op.
     * @returns `{ ok: true }`.
     */
    commit(): Promise<{ ok: boolean; versionstamp?: string }> {
        for (const op of this.#ops) {
            if (op.kind === 'set') {
                this.#kv.store.set(op.key, {
                    value: op.value,
                    expireInMs: op.expireInMs,
                    writtenAt: Date.now(),
                })
            } else {
                this.#kv.store.delete(op.key)
            }
        }
        return Promise.resolve({ ok: true, versionstamp: '00000000' })
    }
}

/**
 * An in-memory stand-in for `Deno.Kv` covering the queue driver's surface.
 *
 * @example
 * ```typescript
 * const fake = new FakeKv()
 * const realOpenKv = Deno.openKv
 * Object.defineProperty(Deno, 'openKv', {
 *   configurable: true,
 *   value: () => Promise.resolve(fake as unknown as Deno.Kv),
 * })
 * try {
 *   // …exercise DenoKvQueueDriver…
 * } finally {
 *   Object.defineProperty(Deno, 'openKv', { configurable: true, value: realOpenKv })
 * }
 * ```
 */
export class FakeKv {
    /** The backing store, keyed by the JSON-encoded key parts. */
    readonly store = new Map<string, FakeKvEntry>()

    #key(key: readonly unknown[]): string {
        return JSON.stringify(key)
    }

    /**
     * Read a value, honouring any recorded `expireIn`.
     * @param key - The KV key parts.
     * @returns A `Deno.KvEntryMaybe`-shaped result (`value` is `null` if absent).
     */
    get<T = unknown>(
        key: readonly unknown[],
    ): Promise<
        {
            key: readonly unknown[]
            value: T | null
            versionstamp: string | null
        }
    > {
        const entry = this.store.get(this.#key(key))
        if (!entry || this.#expired(entry)) {
            this.store.delete(this.#key(key))
            return Promise.resolve({ key, value: null, versionstamp: null })
        }
        return Promise.resolve({
            key,
            value: entry.value as T,
            versionstamp: '00000000',
        })
    }

    /**
     * Write a value, recording its `expireIn`.
     * @param key - The KV key parts.
     * @param value - The value to store.
     * @param options - Optional `{ expireIn }` in milliseconds.
     * @returns `{ ok: true }`.
     */
    set(
        key: readonly unknown[],
        value: unknown,
        options?: { expireIn?: number },
    ): Promise<{ ok: true; versionstamp: string }> {
        this.store.set(this.#key(key), {
            value,
            expireInMs: options?.expireIn,
            writtenAt: Date.now(),
        })
        return Promise.resolve({ ok: true, versionstamp: '00000000' })
    }

    /**
     * Delete a value.
     * @param key - The KV key parts.
     */
    delete(key: readonly unknown[]): Promise<void> {
        this.store.delete(this.#key(key))
        return Promise.resolve()
    }

    /**
     * List entries whose key begins with `prefix`, honouring expiry.
     * @param selector - `{ prefix }` — the leading key parts to match.
     * @returns An async iterator of `{ key, value }` entries.
     */
    async *list<T = unknown>(
        selector: { prefix: readonly unknown[] },
    ): AsyncIterableIterator<{ key: readonly unknown[]; value: T }> {
        const prefix = this.#key(selector.prefix).slice(0, -1) // drop closing ]
        for (const [k, entry] of this.store) {
            if (!k.startsWith(prefix)) continue
            if (this.#expired(entry)) {
                this.store.delete(k)
                continue
            }
            yield { key: JSON.parse(k), value: entry.value as T }
        }
    }

    /**
     * Begin an atomic operation.
     * @returns A fresh {@link FakeAtomic}.
     */
    atomic(): FakeAtomic {
        return new FakeAtomic(this)
    }

    /** Close the store (no-op; present for the driver's shutdown path). */
    close(): void {}

    /**
     * Read the recorded entry for a dead-lettered job id, for TTL assertions.
     * @param jobId - The job id whose `['dlq', id]` entry to fetch.
     * @returns The recorded entry, or `undefined` if absent.
     */
    dlqEntry(jobId: string): FakeKvEntry | undefined {
        return this.store.get(this.#key(['dlq', jobId]))
    }

    #expired(entry: FakeKvEntry): boolean {
        return entry.expireInMs !== undefined &&
            Date.now() > entry.writtenAt + entry.expireInMs
    }
}
