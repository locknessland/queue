/**
 * @fileoverview In-memory queue driver.
 *
 * A process-local `Map` of named queues plus a per-instance dead-letter store.
 * The default driver, and the one the tests exercise; holds no external
 * resources.
 *
 * The dead-letter store is **bounded** (#247): with no external store to expire
 * keys for it, the driver enforces retention itself, purging entries older than
 * the configured window and evicting the oldest once a count cap is exceeded.
 *
 * @module @lockness/queue/drivers/memory
 */

import type {
    DeadLetterEntry,
    DeadLetterRetentionOptions,
    QueueDriver,
    SerializedJob,
} from '../types.ts'
import {
    DEFAULT_DEAD_LETTER_MAX_ENTRIES,
    DEFAULT_DEAD_LETTER_RETENTION_MS,
} from '../config.ts'

const memoryQueues = new Map<string, SerializedJob[]>()

/** One dead-lettered job, held whole (the projection is built on listing). */
interface DeadEntry {
    readonly job: SerializedJob
    readonly error: string
    readonly failedAt: number
}

/** A process-local {@link QueueDriver} backed by in-memory maps. */
export class MemoryQueueDriver implements QueueDriver {
    /** This driver's dead-letter store, keyed by job id. */
    readonly #deadLetters = new Map<string, DeadEntry>()
    readonly #retentionMs: number
    readonly #maxEntries: number
    readonly #now: () => number

    /**
     * @param options - Dead-letter retention controls; each field defaults when
     * unset, so `new MemoryQueueDriver()` uses the framework defaults (#247).
     * @example
     * ```typescript
     * const driver = new MemoryQueueDriver({ retentionMs: 7 * 24 * 3600_000 })
     * ```
     */
    constructor(options: DeadLetterRetentionOptions = {}) {
        this.#retentionMs = options.retentionMs ??
            DEFAULT_DEAD_LETTER_RETENTION_MS
        this.#maxEntries = options.maxEntries ?? DEFAULT_DEAD_LETTER_MAX_ENTRIES
        this.#now = options.now ?? (() => Date.now())
    }

    private getQueue(name: string): SerializedJob[] {
        if (!memoryQueues.has(name)) {
            memoryQueues.set(name, [])
        }
        return memoryQueues.get(name)!
    }

    push(job: SerializedJob): Promise<void> {
        const queue = this.getQueue(job.queue)
        queue.push(job)
        return Promise.resolve()
    }

    pop(queueName: string): Promise<SerializedJob | null> {
        const queue = this.getQueue(queueName)
        const now = Date.now()

        // Find first available job (not delayed)
        const index = queue.findIndex((j) => j.availableAt <= now)
        if (index === -1) return Promise.resolve(null)

        return Promise.resolve(queue.splice(index, 1)[0])
    }

    async complete(_job: SerializedJob): Promise<void> {
        // Job already removed from queue in pop()
        await Promise.resolve()
    }

    fail(job: SerializedJob, _error: Error): Promise<void> {
        // The worker has already set attempts + availableAt and decided a retry
        // remains; the driver just re-persists it. Exhaustion goes to
        // deadLetter, never a silent drop.
        this.getQueue(job.queue).push(job)
        return Promise.resolve()
    }

    deadLetter(job: SerializedJob, error: Error): Promise<void> {
        const failedAt = this.#now()
        this.#deadLetters.set(job.id, { job, error: error.name, failedAt })
        // Keep the store bounded on every write: purge aged entries, then cap
        // the count. Dead-lettering is a rare error-path event, so this is cheap.
        this.#sweepExpired(failedAt)
        this.#enforceCap()
        return Promise.resolve()
    }

    listFailed(queueName?: string): Promise<DeadLetterEntry[]> {
        // Purge on read too, so an aged entry is never surfaced to an operator.
        this.#sweepExpired(this.#now())
        const out: DeadLetterEntry[] = []
        for (const entry of this.#deadLetters.values()) {
            if (queueName !== undefined && entry.job.queue !== queueName) {
                continue
            }
            out.push({
                id: entry.job.id,
                name: entry.job.name,
                queue: entry.job.queue,
                attempts: entry.job.attempts,
                failedAt: entry.failedAt,
                error: entry.error,
            })
        }
        return Promise.resolve(out)
    }

    retryFailed(id: string): Promise<boolean> {
        const entry = this.#deadLetters.get(id)
        if (entry === undefined) return Promise.resolve(false)
        this.#deadLetters.delete(id)
        const job = { ...entry.job, attempts: 0, availableAt: Date.now() }
        this.getQueue(job.queue).push(job)
        return Promise.resolve(true)
    }

    size(queueName: string): Promise<number> {
        return Promise.resolve(this.getQueue(queueName).length)
    }

    clear(queueName: string): Promise<void> {
        memoryQueues.set(queueName, [])
        return Promise.resolve()
    }

    /** Drop every dead-letter entry older than the retention window. */
    #sweepExpired(now: number): void {
        const cutoff = now - this.#retentionMs
        for (const [id, entry] of this.#deadLetters) {
            if (entry.failedAt < cutoff) this.#deadLetters.delete(id)
        }
    }

    /** Evict the oldest entries until the store is within the count cap. */
    #enforceCap(): void {
        while (this.#deadLetters.size > this.#maxEntries) {
            let oldestId: string | undefined
            let oldestAt = Infinity
            for (const [id, entry] of this.#deadLetters) {
                if (entry.failedAt < oldestAt) {
                    oldestAt = entry.failedAt
                    oldestId = id
                }
            }
            if (oldestId === undefined) break
            this.#deadLetters.delete(oldestId)
        }
    }
}
