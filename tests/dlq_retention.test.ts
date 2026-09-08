/**
 * Dead-letter retention (#247) across the three drivers.
 *
 * The DLQ must not retain full job payloads indefinitely: it is a data-erasure
 * gap and unbounded storage growth. Each driver enforces a retention window —
 * memory sweeps + caps, Deno KV self-expires via `expireIn`, Redis purges
 * opportunistically on write and read. A fake clock (`now`) drives time so a
 * test can age an entry past the window deterministically.
 *
 * @module @lockness/queue/tests/dlq_retention
 */

import { assert, assertEquals } from '@std/assert'
import { MemoryQueueDriver } from '../drivers/memory.ts'
import { DenoKvQueueDriver } from '../drivers/deno_kv.ts'
import { type RedisCommandClient, RedisQueueDriver } from '../drivers/redis.ts'
import {
    DEFAULT_DEAD_LETTER_MAX_ENTRIES,
    DEFAULT_DEAD_LETTER_RETENTION_MS,
} from '../config.ts'
import { FakeKv } from './fake_kv.ts'
import type { SerializedJob } from '../types.ts'

const DAY = 24 * 60 * 60 * 1000

function job(id: string, queue = 'default'): SerializedJob {
    return {
        id,
        name: 'SendEmail',
        payload: { secret: 'DO_NOT_LEAK' },
        attempts: 3,
        maxAttempts: 3,
        delay: 0,
        queue,
        createdAt: 0,
        availableAt: 0,
    }
}

// =============================================================================
// Memory driver
// =============================================================================

Deno.test('memory retention - an entry older than the window is swept on listFailed', async () => {
    let clock = 0
    const d = new MemoryQueueDriver({ retentionMs: 7 * DAY, now: () => clock })

    await d.deadLetter(job('old'), new Error('boom')) // failedAt = 0
    clock = 7 * DAY + 1 // just past the 7-day window
    await d.deadLetter(job('fresh'), new Error('boom')) // failedAt = now

    const failed = await d.listFailed()
    assertEquals(failed.map((e) => e.id), ['fresh'], 'the aged entry is gone')
})

Deno.test('memory retention - the default window applies when unset', async () => {
    let clock = 0
    const d = new MemoryQueueDriver({ now: () => clock }) // retentionMs unset

    await d.deadLetter(job('old'), new Error('boom'))
    // Still inside the default window: retained.
    clock = DEFAULT_DEAD_LETTER_RETENTION_MS - 1
    assertEquals((await d.listFailed()).length, 1, 'kept inside the default')
    // Past the default window: swept.
    clock = DEFAULT_DEAD_LETTER_RETENTION_MS + 1
    assertEquals((await d.listFailed()).length, 0, 'swept past the default')
})

Deno.test('memory retention - the size cap evicts the oldest entry', async () => {
    let clock = 0
    const d = new MemoryQueueDriver({
        retentionMs: 365 * DAY, // long, so only the cap can evict
        maxEntries: 2,
        now: () => clock,
    })

    await d.deadLetter(job('a'), new Error('boom')) // failedAt 0
    clock = 10
    await d.deadLetter(job('b'), new Error('boom')) // failedAt 10
    clock = 20
    await d.deadLetter(job('c'), new Error('boom')) // failedAt 20 → evicts 'a'

    const ids = (await d.listFailed()).map((e) => e.id).sort()
    assertEquals(ids, ['b', 'c'], 'the oldest (a) was evicted at the cap')
})

Deno.test('memory retention - the default cap is a positive bound', () => {
    assert(
        DEFAULT_DEAD_LETTER_MAX_ENTRIES > 0,
        'a sensible default cap keeps the in-memory DLQ bounded',
    )
})

// =============================================================================
// Deno KV driver
// =============================================================================

async function withFakeKv(fn: (fake: FakeKv) => Promise<void>): Promise<void> {
    const fake = new FakeKv()
    const realOpenKv = Deno.openKv
    Object.defineProperty(Deno, 'openKv', {
        configurable: true,
        value: () => Promise.resolve(fake as unknown as Deno.Kv),
    })
    try {
        await fn(fake)
    } finally {
        Object.defineProperty(Deno, 'openKv', {
            configurable: true,
            value: realOpenKv,
        })
    }
}

Deno.test('deno-kv retention - deadLetter writes the entry with expireIn = retentionMs', async () => {
    await withFakeKv(async (fake) => {
        const d = new DenoKvQueueDriver(undefined, { retentionMs: 7 * DAY })
        await d.deadLetter(job('k1'), new Error('boom'))
        assertEquals(fake.dlqEntry('k1')?.expireInMs, 7 * DAY)
        await d.close()
    })
})

Deno.test('deno-kv retention - the default window applies when unset', async () => {
    await withFakeKv(async (fake) => {
        const d = new DenoKvQueueDriver() // no retention configured
        await d.deadLetter(job('k2'), new Error('boom'))
        assertEquals(
            fake.dlqEntry('k2')?.expireInMs,
            DEFAULT_DEAD_LETTER_RETENTION_MS,
        )
        await d.close()
    })
})

// =============================================================================
// Redis driver
// =============================================================================

type Reply = { type: string; value?: string | number | readonly Reply[] }

/** A minimal Redis hash double — only what deadLetter/listFailed/purge touch. */
class FakeRedisHash implements RedisCommandClient {
    #hash = new Map<string, string>()
    readonly commands: string[] = []

    command(...args: string[]): Promise<Reply> {
        const [cmd, ...rest] = args
        this.commands.push(cmd)
        switch (cmd) {
            case 'HSET': {
                const [, field, val] = rest
                this.#hash.set(field, val)
                return Promise.resolve({ type: 'integer', value: 1 })
            }
            case 'HVALS':
                return Promise.resolve({
                    type: 'array',
                    value: [...this.#hash.values()].map((v) => ({
                        type: 'bulk',
                        value: v,
                    })),
                })
            case 'HDEL':
                this.#hash.delete(rest[1])
                return Promise.resolve({ type: 'integer', value: 1 })
        }
        return Promise.resolve({ type: 'nil' })
    }

    fields(): string[] {
        return [...this.#hash.keys()]
    }
}

Deno.test('redis retention - deadLetter purges entries past the window opportunistically', async () => {
    let clock = 0
    const redis = new FakeRedisHash()
    const d = new RedisQueueDriver(redis, {
        retentionMs: 7 * DAY,
        now: () => clock,
    })

    await d.deadLetter(job('old'), new Error('boom')) // failedAt 0
    clock = 7 * DAY + 1
    await d.deadLetter(job('fresh'), new Error('boom')) // triggers purge of old

    assertEquals(
        redis.fields(),
        ['fresh'],
        'the aged entry was purged on write',
    )
})

Deno.test('redis retention - listFailed drops entries past the window', async () => {
    let clock = 0
    const redis = new FakeRedisHash()
    const d = new RedisQueueDriver(redis, {
        retentionMs: 7 * DAY,
        now: () => clock,
    })

    await d.deadLetter(job('old'), new Error('boom'))
    clock = 7 * DAY + 1

    const failed = await d.listFailed()
    assertEquals(failed.length, 0, 'the aged entry is not listed')
    assertEquals(redis.fields(), [], 'and it was purged from the hash')
})

Deno.test('redis retention - the default window applies when unset', async () => {
    let clock = 0
    const redis = new FakeRedisHash()
    const d = new RedisQueueDriver(redis, { now: () => clock }) // retention unset

    await d.deadLetter(job('old'), new Error('boom'))
    clock = DEFAULT_DEAD_LETTER_RETENTION_MS - 1
    assertEquals((await d.listFailed()).length, 1, 'kept inside the default')
    clock = DEFAULT_DEAD_LETTER_RETENTION_MS + 1
    assertEquals((await d.listFailed()).length, 0, 'swept past the default')
})
