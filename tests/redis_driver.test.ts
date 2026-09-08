/**
 * Durable Redis queue driver (#220) against an in-memory RESP double modelling a
 * sorted set (the queue) and a hash (the dead-letter store).
 *
 * @module @lockness/queue/tests/redis_driver
 */

import { assert, assertEquals } from '@std/assert'
import { type RedisCommandClient, RedisQueueDriver } from '../drivers/redis.ts'
import type { SerializedJob } from '../types.ts'

type Reply = {
    type: string
    value?: string | number | readonly Reply[]
}

/**
 * Faithful model of Redis's bundled lua-cjson round-trip: `cjson.decode`
 * collapses both JSON `[]` and `{}` into the same empty Lua table, and
 * `cjson.encode` emits that table as `{}`. So any empty collection that passes
 * through a `cjson.decode` → `cjson.encode` cycle comes back as an empty object
 * — the exact corruption #269 fixes. Non-empty collections and scalars survive
 * untouched. The RETRY_SCRIPT emulation runs the parts it re-encodes through
 * this so the defect is reproduced end-to-end.
 */
function cjsonRoundTrip(value: unknown): unknown {
    if (Array.isArray(value)) {
        return value.length === 0 ? {} : value.map(cjsonRoundTrip)
    }
    if (value !== null && typeof value === 'object') {
        const entries = Object.entries(value as Record<string, unknown>)
        if (entries.length === 0) return {}
        const out: Record<string, unknown> = {}
        for (const [k, v] of entries) out[k] = cjsonRoundTrip(v)
        return out
    }
    return value
}

/** A minimal Redis: a sorted set per key + a hash per key. */
class FakeRedis implements RedisCommandClient {
    #zset = new Map<string, { score: number; member: string }[]>()
    #hash = new Map<string, Map<string, string>>()
    /** Every command name issued, in order — lets a test assert atomicity. */
    readonly commands: string[] = []

    command(...args: string[]): Promise<Reply> {
        const [cmd, ...rest] = args
        this.commands.push(cmd)
        switch (cmd) {
            case 'ZADD': {
                const [key, score, member] = rest
                const arr = this.#zset.get(key) ?? []
                arr.push({ score: Number(score), member })
                arr.sort((a, b) => a.score - b.score)
                this.#zset.set(key, arr)
                return Promise.resolve({ type: 'integer', value: 1 })
            }
            case 'EVAL': {
                const script = rest[0]
                if (script.includes('ZRANGEBYSCORE')) {
                    // POP_SCRIPT: <script> 1 <key> <now>
                    const key = rest[2]
                    const now = Number(rest[3])
                    const arr = this.#zset.get(key) ?? []
                    const idx = arr.findIndex((e) => e.score <= now)
                    if (idx === -1) return Promise.resolve({ type: 'nil' })
                    const [claimed] = arr.splice(idx, 1)
                    return Promise.resolve({
                        type: 'bulk',
                        value: claimed.member,
                    })
                }
                // RETRY_SCRIPT: <script> 1 <dlqKey> <id> <now> <queuePrefix>
                const [dlqKey, id, now, prefix] = rest.slice(2)
                const raw = this.#hash.get(dlqKey)?.get(id)
                if (raw === undefined) {
                    return Promise.resolve({ type: 'integer', value: 0 })
                }
                const entry = JSON.parse(raw) as {
                    job: Record<string, unknown>
                    payload?: string
                }
                // Mirror the Lua: the job object (attempts/availableAt reset) is
                // the only thing re-encoded through cjson. In the current wire
                // shape the payload rides alongside as an opaque JSON string the
                // script never decodes, so it is spliced back verbatim; a legacy
                // entry carries the payload nested in the job, so it goes through
                // the lossy round-trip.
                const jobPart = {
                    ...entry.job,
                    attempts: 0,
                    availableAt: Number(now),
                }
                const encoded = cjsonRoundTrip(jobPart) as Record<
                    string,
                    unknown
                >
                const member = entry.payload !== undefined
                    ? JSON.stringify({
                        ...encoded,
                        payload: JSON.parse(entry.payload),
                    })
                    : JSON.stringify(encoded)
                const queue = String(entry.job.queue)
                const qkey = `${prefix}${queue}`
                const arr = this.#zset.get(qkey) ?? []
                arr.push({
                    score: Number(now),
                    member,
                })
                arr.sort((a, b) => a.score - b.score)
                this.#zset.set(qkey, arr)
                this.#hash.get(dlqKey)?.delete(id)
                return Promise.resolve({ type: 'integer', value: 1 })
            }
            case 'ZCARD':
                return Promise.resolve({
                    type: 'integer',
                    value: (this.#zset.get(rest[0]) ?? []).length,
                })
            case 'DEL':
                this.#zset.delete(rest[0])
                return Promise.resolve({ type: 'integer', value: 1 })
            case 'HSET': {
                const [key, field, val] = rest
                const h = this.#hash.get(key) ?? new Map<string, string>()
                h.set(field, val)
                this.#hash.set(key, h)
                return Promise.resolve({ type: 'integer', value: 1 })
            }
            case 'HVALS': {
                const h = this.#hash.get(rest[0]) ?? new Map<string, string>()
                return Promise.resolve({
                    type: 'array',
                    value: [...h.values()].map((v) => ({
                        type: 'bulk',
                        value: v,
                    })),
                })
            }
            case 'HGET': {
                const v = this.#hash.get(rest[0])?.get(rest[1])
                return Promise.resolve(
                    v === undefined
                        ? { type: 'nil' }
                        : { type: 'bulk', value: v },
                )
            }
            case 'HDEL':
                this.#hash.get(rest[0])?.delete(rest[1])
                return Promise.resolve({ type: 'integer', value: 1 })
        }
        return Promise.resolve({ type: 'nil' })
    }
}

function job(id: string, availableAt: number): SerializedJob {
    return {
        id,
        name: 'SendEmail',
        payload: { secret: 'DO_NOT_LEAK' },
        attempts: 0,
        maxAttempts: 3,
        delay: 0,
        queue: 'default',
        createdAt: 0,
        availableAt,
    }
}

Deno.test('RedisQueueDriver - push then pop returns the due job; a delayed job waits', async () => {
    const d = new RedisQueueDriver(new FakeRedis())
    await d.push(job('due', 0)) // available now
    await d.push(job('later', Date.now() + 3_600_000)) // an hour out

    const first = await d.pop('default')
    assertEquals(first?.id, 'due')
    // The delayed job is not yet due.
    assertEquals(await d.pop('default'), null)
    assertEquals(await d.size('default'), 1, 'the delayed job is still queued')
})

Deno.test('RedisQueueDriver - pop on an empty queue is null', async () => {
    const d = new RedisQueueDriver(new FakeRedis())
    assertEquals(await d.pop('default'), null)
})

Deno.test('RedisQueueDriver - deadLetter → listFailed (no payload) → retryFailed round-trip', async () => {
    const d = new RedisQueueDriver(new FakeRedis())
    await d.deadLetter(job('dead', 0), new RangeError('nope'))

    const failed = await d.listFailed()
    assertEquals(failed.length, 1)
    assertEquals(failed[0].id, 'dead')
    assertEquals(failed[0].error, 'RangeError')
    assert(
        !JSON.stringify(failed).includes('DO_NOT_LEAK'),
        'listFailed must not expose the payload',
    )

    assertEquals(await d.retryFailed('dead'), true)
    assertEquals((await d.listFailed()).length, 0, 'removed from the DLQ')
    const revived = await d.pop('default')
    assertEquals(revived?.id, 'dead', 're-enqueued and immediately due')
})

Deno.test('RedisQueueDriver - retryFailed preserves empty-array and empty-object payload fields (#269)', async () => {
    const d = new RedisQueueDriver(new FakeRedis())
    const dead: SerializedJob = {
        id: 'j1',
        name: 'SendReport',
        payload: {
            tags: [],
            meta: {},
            recipients: ['a@b.co'],
            filters: { archived: [] },
            count: 3,
        },
        attempts: 3,
        maxAttempts: 3,
        delay: 0,
        queue: 'default',
        createdAt: 0,
        availableAt: 0,
    }
    await d.deadLetter(dead, new Error('boom'))

    assertEquals(await d.retryFailed('j1'), true)

    const revived = await d.pop('default')
    assert(revived, 'the retried job is immediately due')
    // Byte-equivalent payload: empty [] stays [], empty {} stays {}.
    assertEquals(revived.payload, {
        tags: [],
        meta: {},
        recipients: ['a@b.co'],
        filters: { archived: [] },
        count: 3,
    })
    assert(
        Array.isArray(revived.payload.tags),
        'the empty array must not become an object',
    )
    assert(
        !Array.isArray(revived.payload.filters) &&
            Array.isArray(
                (revived.payload.filters as { archived: unknown }).archived,
            ),
        'a nested empty array must not become an object',
    )
    // Retry semantics still hold: fresh attempt count.
    assertEquals(revived.attempts, 0)
})

Deno.test('RedisQueueDriver - retryFailed still revives a legacy DLQ entry (payload nested in the job)', async () => {
    const redis = new FakeRedis()
    const d = new RedisQueueDriver(redis)
    // Plant an entry in the pre-#269 wire shape: the whole job, payload and
    // all, nested under `job` with no sibling opaque `payload` string.
    const legacyJob = job('legacy', 0)
    await redis.command(
        'HSET',
        'lockness:queue:dlq',
        'legacy',
        JSON.stringify({ job: legacyJob, error: 'RangeError', failedAt: 0 }),
    )

    assertEquals(await d.retryFailed('legacy'), true)
    const revived = await d.pop('default')
    assertEquals(revived?.id, 'legacy', 'a legacy entry still re-enqueues')
    assertEquals(revived?.payload, { secret: 'DO_NOT_LEAK' })
})

Deno.test('RedisQueueDriver - retryFailed on an unknown id is false', async () => {
    const d = new RedisQueueDriver(new FakeRedis())
    assertEquals(await d.retryFailed('ghost'), false)
})

Deno.test('RedisQueueDriver - retryFailed is a single atomic EVAL (no HGET/ZADD/HDEL round-trips)', async () => {
    const redis = new FakeRedis()
    const d = new RedisQueueDriver(redis)
    await d.deadLetter(job('dead', 0), new RangeError('nope'))
    redis.commands.length = 0 // ignore the deadLetter HSET

    assertEquals(await d.retryFailed('dead'), true)
    assertEquals(redis.commands, ['EVAL'], 'retry folds into one EVAL')
})

Deno.test('RedisQueueDriver - retryFailed on a missing id issues one EVAL and touches nothing', async () => {
    const redis = new FakeRedis()
    const d = new RedisQueueDriver(redis)
    redis.commands.length = 0

    assertEquals(await d.retryFailed('ghost'), false)
    assertEquals(redis.commands, ['EVAL'], 'a miss is still one atomic EVAL')
    assertEquals(await d.size('default'), 0, 'nothing enqueued on a miss')
})

Deno.test('RedisQueueDriver - clear empties the queue', async () => {
    const d = new RedisQueueDriver(new FakeRedis())
    await d.push(job('a', 0))
    await d.clear('default')
    assertEquals(await d.size('default'), 0)
})
