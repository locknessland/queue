/**
 * @fileoverview Durable Redis queue driver (#220).
 *
 * A queue is a Redis **sorted set** keyed by `availableAt`, so a delayed or
 * retried job is simply a member with a future score; `pop` claims the earliest
 * due member with an atomic Lua `ZRANGEBYSCORE … ZREM`, so two workers never
 * take the same job. The dead-letter store is a Redis **hash** keyed by job id,
 * holding the whole job plus its failure — durable across restarts, and the
 * source `queue:retry` re-enqueues from.
 *
 * The driver owns no connection itself: it takes a `RedisCommandClient` (the
 * `@lockness/redis` `RedisClient` satisfies it), so a test can drive it against
 * an in-memory RESP double and the composition root owns the real socket.
 *
 * @module @lockness/queue/drivers/redis
 */

import type {
    DeadLetterEntry,
    DeadLetterRetentionOptions,
    QueueDriver,
    SerializedJob,
} from '../types.ts'
import { DEFAULT_DEAD_LETTER_RETENTION_MS } from '../config.ts'

/** One parsed RESP reply — the structural slice this driver reads. */
interface CommandReply {
    readonly type: string
    readonly value?: string | number | readonly CommandReply[]
}

/**
 * The minimal Redis client surface this driver needs. `@lockness/redis`'s
 * `RedisClient` satisfies it; a test passes a fake.
 */
export interface RedisCommandClient {
    command(...args: string[]): Promise<CommandReply>
}

/** Shared prefix for every queue sorted-set key. */
const QUEUE_PREFIX = 'lockness:queue:'

/** The queue sorted-set key. */
function queueKey(name: string): string {
    return `${QUEUE_PREFIX}${name}`
}

/** The single dead-letter hash key. */
const DLQ_KEY = 'lockness:queue:dlq'

/** Atomic claim: take the earliest member whose score (availableAt) is due. */
const POP_SCRIPT =
    "local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 1); " +
    'if #due == 0 then return nil end; ' +
    "redis.call('ZREM', KEYS[1], due[1]); return due[1]"

/**
 * Atomic retry: read the dead-letter entry, re-enqueue its job at score `now`
 * with a fresh attempt count, and drop the dead-letter copy — all in one server
 * round-trip so a crash can never leave the job both queued and dead-lettered.
 *
 * The job's `payload` is **never** `cjson`-decoded here. Redis's bundled
 * lua-cjson collapses both JSON `[]` and `{}` into one empty Lua table and
 * re-encodes it as `{}`, so round-tripping the whole job would silently rewrite
 * an empty-array payload field into an empty object (#269). Instead
 * {@link RedisQueueDriver.deadLetter} stores the payload as an opaque
 * pre-serialised JSON string (`entry.payload`) beside a payload-free `entry.job`
 * of scalars only; the script mutates just those scalars, re-encodes the small
 * scalar object, then splices the untouched payload string back in by string
 * surgery. Legacy entries written before #269 carry the payload nested in
 * `entry.job` with no sibling `entry.payload`; those still take the old lossy
 * path so they remain retryable.
 *
 * `KEYS[1]` is the dead-letter hash; `ARGV[1]` the job id, `ARGV[2]` the new
 * `availableAt` score, `ARGV[3]` the queue key prefix. Returns `1` on a revive
 * and `0` when the id is absent (so nothing is enqueued).
 */
const RETRY_SCRIPT = "local raw = redis.call('HGET', KEYS[1], ARGV[1]); " +
    'if not raw then return 0 end; ' +
    'local entry = cjson.decode(raw); ' +
    'local job = entry.job; ' +
    'job.attempts = 0; ' +
    'job.availableAt = tonumber(ARGV[2]); ' +
    'local member; ' +
    'if entry.payload ~= nil then ' +
    'local head = cjson.encode(job); ' +
    "member = string.sub(head, 1, -2) .. ',\"payload\":' .. entry.payload .. '}'; " +
    'else member = cjson.encode(job); end; ' +
    "redis.call('ZADD', ARGV[3] .. job.queue, ARGV[2], member); " +
    "redis.call('HDEL', KEYS[1], ARGV[1]); return 1"

/** A durable {@link QueueDriver} over Redis. */
export class RedisQueueDriver implements QueueDriver {
    readonly #client: RedisCommandClient
    readonly #retentionMs: number
    readonly #now: () => number

    /**
     * @param client - The Redis command surface (the `@lockness/redis`
     * `RedisClient` satisfies it; a test passes a fake).
     * @param options - Dead-letter retention controls; `retentionMs` sizes the
     * purge window and `now` is an injectable clock. `maxEntries` is ignored —
     * the Redis store is bounded by age alone. Defaults when unset (#247).
     */
    constructor(
        client: RedisCommandClient,
        options: DeadLetterRetentionOptions = {},
    ) {
        this.#client = client
        this.#retentionMs = options.retentionMs ??
            DEFAULT_DEAD_LETTER_RETENTION_MS
        this.#now = options.now ?? (() => Date.now())
    }

    async push(job: SerializedJob): Promise<void> {
        await this.#client.command(
            'ZADD',
            queueKey(job.queue),
            String(job.availableAt),
            JSON.stringify(job),
        )
    }

    async pop(queueName: string): Promise<SerializedJob | null> {
        const reply = await this.#client.command(
            'EVAL',
            POP_SCRIPT,
            '1',
            queueKey(queueName),
            String(Date.now()),
        )
        if (reply.type !== 'bulk' || typeof reply.value !== 'string') {
            return null
        }
        return JSON.parse(reply.value) as SerializedJob
    }

    async complete(_job: SerializedJob): Promise<void> {
        // The job was removed from the sorted set atomically in pop().
        await Promise.resolve()
    }

    async fail(job: SerializedJob, _error: Error): Promise<void> {
        // The worker set attempts + availableAt and decided a retry remains; the
        // driver just re-adds it at its new score. Exhaustion goes to deadLetter.
        await this.push(job)
    }

    async deadLetter(job: SerializedJob, error: Error): Promise<void> {
        const now = this.#now()
        // Split the payload out as an opaque pre-serialised JSON string so the
        // atomic retry script (RETRY_SCRIPT) never has to cjson-decode it —
        // lua-cjson would rewrite an empty-array payload field into an empty
        // object on the round-trip (#269). What is left under `job` is scalars
        // only, safe to re-encode. `listFailed` and the purge read only those
        // scalars, so they are unaffected by the split.
        const { payload, ...scalars } = job
        await this.#client.command(
            'HSET',
            DLQ_KEY,
            job.id,
            JSON.stringify({
                job: scalars,
                payload: JSON.stringify(payload),
                error: error.name,
                failedAt: now,
            }),
        )
        // Per-field hash TTL is not portable across Redis versions, so instead
        // of a native TTL the store carries each entry's `failedAt` and is
        // purged opportunistically: dead-lettering is a rare error-path event,
        // so an O(n) sweep here keeps the hash bounded even with no reads (#247).
        await this.#purgeExpired(now)
    }

    async listFailed(queueName?: string): Promise<DeadLetterEntry[]> {
        const cutoff = this.#now() - this.#retentionMs
        const reply = await this.#client.command('HVALS', DLQ_KEY)
        const values: readonly CommandReply[] = reply.type === 'array' &&
                Array.isArray(reply.value)
            ? reply.value
            : []
        const out: DeadLetterEntry[] = []
        for (const v of values) {
            if (v.type !== 'bulk' || typeof v.value !== 'string') continue
            // Only scalar job fields are read here; the payload (opaque string
            // in the #269 shape, nested object in a legacy entry) is never
            // touched, so `Omit<…, 'payload'>` describes both shapes.
            const { job, error, failedAt } = JSON.parse(v.value) as {
                job: Omit<SerializedJob, 'payload'>
                error: string
                failedAt: number
            }
            // Purge on read too, so an aged entry is never surfaced (#247).
            if (failedAt < cutoff) {
                await this.#client.command('HDEL', DLQ_KEY, job.id)
                continue
            }
            if (queueName !== undefined && job.queue !== queueName) continue
            out.push({
                id: job.id,
                name: job.name,
                queue: job.queue,
                attempts: job.attempts,
                failedAt,
                error,
            })
        }
        return out
    }

    /**
     * Delete every dead-letter entry older than the retention window.
     *
     * Reads the whole hash (`HVALS`) and `HDEL`s each entry whose `failedAt`
     * predates `now - retentionMs`. Kept separate from the atomic retry script
     * so purge never interferes with `retryFailed` (#247).
     *
     * @param now - Current epoch ms (the injected clock's value).
     */
    async #purgeExpired(now: number): Promise<void> {
        const cutoff = now - this.#retentionMs
        const reply = await this.#client.command('HVALS', DLQ_KEY)
        const values: readonly CommandReply[] = reply.type === 'array' &&
                Array.isArray(reply.value)
            ? reply.value
            : []
        for (const v of values) {
            if (v.type !== 'bulk' || typeof v.value !== 'string') continue
            const { job, failedAt } = JSON.parse(v.value) as {
                job: Pick<SerializedJob, 'id'>
                failedAt: number
            }
            if (failedAt < cutoff) {
                await this.#client.command('HDEL', DLQ_KEY, job.id)
            }
        }
    }

    async retryFailed(id: string): Promise<boolean> {
        const reply = await this.#client.command(
            'EVAL',
            RETRY_SCRIPT,
            '1',
            DLQ_KEY,
            id,
            String(Date.now()),
            QUEUE_PREFIX,
        )
        return reply.type === 'integer' && reply.value === 1
    }

    async size(queueName: string): Promise<number> {
        const reply = await this.#client.command('ZCARD', queueKey(queueName))
        return reply.type === 'integer' && typeof reply.value === 'number'
            ? reply.value
            : 0
    }

    async clear(queueName: string): Promise<void> {
        await this.#client.command('DEL', queueKey(queueName))
    }
}
