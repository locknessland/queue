/**
 * @fileoverview Public type vocabulary for the Lockness queue system.
 *
 * The job, driver and configuration contracts shared across the package. Kept
 * dependency-free so drivers, the registry, the worker and the dispatch
 * helpers can all import from here without a cycle.
 *
 * @module @lockness/queue/types
 */

export interface JobPayload {
    /**
     * Arbitrary, JSON-serialisable job data keyed by name. Values are typed as
     * `unknown` because they cross a serialisation boundary; a job narrows them
     * with its own typed payload interface.
     */
    [key: string]: unknown
}

export interface SerializedJob {
    /** Unique id for this enqueued instance, assigned at dispatch. */
    id: string
    /** Registered job class name, used to resolve the handler on pop. */
    name: string
    /** The job's serialised payload data. */
    payload: JobPayload
    /** Number of times this job has been attempted so far. */
    attempts: number
    /** Maximum attempts before the job is dead-lettered. */
    maxAttempts: number
    /** Delay in milliseconds requested at dispatch before first availability. */
    delay: number
    /** Name of the queue this job belongs to. */
    queue: string
    /** When the job was enqueued (epoch milliseconds). */
    createdAt: number
    /** Earliest time the job may be popped (epoch milliseconds); enforces `delay` and retry backoff. */
    availableAt: number
}

export interface Job<T extends JobPayload = JobPayload> {
    /** Unique job name (used for dispatching) */
    name: string
    /** Maximum retry attempts */
    maxAttempts?: number
    /** Job payload data */
    payload: T
    /** Handle the job */
    handle(payload: T): Promise<void>
    /** Called when job fails after all retries */
    failed?(payload: T, error: Error): Promise<void>
}

export type JobClass<T extends JobPayload = JobPayload> = new (
    payload: T,
) => Job<T>

export interface QueueConfig {
    /** Default queue driver */
    driver: 'memory' | 'deno-kv' | 'redis'
    /** Default queue name */
    defaultQueue: string
    /** Deno KV path (optional) */
    kvPath?: string
    /** Redis connection (for the `'redis'` driver) */
    redis?: {
        hostname: string
        port?: number
        password?: string
        db?: number
        tls?: boolean
    }
    /** Job retry delay in ms (the base delay; see `backoff`) */
    retryDelay: number
    /**
     * How the delay before a retry grows with the attempt number.
     * - `'fixed'` — always `retryDelay` (unchanged behaviour).
     * - `'exponential'` — `retryDelay * 2^(attempt-1)`.
     * @default 'fixed'
     */
    backoff?: 'fixed' | 'exponential'
    /**
     * How long a dead-lettered job is retained before it is purged, in
     * milliseconds (#247). A failed job's payload is sensitive data, so it must
     * not sit in the dead-letter store forever: leaving it there is both a
     * data-erasure gap and unbounded storage growth in a long-running
     * deployment. Every driver enforces this window — the Deno KV driver writes
     * dead-letter entries with an `expireIn` so they self-expire, and the memory
     * and Redis drivers purge entries older than the window opportunistically on
     * write and on `listFailed`.
     * @default 1209600000 (14 days)
     */
    deadLetterRetentionMs?: number
    /**
     * Upper bound on the number of entries kept in the **in-memory** dead-letter
     * store (#247). The in-memory driver has no external store to expire keys
     * for it, so it is bounded by count as well as age: once the count is
     * exceeded, the oldest entry is evicted. Ignored by the durable drivers,
     * which are bounded by the retention window alone.
     * @default 10000
     */
    deadLetterMaxEntries?: number
}

/**
 * Per-driver dead-letter retention controls (#247).
 *
 * Passed to a driver's constructor by the queue manager (derived from
 * {@link QueueConfig}) or directly when a driver is constructed by hand. Every
 * field is optional; a driver falls back to the documented default when a field
 * is unset, so `{}` is a valid, fully-defaulted argument.
 */
export interface DeadLetterRetentionOptions {
    /**
     * Retention window in milliseconds. Entries older than this are purged.
     * @default 1209600000 (14 days)
     */
    readonly retentionMs?: number
    /**
     * Upper bound on entries in the in-memory store (oldest evicted first).
     * Only the memory driver honours this; durable drivers ignore it.
     * @default 10000
     */
    readonly maxEntries?: number
    /**
     * Clock source, injectable so a test can age an entry past the window
     * deterministically. Production leaves it unset and `Date.now` is used.
     * @default () => Date.now()
     */
    readonly now?: () => number
}

/**
 * A dead-lettered job, projected for listing (#220, SEC-F8).
 *
 * Deliberately **without the payload**: `listFailed` returns these to an
 * operator by default, and a failed job's payload is exactly the sensitive data
 * that should not be dumped to a console or log. `retryFailed` re-enqueues the
 * full stored job by id.
 */
export interface DeadLetterEntry {
    /** The dead job's id. */
    readonly id: string
    /** The job class name. */
    readonly name: string
    /** The queue it failed on. */
    readonly queue: string
    /** How many attempts were made before it was dead-lettered. */
    readonly attempts: number
    /** When it was dead-lettered (epoch ms). */
    readonly failedAt: number
    /** The failure's error class name — not its full message/payload. */
    readonly error: string
}

export interface QueueDriver {
    /** Push a job to the queue */
    push(job: SerializedJob): Promise<void>
    /** Pop the next available job */
    pop(queue: string): Promise<SerializedJob | null>
    /** Mark job as completed */
    complete(job: SerializedJob): Promise<void>
    /**
     * Re-enqueue a job for another attempt. The worker owns the terminal
     * decision and only calls this while attempts remain, having already set the
     * job's `attempts` and `availableAt`; the driver just persists it. It no
     * longer drops a job on exhaustion — that path is `deadLetter` (#220).
     */
    fail(job: SerializedJob, error: Error): Promise<void>
    /**
     * Move a job that has exhausted its attempts into the durable dead-letter
     * store, instead of dropping it. Called by the worker at exhaustion (#220).
     */
    deadLetter(job: SerializedJob, error: Error): Promise<void>
    /**
     * List dead-lettered jobs (projected, no payload — SEC-F8). Optionally
     * filtered to one queue.
     */
    listFailed(queue?: string): Promise<DeadLetterEntry[]>
    /**
     * Re-enqueue a dead-lettered job by id, resetting its attempts. Returns
     * `true` if the id was found in the dead-letter store.
     */
    retryFailed(id: string): Promise<boolean>
    /** Get queue size */
    size(queue: string): Promise<number>
    /** Clear all jobs from queue */
    clear(queue: string): Promise<void>
}

export interface DispatchOptions {
    /** Queue name */
    queue?: string
    /** Delay in milliseconds before job is available */
    delay?: number
}

export interface WorkerOptions {
    /** Queue(s) to process */
    queues?: string[]
    /** Sleep time in ms when no jobs available */
    sleep?: number
    /** Maximum jobs to process (0 = unlimited) */
    maxJobs?: number
    /** Stop after queue is empty */
    stopWhenEmpty?: boolean
}
