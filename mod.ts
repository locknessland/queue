/**
 * Lockness Queue System
 *
 * Background job processing with multiple driver support.
 * Inspired by Laravel's queue system.
 */

// =============================================================================
// Types & Interfaces
// =============================================================================

export interface JobPayload {
    [key: string]: unknown
}

export interface SerializedJob {
    id: string
    name: string
    payload: JobPayload
    attempts: number
    maxAttempts: number
    delay: number
    queue: string
    createdAt: number
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

// deno-lint-ignore no-explicit-any
type AnyJobClass = new (payload: any) => Job<any>

export interface QueueConfig {
    /** Default queue driver */
    driver: 'memory' | 'deno-kv'
    /** Default queue name */
    defaultQueue: string
    /** Deno KV path (optional) */
    kvPath?: string
    /** Job retry delay in ms */
    retryDelay: number
}

export interface QueueDriver {
    /** Push a job to the queue */
    push(job: SerializedJob): Promise<void>
    /** Pop the next available job */
    pop(queue: string): Promise<SerializedJob | null>
    /** Mark job as completed */
    complete(job: SerializedJob): Promise<void>
    /** Mark job as failed and requeue if retries left */
    fail(job: SerializedJob, error: Error): Promise<void>
    /** Get queue size */
    size(queue: string): Promise<number>
    /** Clear all jobs from queue */
    clear(queue: string): Promise<void>
}

// =============================================================================
// Queue Configuration
// =============================================================================

const defaultConfig: QueueConfig = {
    driver: 'memory',
    defaultQueue: 'default',
    retryDelay: 3000,
}

let globalQueueConfig: QueueConfig = { ...defaultConfig }

export function configureQueue(config: Partial<QueueConfig>): void {
    globalQueueConfig = { ...globalQueueConfig, ...config }
}

export function getQueueConfig(): QueueConfig {
    return globalQueueConfig
}

// =============================================================================
// Job Registry
// =============================================================================

const jobRegistry = new Map<string, AnyJobClass>()

/**
 * Register a job class by providing an instance or class with a name
 */
export function registerJob<T extends JobPayload>(jobClass: JobClass<T>): void {
    // Create a dummy instance to get the name
    const instance = new jobClass({} as T)
    jobRegistry.set(instance.name, jobClass as AnyJobClass)
}

/**
 * Get a job class by name
 */
export function getJobClass(name: string): AnyJobClass | undefined {
    return jobRegistry.get(name)
}

// =============================================================================
// Memory Queue Driver
// =============================================================================

const memoryQueues = new Map<string, SerializedJob[]>()

export class MemoryQueueDriver implements QueueDriver {
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
        if (job.attempts < job.maxAttempts) {
            // Requeue with delay
            job.attempts++
            job.availableAt = Date.now() + globalQueueConfig.retryDelay
            const queue = this.getQueue(job.queue)
            queue.push(job)
        }
        return Promise.resolve()
    }

    size(queueName: string): Promise<number> {
        return Promise.resolve(this.getQueue(queueName).length)
    }

    clear(queueName: string): Promise<void> {
        memoryQueues.set(queueName, [])
        return Promise.resolve()
    }
}

// =============================================================================
// Deno KV Queue Driver
// =============================================================================

export class DenoKvQueueDriver implements QueueDriver {
    private kv: Deno.Kv | null = null
    private kvPath?: string

    constructor(kvPath?: string) {
        this.kvPath = kvPath
    }

    private async getKv(): Promise<Deno.Kv> {
        if (!this.kv) {
            this.kv = await Deno.openKv(this.kvPath)
        }
        return this.kv
    }

    async push(job: SerializedJob): Promise<void> {
        const kv = await this.getKv()
        // Use timestamp + id for ordering
        const key = ['queue', job.queue, job.availableAt, job.id]
        await kv.set(key, job)
    }

    async pop(queueName: string): Promise<SerializedJob | null> {
        const kv = await this.getKv()
        const now = Date.now()

        // List jobs in queue ordered by availableAt
        const iter = kv.list<SerializedJob>({
            prefix: ['queue', queueName],
        })

        for await (const entry of iter) {
            const job = entry.value
            if (job.availableAt <= now) {
                // Try to atomically delete and return
                const result = await kv.atomic()
                    .check(entry)
                    .delete(entry.key)
                    .commit()

                if (result.ok) {
                    return job
                }
                // Someone else got it, continue
            }
        }

        return null
    }

    async complete(_job: SerializedJob): Promise<void> {
        // Job already removed in pop()
    }

    async fail(job: SerializedJob, _error: Error): Promise<void> {
        if (job.attempts < job.maxAttempts) {
            job.attempts++
            job.availableAt = Date.now() + globalQueueConfig.retryDelay
            await this.push(job)
        }
    }

    async size(queueName: string): Promise<number> {
        const kv = await this.getKv()
        let count = 0
        const iter = kv.list({ prefix: ['queue', queueName] })
        for await (const _ of iter) {
            count++
        }
        return count
    }

    async clear(queueName: string): Promise<void> {
        const kv = await this.getKv()
        const iter = kv.list({ prefix: ['queue', queueName] })
        for await (const entry of iter) {
            await kv.delete(entry.key)
        }
    }
}

// =============================================================================
// Queue Manager
// =============================================================================

let queueDriver: QueueDriver | null = null

function getDriver(): QueueDriver {
    if (!queueDriver) {
        const config = globalQueueConfig
        switch (config.driver) {
            case 'deno-kv':
                queueDriver = new DenoKvQueueDriver(config.kvPath)
                break
            case 'memory':
            default:
                queueDriver = new MemoryQueueDriver()
                break
        }
    }
    return queueDriver
}

/**
 * Set a custom queue driver
 */
export function setQueueDriver(driver: QueueDriver): void {
    queueDriver = driver
}

// =============================================================================
// Job Dispatcher
// =============================================================================

function generateJobId(): string {
    return crypto.randomUUID()
}

export interface DispatchOptions {
    /** Queue name */
    queue?: string
    /** Delay in milliseconds before job is available */
    delay?: number
}

/**
 * Dispatch a job to the queue
 *
 * @example
 * await dispatch(new SendEmailJob({ userId: 1, email: 'test@example.com' }))
 * await dispatch(new SendEmailJob({ userId: 1 }), { delay: 60000 }) // delay 1 minute
 * await dispatch(new SendEmailJob({ userId: 1 }), { queue: 'emails' }) // specific queue
 */
export async function dispatch<T extends JobPayload>(
    jobInstance: Job<T>,
    options: DispatchOptions = {},
): Promise<string> {
    const config = globalQueueConfig

    const job: SerializedJob = {
        id: generateJobId(),
        name: jobInstance.name,
        payload: jobInstance.payload,
        attempts: 0,
        maxAttempts: jobInstance.maxAttempts ?? 3,
        delay: options.delay ?? 0,
        queue: options.queue ?? config.defaultQueue,
        createdAt: Date.now(),
        availableAt: Date.now() + (options.delay ?? 0),
    }

    // Register job class if not already registered
    if (!jobRegistry.has(jobInstance.name)) {
        registerJob(jobInstance.constructor as JobClass<T>)
    }

    await getDriver().push(job)

    return job.id
}

/**
 * Dispatch a job by name (for dynamic dispatching)
 */
export function dispatchByName(
    jobName: string,
    payload: JobPayload,
    options: DispatchOptions = {},
): Promise<string> {
    const JobClass = getJobClass(jobName)
    if (!JobClass) {
        throw new Error(`Job "${jobName}" not registered`)
    }
    const instance = new JobClass(payload)
    return dispatch(instance, options)
}

// =============================================================================
// Queue Worker
// =============================================================================

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

export class QueueWorker {
    private running = false
    private processedJobs = 0
    private options: Required<WorkerOptions>

    constructor(options: WorkerOptions = {}) {
        this.options = {
            queues: options.queues ?? [globalQueueConfig.defaultQueue],
            sleep: options.sleep ?? 1000,
            maxJobs: options.maxJobs ?? 0,
            stopWhenEmpty: options.stopWhenEmpty ?? false,
        }
    }

    async start(): Promise<void> {
        this.running = true
        console.log(
            `🚀 Queue worker started. Processing: ${
                this.options.queues.join(', ')
            }`,
        )

        while (this.running) {
            let processed = false

            for (const queueName of this.options.queues) {
                const job = await getDriver().pop(queueName)

                if (job) {
                    processed = true
                    await this.processJob(job)

                    this.processedJobs++
                    if (
                        this.options.maxJobs > 0 &&
                        this.processedJobs >= this.options.maxJobs
                    ) {
                        console.log(
                            `✅ Processed ${this.processedJobs} jobs. Stopping.`,
                        )
                        this.stop()
                        return
                    }
                }
            }

            if (!processed) {
                if (this.options.stopWhenEmpty) {
                    console.log('📭 Queue empty. Stopping.')
                    this.stop()
                    return
                }
                await this.sleep(this.options.sleep)
            }
        }
    }

    stop(): void {
        this.running = false
    }

    private async processJob(serializedJob: SerializedJob): Promise<void> {
        const JobClass = getJobClass(serializedJob.name)

        if (!JobClass) {
            console.error(`❌ Unknown job: ${serializedJob.name}`)
            return
        }

        const job = new JobClass(serializedJob.payload)
        const attempt = serializedJob.attempts + 1

        console.log(
            `⚙️  Processing [${serializedJob.name}] (attempt ${attempt}/${serializedJob.maxAttempts})`,
        )

        try {
            await job.handle(serializedJob.payload)
            await getDriver().complete(serializedJob)
            console.log(`✅ Completed [${serializedJob.name}]`)
        } catch (error) {
            console.error(
                `❌ Failed [${serializedJob.name}]: ${
                    (error as Error).message
                }`,
            )

            serializedJob.attempts = attempt
            await getDriver().fail(serializedJob, error as Error)

            if (attempt >= serializedJob.maxAttempts && job.failed) {
                await job.failed(serializedJob.payload, error as Error)
            }
        }
    }

    private sleep(ms: number): Promise<void> {
        return new Promise((resolve) => setTimeout(resolve, ms))
    }
}

// =============================================================================
// Queue Helper Functions
// =============================================================================

/**
 * Get queue size
 */
export function queueSize(queue?: string): Promise<number> {
    return getDriver().size(queue ?? globalQueueConfig.defaultQueue)
}

/**
 * Clear a queue
 */
export function clearQueue(queue?: string): Promise<void> {
    return getDriver().clear(queue ?? globalQueueConfig.defaultQueue)
}

// =============================================================================
// @Queueable Decorator (optional)
// =============================================================================

/**
 * Decorator to define a job class
 *
 * @example
 * @Queueable('send-welcome-email')
 * class SendWelcomeEmailJob implements Job {
 *     async handle(payload: { userId: number }) { ... }
 * }
 */
export function Queueable(name: string, maxAttempts = 3): ClassDecorator {
    // deno-lint-ignore no-explicit-any
    return function (target: any) {
        target.prototype.name = name
        target.prototype.maxAttempts = maxAttempts
        registerJob(target)
        return target
    }
}
