/**
 * @fileoverview Job dispatch and queue helper functions.
 *
 * Serialises a job instance and pushes it through the active driver, plus the
 * dynamic dispatch-by-name path and the `queueSize` / `clearQueue` helpers.
 *
 * @module @lockness/queue/dispatch
 */

import type {
    DispatchOptions,
    Job,
    JobClass,
    JobPayload,
    SerializedJob,
} from './types.ts'
import { getQueueConfig } from './config.ts'
import { getDriver } from './manager.ts'
import { getJobClass, registerJob } from './registry.ts'

function generateJobId(): string {
    return crypto.randomUUID()
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
    const config = getQueueConfig()

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
    if (!getJobClass(jobInstance.name)) {
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

/**
 * Get queue size
 */
export function queueSize(queue?: string): Promise<number> {
    return getDriver().size(queue ?? getQueueConfig().defaultQueue)
}

/**
 * Clear a queue
 */
export function clearQueue(queue?: string): Promise<void> {
    return getDriver().clear(queue ?? getQueueConfig().defaultQueue)
}

/**
 * List dead-lettered jobs (projected, no payload — #220 / SEC-F8), optionally
 * filtered to one queue.
 *
 * @param queue - Restrict to this queue; omit for all.
 * @returns The projected dead-letter entries.
 */
export function listFailedJobs(
    queue?: string,
): Promise<import('./types.ts').DeadLetterEntry[]> {
    return getDriver().listFailed(queue)
}

/**
 * Re-enqueue a dead-lettered job by id, resetting its attempts (#220).
 *
 * @param id - The dead job's id.
 * @returns `true` if the id was found and re-enqueued.
 */
export function retryFailedJob(id: string): Promise<boolean> {
    return getDriver().retryFailed(id)
}
