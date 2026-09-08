/**
 * @fileoverview The queue worker loop.
 *
 * Polls the configured queues through the active driver, runs each job's
 * handler with retry accounting, and registers itself with the lifecycle drain
 * so a running loop stops before the stores it reads from close.
 *
 * @module @lockness/queue/worker
 */

import { renderError, safeForLog } from '@lockness/contract'
import {
    deregisterDisposable,
    type DisposableHandle,
    registerDisposable,
} from '@lockness/contract/lifecycle/internal'
import type { SerializedJob, WorkerOptions } from './types.ts'
import { getQueueConfig } from './config.ts'
import { computeNextAvailable } from './backoff.ts'
import { getDriver } from './manager.ts'
import { getJobClass } from './registry.ts'

export class QueueWorker {
    private running = false
    /**
     * SERVICES: producers stop before the stores they write into close.
     *
     * Registered in {@link QueueWorker.start}, not here: a worker built but
     * never started owns no running loop, so it must be invisible to the drain
     * (#140, invariant "a registrant that owns nothing is not registered").
     * `start()` re-registers, so a stopped-then-restarted worker is drained too.
     */
    #handle: DisposableHandle | undefined = undefined
    private processedJobs = 0
    private options: Required<WorkerOptions>

    constructor(options: WorkerOptions = {}) {
        this.options = {
            queues: options.queues ?? [getQueueConfig().defaultQueue],
            sleep: options.sleep ?? 1000,
            maxJobs: options.maxJobs ?? 0,
            stopWhenEmpty: options.stopWhenEmpty ?? false,
        }
    }

    async start(): Promise<void> {
        this.running = true
        // Register the running loop with the drain if it is not already —
        // `??=` keeps a double start() without an intervening stop() to one
        // entry, and re-registers a worker that was stopped and restarted.
        this.#handle ??= registerDisposable({
            name: 'queue:worker',
            dispose: () => this.stop(),
            priority: 30,
        })
        console.log(
            `🚀 Queue worker started. Processing: ${
                this.options.queues.join(', ')
            }`,
        )

        while (this.running) {
            let processed = false
            // Tracked beside `processed` because they are different facts and
            // the success exit below must not confuse them. `continue` on a
            // fault leaves `processed` false, which without this reads as "the
            // queue is empty" — so a `stopWhenEmpty` worker printed "Queue
            // empty. Stopping." and exited 0 during a broker outage or an auth
            // failure. Before this branch the rejection propagated and CI saw a
            // failure; silently succeeding is worse than crashing.
            let faulted = false

            for (const queueName of this.options.queues) {
                // A DRIVER FAULT MUST NOT KILL THE WORKER (#299 AC 5).
                //
                // `pop()` was awaited bare, so a rejection escaped `start()`
                // and terminated the loop. That was survivable while a Redis
                // outage took ~30 seconds to surface; once the client fails
                // fast it happens in milliseconds, and under a restart
                // supervisor each restart builds a fresh client with a fresh
                // backoff streak — so the throttle the client just gained is
                // reset on every crash and never grows. The backoff's lifetime
                // is the process's, and the process's lifetime would be
                // milliseconds.
                //
                // Falling through to the existing sleep is what makes the
                // worker ride out an outage on the client's cadence instead.
                let job
                try {
                    job = await getDriver().pop(queueName)
                } catch (error) {
                    faulted = true
                    console.warn(
                        `[queue] ${
                            safeForLog(queueName)
                        }: pop failed; retrying after the poll interval: ${
                            renderError(error)
                        }`,
                    )
                    continue
                }

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
                if (this.options.stopWhenEmpty && !faulted) {
                    console.log('📭 Queue empty. Stopping.')
                    this.stop()
                    return
                }
                await this.sleep(this.options.sleep)
            }
        }
    }

    /**
     * Stop the worker loop.
     *
     * Withdraws the shutdown registration too: a worker that has already
     * stopped is not something the framework still needs to release, and
     * leaving the entry behind grows the registry in a long-lived process that
     * starts and stops workers.
     *
     * @example
     * ```typescript
     * worker.stop()
     * ```
     */
    stop(): void {
        this.running = false
        if (this.#handle) {
            deregisterDisposable(this.#handle)
            this.#handle = undefined
        }
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

            // The worker owns the terminal decision (#220): while attempts
            // remain, re-enqueue with the shared backoff; on exhaustion,
            // dead-letter the job (never a silent drop) and run its failure hook.
            if (attempt < serializedJob.maxAttempts) {
                serializedJob.availableAt = computeNextAvailable(
                    attempt,
                    getQueueConfig(),
                )
                await getDriver().fail(serializedJob, error as Error)
            } else {
                await getDriver().deadLetter(serializedJob, error as Error)
                if (job.failed) {
                    await job.failed(serializedJob.payload, error as Error)
                }
            }
        }
    }

    private sleep(ms: number): Promise<void> {
        return new Promise((resolve) => setTimeout(resolve, ms))
    }
}
