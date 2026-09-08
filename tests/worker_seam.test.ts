/**
 * The worker's terminal seam (#220): a job that exhausts its attempts is
 * dead-lettered (never silently dropped) and its `failed()` hook runs; a job
 * with attempts remaining is re-enqueued, not dead-lettered.
 *
 * This drives a *failing* job all the way through `QueueWorker.processJob`, the
 * integration point the driver-level DLQ tests do not cover.
 *
 * @module @lockness/queue/tests/worker_seam
 */

import { assert, assertEquals } from '@std/assert'
import {
    configureQueue,
    dispatch,
    type Job,
    type JobPayload,
    listFailedJobs,
    queueSize,
    QueueWorker,
    registerJob,
} from '../mod.ts'
import { getDriver } from '../manager.ts'

/** A job that records having run, for the fault-survival test. */
class SucceedingJob implements Job<JobPayload> {
    readonly name = 'seam-succeeding'
    readonly maxAttempts = 1
    readonly payload: JobPayload
    static ran = false
    constructor(payload: JobPayload) {
        this.payload = payload
    }
    handle(): Promise<void> {
        SucceedingJob.ran = true
        return Promise.resolve()
    }
}

class FailingJob implements Job<JobPayload> {
    readonly name = 'seam-failing'
    readonly maxAttempts = 1
    readonly payload: JobPayload
    static failedCalled = false
    constructor(payload: JobPayload) {
        this.payload = payload
    }
    handle(): Promise<void> {
        return Promise.reject(new Error('always fails'))
    }
    failed(): Promise<void> {
        FailingJob.failedCalled = true
        return Promise.resolve()
    }
}

class RetryJob implements Job<JobPayload> {
    readonly name = 'seam-retry'
    readonly maxAttempts = 3
    readonly payload: JobPayload
    constructor(payload: JobPayload) {
        this.payload = payload
    }
    handle(): Promise<void> {
        return Promise.reject(new Error('transient'))
    }
}

Deno.test('worker - a job that exhausts its attempts is dead-lettered and its failed() hook runs', async () => {
    FailingJob.failedCalled = false
    configureQueue({
        driver: 'memory',
        defaultQueue: 'wseam-1',
        retryDelay: 1_000,
    })
    registerJob(FailingJob)
    const id = await dispatch(new FailingJob({ n: 1 }))

    await new QueueWorker({ queues: ['wseam-1'], maxJobs: 1, sleep: 0 }).start()

    const failed = await listFailedJobs('wseam-1')
    assertEquals(
        failed.map((f) => f.id),
        [id],
        'the exhausted job is dead-lettered',
    )
    assert(FailingJob.failedCalled, 'the failed() hook ran on exhaustion')
})

Deno.test('worker - a job with attempts remaining is re-enqueued, not dead-lettered', async () => {
    // A long retryDelay keeps the re-enqueued job in the queue (delayed) rather
    // than immediately re-popped, so we can observe it was NOT dead-lettered.
    configureQueue({
        driver: 'memory',
        defaultQueue: 'wseam-2',
        retryDelay: 60_000,
    })
    registerJob(RetryJob)
    await dispatch(new RetryJob({ n: 2 }))

    await new QueueWorker({ queues: ['wseam-2'], maxJobs: 1, sleep: 0 }).start()

    assertEquals(
        (await listFailedJobs('wseam-2')).length,
        0,
        'a retryable failure must not dead-letter',
    )
    assertEquals(
        await queueSize('wseam-2'),
        1,
        're-enqueued for the next attempt',
    )
})

Deno.test('#299: a driver fault does not kill the worker', async () => {
    // `pop()` was awaited bare, so a rejection escaped `start()` and terminated
    // the loop. That was survivable while a Redis outage took ~30 seconds to
    // surface; once the client fails fast (#297) it happens in milliseconds,
    // and under a restart supervisor each restart builds a fresh client with a
    // fresh backoff streak — so #299's throttle would be reset on every crash
    // and never grow. The backoff's lifetime is the process's, and the
    // process's lifetime would be milliseconds.
    //
    // This is #299's acceptance criterion 5: the four consumers' timing
    // assumptions checked. Of the four, this is the only one that breaks.
    configureQueue({
        driver: 'memory',
        defaultQueue: 'wseam-fault',
        retryDelay: 1_000,
    })
    registerJob(SucceedingJob)
    await dispatch(new SucceedingJob({ n: 1 }))

    const driver = getDriver()
    const realPop = driver.pop.bind(driver)
    let faults = 0
    driver.pop = (queue: string) => {
        if (faults++ < 2) {
            return Promise.reject(new Error('broker wedged'))
        }
        return realPop(queue)
    }
    try {
        // Without the try/catch this rejects out of `start()` and the job is
        // never processed — the worker is simply gone.
        await new QueueWorker({
            queues: ['wseam-fault'],
            maxJobs: 1,
            sleep: 0,
        }).start()
        assert(
            faults >= 3,
            `the worker gave up after ${faults} pop attempt(s); it must ride ` +
                'out a fault and keep polling',
        )
        assert(
            SucceedingJob.ran,
            'the job was never processed — the worker died on the fault ' +
                'instead of retrying after the poll interval',
        )
    } finally {
        driver.pop = realPop
    }
})

Deno.test('#299: a persistent driver fault does not exit as "queue empty"', async () => {
    // The review gate's finding, and it is worse than the crash it replaced.
    // `continue` leaves `processed` false, which a `stopWhenEmpty` worker reads
    // as "nothing to do" — so a broker outage or an auth failure printed
    // "📭 Queue empty. Stopping." and returned success. Before the try/catch
    // the rejection propagated and CI saw a failure; silently succeeding on an
    // outage is the worse of the two.
    configureQueue({
        driver: 'memory',
        defaultQueue: 'wseam-silent',
        retryDelay: 1_000,
    })
    const driver = getDriver()
    const realPop = driver.pop.bind(driver)
    let pops = 0
    driver.pop = () => {
        pops++
        // Always faults, so the loop can only end by giving up.
        return Promise.reject(new Error('broker wedged'))
    }
    try {
        const worker = new QueueWorker({
            queues: ['wseam-silent'],
            stopWhenEmpty: true,
            sleep: 0,
        })
        // Stop it from outside after a few cycles: if the faulted flag works,
        // the worker never takes the success exit and would otherwise spin.
        const spin = worker.start()
        await new Promise((r) => setTimeout(r, 30))
        worker.stop()
        await spin
        assert(
            pops > 1,
            `the worker gave up after ${pops} attempt(s) — a persistent fault ` +
                'was reported as an empty queue and the process exited 0',
        )
    } finally {
        driver.pop = realPop
    }
})
