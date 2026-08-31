/**
 * Tests for Queue System
 */

import { assertEquals, assertExists } from '@std/assert'
import {
    clearQueue,
    configureQueue,
    dispatch,
    type Job,
    type JobPayload,
    MemoryQueueDriver,
    QueueWorker,
    registerJob,
} from '../mod.ts'

interface TestPayload extends JobPayload {
    message: string
}

class TestJob implements Job<TestPayload> {
    readonly name = 'test-job'
    readonly maxAttempts = 3
    readonly payload: TestPayload
    static executedPayloads: TestPayload[] = []

    constructor(payload: TestPayload) {
        this.payload = payload
    }

    async handle(payload: TestPayload): Promise<void> {
        TestJob.executedPayloads.push(payload)
        await Promise.resolve()
    }

    async failed(_payload: TestPayload, _error: Error): Promise<void> {
        await Promise.resolve()
    }
}

Deno.test('queue system', async (t) => {
    await t.step('configureQueue sets up memory driver', () => {
        configureQueue({ driver: 'memory' })
    })

    await t.step('registerJob registers a job class', () => {
        registerJob(TestJob)
    })

    await t.step('dispatch adds job to queue', async () => {
        configureQueue({ driver: 'memory' })
        registerJob(TestJob)

        const jobId = await dispatch(new TestJob({ message: 'Hello' }))
        assertExists(jobId)
        assertEquals(typeof jobId, 'string')
    })

    await t.step('worker processes jobs', async () => {
        configureQueue({ driver: 'memory' })
        await clearQueue('default')
        registerJob(TestJob)
        TestJob.executedPayloads = []

        await dispatch(new TestJob({ message: 'Test 1' }))
        await dispatch(new TestJob({ message: 'Test 2' }))

        const worker = new QueueWorker({
            queues: ['default'],
            stopWhenEmpty: true,
        })

        await worker.start()

        assertEquals(TestJob.executedPayloads.length, 2)
        assertEquals(TestJob.executedPayloads[0].message, 'Test 1')
        assertEquals(TestJob.executedPayloads[1].message, 'Test 2')
    })

    await t.step('MemoryQueueDriver push and pop', async () => {
        const driver = new MemoryQueueDriver()

        await driver.push({
            id: 'test-1',
            name: 'test',
            payload: { data: 'value' },
            attempts: 0,
            maxAttempts: 3,
            delay: 0,
            queue: 'default',
            createdAt: Date.now(),
            availableAt: Date.now(),
        })

        const job = await driver.pop('default')
        assertExists(job)
        assertEquals(job?.id, 'test-1')
    })

    await t.step('MemoryQueueDriver size and clear', async () => {
        const driver = new MemoryQueueDriver()

        await driver.push({
            id: 'test-size-1',
            name: 'test',
            payload: {},
            attempts: 0,
            maxAttempts: 3,
            delay: 0,
            queue: 'size-test-queue',
            createdAt: Date.now(),
            availableAt: Date.now(),
        })

        const size = await driver.size('size-test-queue')
        assertEquals(size, 1)

        await driver.clear('size-test-queue')
        const sizeAfterClear = await driver.size('size-test-queue')
        assertEquals(sizeAfterClear, 0)
    })
})
