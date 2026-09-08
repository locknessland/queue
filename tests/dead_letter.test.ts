/**
 * Cross-driver dead-letter (#220) on the in-memory driver: an exhausted job is
 * stored (not dropped), listed without its payload (SEC-F8), and re-enqueued by
 * `retryFailed`.
 *
 * @module @lockness/queue/tests/dead_letter
 */

import { assert, assertEquals } from '@std/assert'
import { MemoryQueueDriver } from '../drivers/memory.ts'
import type { SerializedJob } from '../types.ts'

function job(id: string, queue: string): SerializedJob {
    return {
        id,
        name: 'SendEmail',
        payload: { to: 'a@b.c', secret: 'TOP_SECRET_TOKEN' },
        attempts: 3,
        maxAttempts: 3,
        delay: 0,
        queue,
        createdAt: 0,
        availableAt: 0,
    }
}

Deno.test('deadLetter - an exhausted job is stored and listed without its payload', async () => {
    const d = new MemoryQueueDriver()
    const q = 'dlq-test-1'
    await d.deadLetter(job('j1', q), new TypeError('provider rejected'))

    const failed = await d.listFailed(q)
    assertEquals(failed.length, 1)
    assertEquals(failed[0].id, 'j1')
    assertEquals(failed[0].name, 'SendEmail')
    assertEquals(
        failed[0].error,
        'TypeError',
        'the error CLASS, not its message',
    )
    // The projection must not carry the payload (SEC-F8).
    assert(
        !JSON.stringify(failed).includes('TOP_SECRET_TOKEN'),
        'listFailed must not expose the job payload',
    )
})

Deno.test('deadLetter - retryFailed re-enqueues the job with attempts reset', async () => {
    const d = new MemoryQueueDriver()
    const q = 'dlq-test-2'
    await d.deadLetter(job('j2', q), new Error('boom'))

    assertEquals(await d.retryFailed('j2'), true)
    // It is back on the queue and its attempts were reset.
    const popped = await d.pop(q)
    assert(popped !== null)
    assertEquals(popped!.id, 'j2')
    assertEquals(popped!.attempts, 0)
    // And it is gone from the dead-letter store.
    assertEquals((await d.listFailed(q)).length, 0)
})

Deno.test('deadLetter - retryFailed on an unknown id returns false', async () => {
    const d = new MemoryQueueDriver()
    assertEquals(await d.retryFailed('nope-404'), false)
})

Deno.test('fail - a retry re-pushes the job (no silent drop)', async () => {
    const d = new MemoryQueueDriver()
    const q = 'dlq-test-3'
    const j = { ...job('j3', q), attempts: 1, availableAt: 0 }
    await d.fail(j, new Error('transient'))
    // fail() re-persists — the job is back on the queue for the next attempt.
    const popped = await d.pop(q)
    assertEquals(popped?.id, 'j3')
})
