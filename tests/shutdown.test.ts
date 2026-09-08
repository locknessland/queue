/**
 * Queue resources are released at shutdown — **both** of them.
 *
 * #136 named only `QueueWorker.stop()`. The driver also opens a Deno KV handle
 * (`mod.ts` `getKv`) and `kv.close` appeared nowhere in the package, so a
 * stopped worker had not released the store it was reading from. SC-002 would
 * have gone green over that.
 */

import { assertEquals, assertRejects } from '@std/assert'
import {
    disposableCount,
    drainDisposables,
} from '@lockness/contract/lifecycle/internal'
import { DenoKvQueueDriver, QueueWorker } from '../mod.ts'

Deno.test('queue - a RUNNING worker loop is stopped by the drain', async () => {
    // SC-002 is about a live loop terminating. The first version of this test
    // never called start(), so `running` was already false and stop() set
    // false -> false: a stop() that did nothing at all would have passed.
    drainDisposables()
    const worker = new QueueWorker({ sleep: 5, stopWhenEmpty: false })

    const loop = worker.start()
    // Let the loop actually enter its while(this.running) body.
    await new Promise((r) => setTimeout(r, 30))

    const drained = drainDisposables()
    assertEquals(drained.length, 1)
    assertEquals(drained[0].name, 'queue:worker')
    for (const d of drained) await d.dispose()

    // The real assertion: start()'s promise resolves, which it can only do once
    // `running` went false. Without a working stop() this hangs and the test
    // times out rather than passing.
    await loop
})

Deno.test('queue - construction alone registers nothing (SC-002)', () => {
    // A worker that was built but never started owns no running loop, so it is
    // invisible to the drain. Registration is a property of a RUNNING worker,
    // decided in start() — not of construction. (#140, invariant 2.)
    drainDisposables()
    const worker = new QueueWorker()
    assertEquals(
        disposableCount(),
        0,
        'a constructed-but-never-started worker owns nothing',
    )
    void worker
})

Deno.test('queue - a restarted worker is registered again (SC-004)', async () => {
    // The gap #140 closes: registration was created at field-init and cleared by
    // stop(), so a stopped-then-restarted worker was invisible to the drain and
    // its loop was never stopped at shutdown. start() must (re-)register.
    drainDisposables()
    const worker = new QueueWorker({ sleep: 5, stopWhenEmpty: false })

    const loop1 = worker.start()
    await new Promise((r) => setTimeout(r, 30))
    assertEquals(disposableCount(), 1, 'start registers the running worker')

    worker.stop()
    await loop1
    assertEquals(disposableCount(), 0, 'stop withdraws it')

    const loop2 = worker.start()
    await new Promise((r) => setTimeout(r, 30))
    assertEquals(
        disposableCount(),
        1,
        'a re-started worker is registered again',
    )

    worker.stop()
    await loop2
})

Deno.test('queue - the KV driver announces itself only once it has a handle', async () => {
    drainDisposables()
    await Deno.mkdir(`${Deno.cwd()}/tmp`, { recursive: true })
    const path = `${Deno.cwd()}/tmp/queue-${crypto.randomUUID().slice(0, 8)}.db`

    try {
        const driver = new DenoKvQueueDriver(path)
        assertEquals(
            disposableCount(),
            0,
            'constructed but never used owns nothing',
        )

        await driver.push({
            id: '1',
            name: 'x',
            queue: 'default',
            payload: {},
            attempts: 0,
            availableAt: Date.now(),
        } as never)

        assertEquals(disposableCount(), 1, 'announced once the handle exists')
        for (const d of drainDisposables()) await d.dispose()
        await driver.close()
    } finally {
        for (const suffix of ['', '-shm', '-wal']) {
            await Deno.remove(path + suffix).catch(() => {})
        }
    }
})

Deno.test('queue - a concurrent cold start opens exactly one KV handle (SC-006)', async () => {
    // Two callers racing the very first operation must share one open. The
    // unfixed `if (!this.kv) { this.kv = await Deno.openKv() }` awaits between
    // the guard and the assignment, so both open a handle and the first leaks.
    drainDisposables()
    let opens = 0
    let closes = 0
    const fakeKv = {
        set: () => Promise.resolve({ ok: true }),
        close: () => {
            closes++
        },
    }
    const original = Deno.openKv
    Object.defineProperty(Deno, 'openKv', {
        value: () => {
            opens++
            return Promise.resolve(fakeKv)
        },
        configurable: true,
        writable: true,
    })

    try {
        const driver = new DenoKvQueueDriver()
        const job = {
            id: '1',
            name: 'x',
            queue: 'default',
            payload: {},
            attempts: 0,
            availableAt: Date.now(),
        } as never

        await Promise.all([driver.push(job), driver.push(job)])
        assertEquals(opens, 1, 'both racers share one Deno.openKv')
        assertEquals(disposableCount(), 1, 'registered exactly once')

        await driver.close()
        await driver.close() // idempotent: must not throw on a second close
        assertEquals(closes, 1, 'the single handle is closed exactly once')
    } finally {
        Object.defineProperty(Deno, 'openKv', {
            value: original,
            configurable: true,
            writable: true,
        })
    }
})

Deno.test('queue - the worker sorts before the store it reads from', async () => {
    drainDisposables()
    const worker = new QueueWorker({ sleep: 5, stopWhenEmpty: false })
    const loop = worker.start()
    await new Promise((r) => setTimeout(r, 30))

    const [first] = drainDisposables()
    // SERVICES(30) before STORES(60): draining work into a store already closed
    // loses it.
    assertEquals(first.priority, 30)

    worker.stop()
    await loop
})

Deno.test('queue - a failed cold open is not memoised; a later call retries', async () => {
    drainDisposables()
    let opens = 0
    const original = Deno.openKv
    Object.defineProperty(Deno, 'openKv', {
        value: () => {
            opens++
            if (opens === 1) {
                return Promise.reject(new Error('cold open failed'))
            }
            return Promise.resolve({
                set: () => Promise.resolve({ ok: true }),
                close: () => {},
            })
        },
        configurable: true,
        writable: true,
    })

    const job = {
        id: '1',
        name: 'x',
        queue: 'default',
        payload: {},
        attempts: 0,
        availableAt: Date.now(),
    } as never
    try {
        const driver = new DenoKvQueueDriver()
        await assertRejects(() => driver.push(job))
        assertEquals(disposableCount(), 0, 'a failed open registers nothing')

        await driver.push(job) // retries rather than replaying the rejection
        assertEquals(opens, 2, 'the failure was not memoised')
        assertEquals(disposableCount(), 1, 'the retry registered once')
        await driver.close()
    } finally {
        Object.defineProperty(Deno, 'openKv', {
            value: original,
            configurable: true,
            writable: true,
        })
    }
})

Deno.test('queue - close() during a cold open orphans no KV handle', async () => {
    // Same interleaving as the cache driver's raced test: open #1 in flight,
    // close() clears the memo, a fresh op starts open #2, the two resolve out of
    // order. The stale open #1 must release its handle, not clobber the current.
    drainDisposables()
    let opens = 0
    const opened: Array<{ closed: boolean }> = []
    const resolvers: Array<() => void> = []
    const original = Deno.openKv
    Object.defineProperty(Deno, 'openKv', {
        value: () =>
            new Promise((resolve) => {
                opens++
                resolvers.push(() => {
                    const kv = {
                        closed: false,
                        set: () => Promise.resolve({ ok: true }),
                        close() {
                            this.closed = true
                        },
                    }
                    opened.push(kv)
                    resolve(kv)
                })
            }),
        configurable: true,
        writable: true,
    })

    const job = {
        id: '1',
        name: 'x',
        queue: 'default',
        payload: {},
        attempts: 0,
        availableAt: Date.now(),
    } as never
    try {
        const driver = new DenoKvQueueDriver()
        const op1 = driver.push(job) // open #1
        const closing = driver.close() // clears memo, awaits open #1
        const op2 = driver.push(job) // open #2 on a fresh memo

        resolvers[1]()
        resolvers[0]()
        await Promise.allSettled([op1, op2, closing])
        assertEquals(opens, 2)

        await driver.close()
        assertEquals(
            opened.every((k) => k.closed),
            true,
            'every opened handle was closed — no orphan',
        )
    } finally {
        Object.defineProperty(Deno, 'openKv', {
            value: original,
            configurable: true,
            writable: true,
        })
    }
})
