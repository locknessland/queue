/**
 * Retry backoff (#220) — the single home of the next-attempt delay.
 *
 * @module @lockness/queue/tests/backoff
 */

import { assertEquals } from '@std/assert'
import { computeNextAvailable } from '../backoff.ts'
import type { QueueConfig } from '../types.ts'

const base: QueueConfig = {
    driver: 'memory',
    defaultQueue: 'default',
    retryDelay: 1_000,
}

Deno.test('backoff - fixed is always retryDelay regardless of attempt', () => {
    assertEquals(
        computeNextAvailable(1, { ...base, backoff: 'fixed' }, 0),
        1_000,
    )
    assertEquals(
        computeNextAvailable(5, { ...base, backoff: 'fixed' }, 0),
        1_000,
    )
    // Default (unset) is fixed.
    assertEquals(computeNextAvailable(3, base, 0), 1_000)
})

Deno.test('backoff - exponential doubles per attempt from retryDelay', () => {
    const cfg: QueueConfig = { ...base, backoff: 'exponential' }
    assertEquals(computeNextAvailable(1, cfg, 0), 1_000) // 1×
    assertEquals(computeNextAvailable(2, cfg, 0), 2_000) // 2×
    assertEquals(computeNextAvailable(3, cfg, 0), 4_000) // 4×
})

Deno.test('backoff - the delay is added to `now`', () => {
    assertEquals(computeNextAvailable(1, base, 10_000), 11_000)
})
