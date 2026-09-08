/**
 * @fileoverview Queue driver manager.
 *
 * Holds the single active driver instance, constructing it lazily from the
 * configured driver name. `getDriver` is internal to the package; only
 * `setQueueDriver` is part of the public surface.
 *
 * @module @lockness/queue/manager
 */

import type { QueueDriver } from './types.ts'
import { getQueueConfig } from './config.ts'
import { MemoryQueueDriver } from './drivers/memory.ts'
import { DenoKvQueueDriver } from './drivers/deno_kv.ts'
import { RedisQueueDriver } from './drivers/redis.ts'
import { RedisClient } from '@lockness/redis'

let queueDriver: QueueDriver | null = null

export function getDriver(): QueueDriver {
    if (!queueDriver) {
        const config = getQueueConfig()
        // Dead-letter retention (#247), threaded from config into every driver.
        const retention = {
            retentionMs: config.deadLetterRetentionMs,
            maxEntries: config.deadLetterMaxEntries,
        }
        switch (config.driver) {
            case 'deno-kv':
                queueDriver = new DenoKvQueueDriver(config.kvPath, retention)
                break
            case 'redis': {
                if (!config.redis) {
                    throw new Error(
                        "queue driver 'redis' requires a `redis` connection in the queue config",
                    )
                }
                // Lazy-connecting client (RedisClient connects on first command),
                // so constructing the driver here opens no socket.
                queueDriver = new RedisQueueDriver(
                    new RedisClient(config.redis),
                    retention,
                )
                break
            }
            case 'memory':
            default:
                queueDriver = new MemoryQueueDriver(retention)
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
