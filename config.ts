/**
 * @fileoverview Process-global queue configuration.
 *
 * Holds the single mutable `globalQueueConfig` (kept here and nowhere else so
 * there is exactly one configuration) and the accessors every other module
 * reads it through.
 *
 * @module @lockness/queue/config
 */

import type { QueueConfig } from './types.ts'

/**
 * Default dead-letter retention window, in milliseconds (14 days).
 *
 * Chosen as a middle ground: long enough to investigate and manually retry a
 * failed job during a normal on-call rotation, short enough that a payload
 * cannot linger past a reasonable data-retention horizon (#247). Applied by
 * every driver when {@link QueueConfig.deadLetterRetentionMs} is unset.
 */
export const DEFAULT_DEAD_LETTER_RETENTION_MS = 14 * 24 * 60 * 60 * 1000

/**
 * Default upper bound on entries kept in the in-memory dead-letter store.
 *
 * The in-memory driver has no external store to expire keys for it, so it is
 * bounded by both age (retention) and count. When the count is exceeded the
 * oldest entry is evicted. Applied when {@link QueueConfig.deadLetterMaxEntries}
 * is unset (#247).
 */
export const DEFAULT_DEAD_LETTER_MAX_ENTRIES = 10_000

const defaultConfig: QueueConfig = {
    driver: 'memory',
    defaultQueue: 'default',
    retryDelay: 3000,
    deadLetterRetentionMs: DEFAULT_DEAD_LETTER_RETENTION_MS,
    deadLetterMaxEntries: DEFAULT_DEAD_LETTER_MAX_ENTRIES,
}

let globalQueueConfig: QueueConfig = { ...defaultConfig }

export function configureQueue(config: Partial<QueueConfig>): void {
    globalQueueConfig = { ...globalQueueConfig, ...config }
}

export function getQueueConfig(): QueueConfig {
    return globalQueueConfig
}
