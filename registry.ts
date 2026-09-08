/**
 * @fileoverview Job class registry and the {@link Queueable} decorator.
 *
 * A single process-global `Map` from job name to job class, plus the register
 * / lookup accessors and the decorator that registers a class on definition.
 *
 * @module @lockness/queue/registry
 */

import type { Job, JobClass, JobPayload } from './types.ts'

// deno-lint-ignore no-explicit-any
type AnyJobClass = new (payload: any) => Job<any>

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
