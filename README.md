# @lockness/queue

Background job processing system with multiple driver support.

## Features

- 🚀 Simple job dispatch and processing
- 📦 Multiple drivers: Memory, Deno KV
- 🔄 Automatic retries with configurable delays
- ⏱️ Delayed job execution
- 🎯 Multiple queue support
- 🏷️ Job registry system
- 💪 TypeScript support with full type safety

## Installation

```typescript
import { configureQueue, dispatch, QueueWorker } from '@lockness/queue'
```

## Configuration

```typescript
configureQueue({
    driver: 'memory', // or 'deno-kv'
    defaultQueue: 'default',
    kvPath: './data/kv', // optional, for deno-kv
    retryDelay: 3000, // 3 seconds
})
```

## Usage

### 1. Define a Job

```typescript
import type { Job, JobPayload } from '@lockness/queue'

interface SendEmailPayload extends JobPayload {
    userId: number
    email: string
    subject: string
}

class SendEmailJob implements Job<SendEmailPayload> {
    name = 'send-email'
    maxAttempts = 3

    constructor(public payload: SendEmailPayload) {}

    async handle(payload: SendEmailPayload): Promise<void> {
        // Send email logic
        console.log(`Sending email to ${payload.email}`)
        await sendEmail(payload.email, payload.subject)
    }

    async failed(payload: SendEmailPayload, error: Error): Promise<void> {
        // Called after all retries failed
        console.error(`Failed to send email: ${error.message}`)
    }
}
```

### 2. Dispatch Jobs

```typescript
import { dispatch } from '@lockness/queue'

// Simple dispatch
await dispatch(
    new SendEmailJob({
        userId: 1,
        email: 'user@example.com',
        subject: 'Welcome!',
    }),
)

// With delay (1 minute)
await dispatch(job, { delay: 60000 })

// To specific queue
await dispatch(job, { queue: 'emails' })
```

### 3. Process Jobs with Worker

```typescript
import { QueueWorker } from '@lockness/queue'

const worker = new QueueWorker({
    queues: ['default', 'emails'],
    sleep: 1000, // Check every second
    maxJobs: 0, // Unlimited
    stopWhenEmpty: false, // Keep running
})

await worker.start()
```

## Decorator Alternative

Use the `@Queueable` decorator for cleaner job definitions:

```typescript
import { Queueable } from '@lockness/queue'

@Queueable('send-welcome-email', 5) // name, maxAttempts
class SendWelcomeEmailJob implements Job {
    constructor(public payload: { userId: number }) {}

    async handle(payload: { userId: number }): Promise<void> {
        // Job logic
    }
}

// Job is automatically registered
await dispatch(new SendWelcomeEmailJob({ userId: 1 }))
```

## Queue Management

```typescript
import { clearQueue, queueSize } from '@lockness/queue'

// Get queue size
const size = await queueSize('default')

// Clear queue
await clearQueue('emails')
```

## Drivers

### Memory Driver (Default)

In-memory queue, good for development and testing.

```typescript
configureQueue({ driver: 'memory' })
```

### Deno KV Driver

Persistent queue using Deno KV storage.

```typescript
configureQueue({
    driver: 'deno-kv',
    kvPath: './data/kv', // optional
})
```

## Advanced Example

```typescript
// Job with retry logic
@Queueable('process-payment', 5)
class ProcessPaymentJob implements Job<{ orderId: number }> {
    constructor(public payload: { orderId: number }) {}

    async handle({ orderId }: { orderId: number }): Promise<void> {
        const order = await getOrder(orderId)
        await processPayment(order)

        // Dispatch another job
        await dispatch(new SendReceiptJob({ orderId }), {
            delay: 5000, // Wait 5 seconds
        })
    }

    async failed(
        { orderId }: { orderId: number },
        error: Error,
    ): Promise<void> {
        await logError(`Payment failed for order ${orderId}`, error)
        await notifyAdmin(orderId, error)
    }
}

// Worker with multiple queues
const worker = new QueueWorker({
    queues: ['payments', 'emails', 'default'],
    sleep: 500,
})

await worker.start()
```

## License

MIT
