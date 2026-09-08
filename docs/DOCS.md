# Lockness Queue

Background job processing system with automatic retries, delayed execution, and
multiple driver support.

## Overview

@lockness/queue provides a simple yet powerful job queue system with Memory and
Deno KV drivers. Features include automatic retries, delayed jobs, multiple
queues, and full TypeScript type safety.

## Installation

```typescript
import { configureQueue, dispatch, QueueWorker } from '@lockness/queue'
```

## Configuration

```typescript
configureQueue({
    driver: 'memory', // 'memory' | 'deno-kv' | 'redis'
    defaultQueue: 'default',
    kvPath: './data/kv', // optional, for deno-kv driver
    retryDelay: 3000, // 3 seconds between retries
    deadLetterRetentionMs: 14 * 24 * 60 * 60 * 1000, // 14 days (default)
    deadLetterMaxEntries: 10_000, // in-memory cap only (default)
})
```

### Dead-letter retention

A job that exhausts its retries is moved to the dead-letter store rather than
dropped. Because a failed job's payload is sensitive data, the store is **not**
retained indefinitely — every driver enforces a retention window:

- **`deadLetterRetentionMs`** — how long a dead-lettered job is kept, in
  milliseconds. **Default: 14 days.** The Deno KV driver writes each entry with
  an `expireIn` so it self-expires; the memory and Redis drivers purge entries
  older than the window opportunistically, on every `deadLetter` write and on
  `listFailed`.
- **`deadLetterMaxEntries`** — an additional count cap for the **in-memory**
  driver only (it has no external store to expire keys for it). Once exceeded,
  the oldest entry is evicted. **Default: 10 000.** The durable drivers ignore
  it; they are bounded by the retention window alone.

### Redis-backed workers from the environment

The `queue:work`, `queue:clear` and `queue:retry` commands read the driver and
its connection from the environment, so a Redis-backed worker needs no code
change — only variables. Set `QUEUE_DRIVER=redis` and the `REDIS_*` connection
variables (the same names the Redis-backed session store uses):

| Variable         | Maps to    | Notes                                   |
| ---------------- | ---------- | --------------------------------------- |
| `REDIS_HOST`     | `hostname` | **Required** when `QUEUE_DRIVER=redis`. |
| `REDIS_PORT`     | `port`     | Positive integer; Redis default `6379`. |
| `REDIS_DB`       | `db`       | Positive integer; Redis default `0`.    |
| `REDIS_PASSWORD` | `password` | Optional `AUTH` credential.             |
| `REDIS_TLS`      | `tls`      | `true`/`1` to wrap the socket with TLS. |

```bash
QUEUE_DRIVER=redis \
REDIS_HOST=redis.internal \
REDIS_PORT=6379 \
REDIS_PASSWORD=... \
REDIS_TLS=true \
  deno task cli queue:work --queue=emails
```

`REDIS_HOST` is required (unlike the session store's dev-friendly `localhost`
default): a background worker is a deployed process, so a silent fallback to
localhost would mask a misconfiguration. When it is missing the command exits
with a clear `RedisQueueConfigError` naming the variable, rather than a
downstream socket error. Setting `REDIS_PASSWORD` with `REDIS_TLS` off sends the
credential over plaintext — `RedisClient` raises a one-time cleartext-`AUTH`
warning when it connects (it is neither suppressed nor duplicated here).

## Basic Usage

### 1. Define a Job

```typescript
import type { Job, JobPayload } from '@lockness/queue'

interface SendEmailPayload extends JobPayload {
    userId: number
    email: string
    subject: string
    body: string
}

class SendEmailJob implements Job<SendEmailPayload> {
    name = 'send-email'
    maxAttempts = 3

    constructor(public payload: SendEmailPayload) {}

    async handle(payload: SendEmailPayload): Promise<void> {
        console.log(`Sending email to ${payload.email}`)
        await sendEmail(payload.email, payload.subject, payload.body)
    }

    async failed(payload: SendEmailPayload, error: Error): Promise<void> {
        // Called after all retries failed
        console.error(
            `Failed to send email after ${this.maxAttempts} attempts: ${error.message}`,
        )
        await logError(error)
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
        body: 'Welcome to our service',
    }),
)

// With delay (1 minute)
await dispatch(job, { delay: 60000 })

// To specific queue
await dispatch(job, { queue: 'emails' })

// Combined options
await dispatch(job, { queue: 'notifications', delay: 5000 })
```

### 3. Process Jobs with Worker

```typescript
import { QueueWorker } from '@lockness/queue'

const worker = new QueueWorker({
    queues: ['default', 'emails', 'notifications'],
    sleep: 1000, // Check for jobs every second
    maxJobs: 0, // Unlimited (0 = continuous)
    stopWhenEmpty: false, // Keep running
})

await worker.start()
```

## Decorator Alternative

Use @Queueable decorator for cleaner job definitions:

```typescript
import { Queueable } from '@lockness/queue'

@Queueable('send-welcome-email', 5) // name, maxAttempts
class SendWelcomeEmailJob implements Job<{ userId: number }> {
    constructor(public payload: { userId: number }) {}

    async handle(payload: { userId: number }): Promise<void> {
        const user = await getUserById(payload.userId)
        await sendWelcomeEmail(user)
    }

    async failed(payload: { userId: number }, error: Error): Promise<void> {
        await notifyAdmin(
            `Welcome email failed for user ${payload.userId}`,
            error,
        )
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
console.log(`Default queue has ${size} jobs`)

// Clear queue
await clearQueue('emails')
```

## Drivers

### Memory Driver (Default)

In-memory queue for development and testing. Jobs are lost on restart.

```typescript
configureQueue({ driver: 'memory' })
```

**Pros:**

- Fast
- Simple setup
- Good for development

**Cons:**

- Jobs lost on restart
- Not shared across instances
- Memory grows with queue size

### Deno KV Driver

Persistent queue using Deno KV storage. Jobs survive restarts.

```typescript
configureQueue({
    driver: 'deno-kv',
    kvPath: './data/kv', // optional, defaults to Deno.openKv()
})
```

**Pros:**

- Persistent across restarts
- Shared across instances
- Production-ready

**Cons:**

- Requires Deno KV access
- Slight performance overhead

## Common Use Cases

### Email Notifications

```typescript
@Queueable('send-notification', 3)
class SendNotificationJob implements Job<{ userId: number; message: string }> {
    constructor(public payload: { userId: number; message: string }) {}

    async handle({ userId, message }: { userId: number; message: string }) {
        const user = await getUserById(userId)
        await mail()
            .to(user.email)
            .subject('Notification')
            .html(message)
            .send()
    }
}

// Dispatch from controller
@Post('/notify')
async notify(c: Context) {
    const { userId, message } = await c.req.json()
    await dispatch(new SendNotificationJob({ userId, message }))
    return c.json({ queued: true })
}
```

### Image Processing

```typescript
@Queueable('process-image', 5)
class ProcessImageJob implements Job<{ imageId: number }> {
    constructor(public payload: { imageId: number }) {}

    async handle({ imageId }: { imageId: number }) {
        const image = await getImage(imageId)

        // Generate thumbnails
        const thumbnail = await resizeImage(image, 200, 200)
        await storage().put(`thumbnails/${imageId}.jpg`, thumbnail)

        // Generate medium size
        const medium = await resizeImage(image, 800, 800)
        await storage().put(`medium/${imageId}.jpg`, medium)

        // Update database
        await markImageProcessed(imageId)
    }

    async failed({ imageId }: { imageId: number }, error: Error) {
        await markImageFailed(imageId, error.message)
    }
}
```

### Payment Processing with Chaining

```typescript
@Queueable('process-payment', 5)
class ProcessPaymentJob implements Job<{ orderId: number }> {
    constructor(public payload: { orderId: number }) {}

    async handle({ orderId }: { orderId: number }) {
        const order = await getOrder(orderId)

        // Process payment
        const payment = await processPayment(order)
        await updateOrderStatus(orderId, 'paid')

        // Chain next job: send receipt
        await dispatch(
            new SendReceiptJob({ orderId, paymentId: payment.id }),
            { delay: 2000 },
        )

        // Chain another job: update inventory
        await dispatch(new UpdateInventoryJob({ orderId }))
    }

    async failed({ orderId }: { orderId: number }, error: Error) {
        await updateOrderStatus(orderId, 'failed')
        await notifyAdmin(`Payment failed for order ${orderId}`, error)
    }
}

@Queueable('send-receipt', 3)
class SendReceiptJob implements Job<{ orderId: number; paymentId: string }> {
    constructor(public payload: { orderId: number; paymentId: string }) {}

    async handle(
        { orderId, paymentId }: { orderId: number; paymentId: string },
    ) {
        const order = await getOrder(orderId)
        const receipt = await generateReceipt(order, paymentId)
        await sendReceiptEmail(order.user.email, receipt)
    }
}
```

### Scheduled Reports

```typescript
@Queueable('generate-report', 3)
class GenerateReportJob implements Job<{ reportType: string; userId: number }> {
    constructor(public payload: { reportType: string; userId: number }) {}

    async handle(
        { reportType, userId }: { reportType: string; userId: number },
    ) {
        const data = await fetchReportData(reportType, userId)
        const report = await generatePDF(data)

        const filename = `reports/${userId}/${reportType}-${Date.now()}.pdf`
        await storage().put(filename, report)

        const url = await storage().signedUrl(filename, 86400) // 24 hours
        await notifyUserReportReady(userId, url)
    }
}

// Schedule daily reports
Deno.cron('daily-reports', '0 0 * * *', async () => {
    const users = await getActiveUsers()
    for (const user of users) {
        await dispatch(
            new GenerateReportJob({ reportType: 'daily', userId: user.id }),
        )
    }
})
```

## Worker Configuration

### Basic Worker

```typescript
const worker = new QueueWorker({
    queues: ['default'],
    sleep: 1000,
})

await worker.start()
```

### Multi-Queue Worker with Priority

```typescript
// Process high-priority queues first
const worker = new QueueWorker({
    queues: ['critical', 'high', 'normal', 'low'],
    sleep: 500, // Check more frequently
    maxJobs: 100, // Stop after 100 jobs
})

await worker.start()
```

### Worker for Specific Job Types

```typescript
const emailWorker = new QueueWorker({
    queues: ['emails'],
    sleep: 2000,
    stopWhenEmpty: false,
})

const imageWorker = new QueueWorker({
    queues: ['images', 'thumbnails'],
    sleep: 1000,
    stopWhenEmpty: false,
})

await Promise.all([
    emailWorker.start(),
    imageWorker.start(),
])
```

## Best Practices

- Use meaningful job names (e.g., 'send-welcome-email', not 'job1')
- Set appropriate maxAttempts based on job criticality
- Implement failed() handler for alerting and logging
- Use specific queues for different job types
- Add delays for rate-limited operations
- Chain related jobs instead of doing everything in one job
- Use Deno KV driver in production for persistence
- Monitor queue sizes to detect processing issues
- Implement idempotent job handlers (safe to retry)
- Log job execution for debugging
- Use payload interfaces for type safety
- Keep payload data small and serializable

## Testing

```typescript
import { configureQueue, dispatch, QueueWorker } from '@lockness/queue'

Deno.test('job processing', async () => {
    // Use memory driver for tests
    configureQueue({ driver: 'memory' })

    let executed = false

    @Queueable('test-job', 1)
    class TestJob implements Job<{ value: string }> {
        constructor(public payload: { value: string }) {}

        async handle({ value }: { value: string }) {
            executed = true
            assertEquals(value, 'test')
        }
    }

    await dispatch(new TestJob({ value: 'test' }))

    const worker = new QueueWorker({
        queues: ['default'],
        maxJobs: 1,
        stopWhenEmpty: true,
    })

    await worker.start()

    assertEquals(executed, true)
})
```
