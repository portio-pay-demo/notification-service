import { Queue, Worker, Job, QueueEvents } from 'bullmq';
import { redis } from '../shared/redis';
import { logger } from '../shared/logger';

interface WebhookJob {
  url: string;
  payload: Record<string, unknown>;
  merchantId: string;
  eventType: string;
  deliveryId: string;
}

/**
 * NP-2033: Fixed webhook queue backpressure.
 * Previous implementation dropped events silently when queue depth > 10k.
 * Now uses BullMQ with a dead-letter queue for failed deliveries and
 * explicit flow control to pause new events when queue depth is high.
 *
 * NP-2032: Fixed memory leak — previous implementation captured full event
 * payloads in closures inside the retry loop. Now uses job data references
 * only; payload is serialized to Redis and not held in process memory.
 */
export class WebhookProcessor {
  private readonly MAX_QUEUE_DEPTH = 50_000;

  private queue: Queue<WebhookJob>;
  private dlq: Queue<WebhookJob & { failureReason: string }>;
  private worker: Worker<WebhookJob>;

  constructor() {
    this.queue = new Queue<WebhookJob>('webhooks', { connection: redis });
    this.dlq = new Queue('webhooks-dlq', { connection: redis });

    this.worker = new Worker<WebhookJob>(
      'webhooks',
      async (job: Job<WebhookJob>) => this.deliver(job),
      {
        connection: redis,
        concurrency: 50,
      }
    );
  }

  async start() {
    this.worker.on('failed', async (job, err) => {
      if (job && job.attemptsMade >= (job.opts.attempts ?? 3)) {
        logger.error({ deliveryId: job.data.deliveryId }, 'Moving to DLQ after max retries');
        await this.dlq.add('failed-webhook', {
          ...job.data,
          failureReason: err.message,
        });
      }
    });

    logger.info('WebhookProcessor started');
  }

  async enqueue(job: WebhookJob) {
    const depth = await this.queue.count();
    if (depth >= this.MAX_QUEUE_DEPTH) {
      logger.warn({ depth, merchantId: job.merchantId }, 'Queue depth limit reached, pausing intake');
      // Signal backpressure to upstream — do not drop silently
      throw new Error(`Webhook queue at capacity (${depth}). Retry after backoff.`);
    }

    await this.queue.add('deliver', job, {
      jobId: job.deliveryId,
      attempts: 5,
      backoff: { type: 'exponential', delay: 2000 },
      removeOnComplete: { age: 3600 },
    });
  }

  private async deliver(job: Job<WebhookJob>) {
    const { url, payload, merchantId, eventType, deliveryId } = job.data;

    logger.info({ deliveryId, merchantId, eventType, attempt: job.attemptsMade }, 'Delivering webhook');

    const response = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-PortIOPay-Delivery': deliveryId,
        'X-PortIOPay-Event': eventType,
      },
      body: JSON.stringify(payload),
      signal: AbortSignal.timeout(10_000),
    });

    if (!response.ok) {
      throw new Error(`Webhook delivery failed: ${response.status} ${response.statusText}`);
    }

    logger.info({ deliveryId, status: response.status }, 'Webhook delivered');
  }
}

// NP-6 fix: BullMQ DLQ + backpressure — no more silent event drops
