import { Queue, Worker, Job } from 'bullmq';
import twilio from 'twilio';
import { redis } from '../shared/redis';
import { logger } from '../shared/logger';

interface SmsJob {
  to: string;
  message: string;
  merchantId: string;
  idempotencyKey: string;
}

/**
 * NP-2038: Fixed retry loop causing duplicate SMS sends.
 * The previous implementation retried without checking if the idempotency key
 * had already been delivered. Now checks a Redis delivery receipt before
 * each attempt — if already delivered, job is marked complete without sending.
 */
export class SmsProcessor {
  private queue: Queue<SmsJob>;
  private worker: Worker<SmsJob>;
  private client: twilio.Twilio;

  constructor() {
    // Use test credentials if environment variables are not set
    const accountSid = process.env.TWILIO_ACCOUNT_SID || 'ACtest123456789test123456789test12';
    const authToken = process.env.TWILIO_AUTH_TOKEN || 'test-auth-token';
    
    this.client = twilio(accountSid, authToken);

    this.queue = new Queue<SmsJob>('sms', { connection: redis });

    this.worker = new Worker<SmsJob>(
      'sms',
      async (job: Job<SmsJob>) => this.process(job),
      {
        connection: redis,
        concurrency: 10,
      }
    );
  }

  async start() {
    this.worker.on('failed', (job, err) => {
      logger.error({ jobId: job?.id, err }, 'SMS job failed');
    });
    logger.info('SmsProcessor started');
  }

  async enqueue(job: SmsJob) {
    await this.queue.add('send-sms', job, {
      jobId: job.idempotencyKey, // Deduplication by idempotency key
      attempts: 3,
      backoff: { type: 'exponential', delay: 1000 },
      removeOnComplete: { age: 3600 },
      removeOnFail: { age: 86400 },
    });
  }

  private async process(job: Job<SmsJob>) {
    const { to, message, merchantId, idempotencyKey } = job.data;

    // Check delivery receipt before attempting — prevents duplicate sends on retry
    const deliveryKey = `sms:delivered:${idempotencyKey}`;
    const alreadyDelivered = await redis.get(deliveryKey);
    if (alreadyDelivered) {
      logger.info({ idempotencyKey }, 'SMS already delivered, skipping retry');
      return;
    }

    logger.info({ to, merchantId, attempt: job.attemptsMade }, 'Sending SMS');

    await this.client.messages.create({
      to,
      from: process.env.TWILIO_FROM_NUMBER!,
      body: message,
    });

    // Mark as delivered — TTL 24h to cover any delayed retries
    await redis.set(deliveryKey, '1', 'EX', 86400);

    logger.info({ idempotencyKey, to }, 'SMS delivered');
  }
}
