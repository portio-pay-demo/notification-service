import { Router, Request, Response } from 'express';
import { z } from 'zod';
import { SmsProcessor } from '../sms/SmsProcessor';
import { WebhookProcessor } from '../webhook/WebhookProcessor';
import { logger } from './logger';

const smsNotificationSchema = z.object({
  channel: z.literal('sms'),
  to: z.string().min(1),
  message: z.string().min(1).max(1600),
  merchantId: z.string().min(1),
  idempotencyKey: z.string().min(1),
});

const webhookNotificationSchema = z.object({
  channel: z.literal('webhook'),
  url: z.string().url(),
  payload: z.record(z.unknown()),
  merchantId: z.string().min(1),
  eventType: z.string().min(1),
  deliveryId: z.string().min(1),
});

const sendNotificationSchema = z.discriminatedUnion('channel', [
  smsNotificationSchema,
  webhookNotificationSchema,
]);

export function createNotificationRouter(
  smsProcessor: SmsProcessor,
  webhookProcessor: WebhookProcessor
): Router {
  const router = Router();

  router.post('/notifications/send', async (req: Request, res: Response) => {
    const parsed = sendNotificationSchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).json({
        error: 'Invalid request',
        details: parsed.error.flatten().fieldErrors,
      });
    }

    const notification = parsed.data;

    try {
      if (notification.channel === 'sms') {
        await smsProcessor.enqueue({
          to: notification.to,
          message: notification.message,
          merchantId: notification.merchantId,
          idempotencyKey: notification.idempotencyKey,
        });

        return res.status(202).json({
          status: 'queued',
          channel: 'sms',
          idempotencyKey: notification.idempotencyKey,
        });
      }

      await webhookProcessor.enqueue({
        url: notification.url,
        payload: notification.payload,
        merchantId: notification.merchantId,
        eventType: notification.eventType,
        deliveryId: notification.deliveryId,
      });

      return res.status(202).json({
        status: 'queued',
        channel: 'webhook',
        deliveryId: notification.deliveryId,
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to queue notification';
      logger.error({ err, channel: notification.channel }, 'Failed to enqueue notification');

      if (message.includes('queue at capacity')) {
        return res.status(503).json({ error: message });
      }

      return res.status(500).json({ error: 'Failed to queue notification' });
    }
  });

  router.get('/notifications/:id/status', (req, res) => {
    res.json({ id: req.params.id, status: 'delivered' });
  });

  return router;
}
