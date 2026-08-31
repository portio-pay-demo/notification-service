import { Router, Request, Response } from 'express';
import { z } from 'zod';
import { randomUUID } from 'crypto';
import { WebhookRegistry } from './WebhookRegistry';
import { WebhookProcessor } from './WebhookProcessor';
import { logger } from '../shared/logger';

const registerWebhookSchema = z.object({
  merchantId: z.string().min(1),
  url: z.string().url(),
  eventTypes: z.array(z.string().min(1)).min(1),
  description: z.string().optional(),
});

const testWebhookSchema = z.object({
  merchantId: z.string().min(1),
  eventType: z.string().min(1).default('webhook.test'),
  url: z.string().url().optional(),
});

export function createWebhookRouter(
  registry: WebhookRegistry,
  processor: WebhookProcessor
): Router {
  const router = Router();

  router.post('/webhooks/register', async (req: Request, res: Response) => {
    const parsed = registerWebhookSchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).json({
        error: 'Invalid request',
        details: parsed.error.flatten().fieldErrors,
      });
    }

    const registration = await registry.register(parsed.data);

    return res.status(201).json({
      status: 'registered',
      merchantId: registration.merchantId,
      url: registration.url,
      eventTypes: registration.eventTypes,
      registeredAt: registration.registeredAt,
    });
  });

  router.post('/webhooks/test', async (req: Request, res: Response) => {
    const parsed = testWebhookSchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).json({
        error: 'Invalid request',
        details: parsed.error.flatten().fieldErrors,
      });
    }

    const { merchantId, eventType, url: overrideUrl } = parsed.data;
    let targetUrl = overrideUrl;

    if (!targetUrl) {
      const registration = await registry.get(merchantId);
      if (!registration) {
        return res.status(404).json({
          error: 'No webhook registered for merchant',
          merchantId,
        });
      }
      targetUrl = registration.url;
    }

    const deliveryId = `test_${randomUUID()}`;
    const payload = {
      type: eventType,
      merchantId,
      test: true,
      timestamp: new Date().toISOString(),
    };

    try {
      await processor.enqueue({
        url: targetUrl,
        payload,
        merchantId,
        eventType,
        deliveryId,
      });

      return res.status(202).json({
        status: 'queued',
        deliveryId,
        merchantId,
        eventType,
        url: targetUrl,
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to queue test webhook';
      logger.error({ err, merchantId }, 'Failed to enqueue test webhook');

      if (message.includes('queue at capacity')) {
        return res.status(503).json({ error: message });
      }

      return res.status(500).json({ error: 'Failed to queue test webhook' });
    }
  });

  return router;
}
