import { redis } from '../shared/redis';
import { logger } from '../shared/logger';

export interface RegisteredWebhook {
  merchantId: string;
  url: string;
  eventTypes: string[];
  description?: string;
  registeredAt: string;
}

const REGISTRY_KEY_PREFIX = 'webhooks:registry:';

export class WebhookRegistry {
  private key(merchantId: string): string {
    return `${REGISTRY_KEY_PREFIX}${merchantId}`;
  }

  async register(webhook: Omit<RegisteredWebhook, 'registeredAt'>): Promise<RegisteredWebhook> {
    const entry: RegisteredWebhook = {
      ...webhook,
      registeredAt: new Date().toISOString(),
    };

    await redis.set(this.key(webhook.merchantId), JSON.stringify(entry));
    logger.info({ merchantId: webhook.merchantId, url: webhook.url }, 'Webhook endpoint registered');

    return entry;
  }

  async get(merchantId: string): Promise<RegisteredWebhook | null> {
    const raw = await redis.get(this.key(merchantId));
    if (!raw) {
      return null;
    }

    return JSON.parse(raw) as RegisteredWebhook;
  }

  async unregister(merchantId: string): Promise<boolean> {
    const deleted = await redis.del(this.key(merchantId));
    if (deleted > 0) {
      logger.info({ merchantId }, 'Webhook endpoint unregistered');
    }
    return deleted > 0;
  }
}
