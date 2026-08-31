import express from 'express';
import pinoHttp from 'pino-http';
import { createNotificationRouter } from './shared/router';
import { WebhookProcessor } from './webhook/WebhookProcessor';
import { SmsProcessor } from './sms/SmsProcessor';
import { logger } from './shared/logger';
import { redis } from './shared/redis';

const app = express();
const PORT = process.env.PORT || 3001;

app.use(express.json());
app.use(pinoHttp());

const webhookProcessor = new WebhookProcessor();
const smsProcessor = new SmsProcessor();

app.use('/api/v1', createNotificationRouter(smsProcessor, webhookProcessor));

app.get('/health', (_req, res) => {
  res.json({ status: 'ok', service: 'notification-service', version: '1.8.3' });
});

async function shutdown(signal: string) {
  logger.info({ signal }, 'Shutting down notification-service');
  await webhookProcessor.close();
  await smsProcessor.close();
  await redis.quit();
  process.exit(0);
}

async function start() {
  await webhookProcessor.start();
  await smsProcessor.start();

  app.listen(PORT, () => {
    logger.info({ port: PORT }, 'notification-service listening');
  });

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}

start().catch((err) => {
  logger.fatal({ err }, 'Failed to start notification-service');
  process.exit(1);
});
