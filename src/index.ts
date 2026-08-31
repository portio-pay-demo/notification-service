import express from 'express';
import pinoHttp from 'pino-http';
import { notificationRouter } from './shared/router';
import { WebhookProcessor } from './webhook/WebhookProcessor';
import { WebhookRegistry } from './webhook/WebhookRegistry';
import { createWebhookRouter } from './webhook/webhookRoutes';
import { SmsProcessor } from './sms/SmsProcessor';

const app = express();
const PORT = process.env.PORT || 3001;

const webhookProcessor = new WebhookProcessor();
const webhookRegistry = new WebhookRegistry();

app.use(express.json());
app.use(pinoHttp());

app.use('/api/v1', notificationRouter);
app.use('/api/v1', createWebhookRouter(webhookRegistry, webhookProcessor));

app.get('/health', (_req, res) => {
  res.json({ status: 'ok', service: 'notification-service', version: '1.8.3' });
});

async function start() {
  const smsProcessor = new SmsProcessor();

  await webhookProcessor.start();
  await smsProcessor.start();

  app.listen(PORT, () => {
    console.log(`notification-service listening on :${PORT}`);
  });
}

start().catch((err) => {
  console.error('Failed to start notification-service:', err);
  process.exit(1);
});
