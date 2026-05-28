# PortIOPay Notification Service

Multi-channel notification delivery service for [PortIOPay](https://github.com/portio-pay-demo). Processes **SMS** (Twilio) and **merchant webhooks** for payment-related events using **BullMQ** workers backed by **Redis**, with idempotent SMS retries, webhook dead-letter queue (DLQ) support, and queue backpressure handling.

| | |
|---|---|
| **Repository** | [portio-pay-demo/notification-service](https://github.com/portio-pay-demo/notification-service) |
| **Runtime** | Node.js 20+ |
| **Language** | TypeScript |
| **Default port** | `3001` |
| **Version** | `1.8.3` (see `package.json`) |

## What it does

On startup, the service:

1. Starts an **Express** HTTP server for health checks and status queries.
2. Starts **BullMQ workers** for SMS (`SmsProcessor`) and webhooks (`WebhookProcessor`).
3. Connects to **Redis** for queues, job deduplication, and SMS delivery receipts.

Upstream services enqueue work by calling the processor APIs in-process (or via future HTTP routes). SMS jobs use an **idempotency key** stored in Redis to prevent duplicate sends on retry. Webhook jobs move to a **DLQ** after max retries and reject new work when the queue exceeds a depth threshold.

## Tech stack

| Layer | Technology |
|-------|------------|
| Runtime | Node.js 20+ |
| Language | TypeScript 5 |
| HTTP | Express 4 |
| Job queues | BullMQ 5, ioredis |
| SMS | Twilio |
| Logging | Pino, pino-http |
| Validation | Zod (dependencies; extend as routes grow) |

`package.json` also lists SendGrid and PostgreSQL clients for planned email and persistence features; the current `src/` tree implements SMS and webhooks only.

## Prerequisites

- **Node.js** 20 or newer
- **Redis** 6+ (required for BullMQ)
- **Twilio** account (required when the SMS worker processes jobs)

## Local development

```bash
git clone https://github.com/portio-pay-demo/notification-service.git
cd notification-service
npm install
cp .env.example .env   # set Twilio and Redis values
npm run dev
```

The dev server listens on `http://localhost:3001` (or `PORT` from the environment) and reloads on file changes via `tsx watch`.

Build and run the compiled service:

```bash
npm run build
npm start
```

### Scripts

| Command | Description |
|---------|-------------|
| `npm run dev` | Start with hot reload (`tsx watch src/index.ts`) |
| `npm run build` | Compile TypeScript to `dist/` |
| `npm start` | Run `node dist/index.js` |
| `npm test` | Run Jest tests |
| `npm run test:coverage` | Jest with coverage |
| `npm run lint` | ESLint on `src/**/*.ts` |

## Environment variables

| Variable | Description | Default |
|----------|-------------|---------|
| `PORT` | HTTP listen port | `3001` |
| `LOG_LEVEL` | Pino log level (`debug`, `info`, `warn`, `error`) | `info` |
| `REDIS_HOST` | Redis hostname | `localhost` |
| `REDIS_PORT` | Redis port | `6379` |
| `TWILIO_ACCOUNT_SID` | Twilio account SID | — (required for SMS) |
| `TWILIO_AUTH_TOKEN` | Twilio auth token | — (required for SMS) |
| `TWILIO_FROM_NUMBER` | E.164 sender number for outbound SMS | — (required for SMS) |

Copy [`.env.example`](./.env.example) to `.env` for local development. Do not commit secrets.

## API (HTTP)

All routes are mounted under `/api/v1` except `/health`.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Liveness: `{ status, service, version }` |
| `GET` | `/api/v1/notifications/:id/status` | Delivery status for a notification ID (stub returns `delivered`) |

### Background processing (programmatic)

Jobs are enqueued through TypeScript classes started in `src/index.ts`:

| Component | Queue name | Purpose |
|-----------|------------|---------|
| `SmsProcessor` | `sms` | Send SMS via Twilio; dedupe by `idempotencyKey`; 3 attempts, exponential backoff |
| `WebhookProcessor` | `webhooks` | POST JSON to merchant URLs; DLQ `webhooks-dlq` after failures; backpressure at 50k depth |

**SMS job payload** (`SmsProcessor.enqueue`):

```ts
{ to: string; message: string; merchantId: string; idempotencyKey: string }
```

**Webhook job payload** (`WebhookProcessor.enqueue`):

```ts
{ url: string; payload: Record<string, unknown>; merchantId: string; eventType: string; deliveryId: string }
```

Webhook requests include headers `X-PortIOPay-Delivery` and `X-PortIOPay-Event`, with a 10s timeout.

## Project structure

```
src/
  index.ts                    # Express app, starts processors
  sms/SmsProcessor.ts         # Twilio + BullMQ SMS worker
  webhook/WebhookProcessor.ts # HTTP webhook delivery + DLQ
  shared/
    router.ts                 # HTTP routes
    redis.ts                  # ioredis client (BullMQ)
    logger.ts                 # Pino logger
```

## Deployment

This repository ships application code only; deploy it as a stateless Node.js service with a managed Redis instance.

### Recommended production setup

1. **Build** — `npm ci && npm run build` in CI.
2. **Image** — Use a Node 20 slim base image; set `CMD ["node", "dist/index.js"]`.
3. **Config** — Inject environment variables from your secrets manager (Twilio, Redis host/port).
4. **Redis** — Use a highly available Redis cluster; BullMQ requires `maxRetriesPerRequest: null` on the client (already set in `src/shared/redis.ts`).
5. **Health checks** — Probe `GET /health` on the service port.
6. **Scaling** — Run multiple replicas for HTTP; coordinate BullMQ worker concurrency per replica (`SmsProcessor`: 10, `WebhookProcessor`: 50) to avoid overwhelming Twilio or merchant endpoints.
7. **Observability** — Structured JSON logs via Pino; alert on DLQ growth and `Webhook queue at capacity` errors.

Example container run (after build):

```bash
docker build -t notification-service .
docker run --rm -p 3001:3001 \
  -e REDIS_HOST=redis.example.com \
  -e TWILIO_ACCOUNT_SID=... \
  -e TWILIO_AUTH_TOKEN=... \
  -e TWILIO_FROM_NUMBER=+1... \
  notification-service
```

Deploy to Kubernetes, ECS, or your platform of choice using the same env vars and health probe path.

## Ownership

Code ownership is defined in [`CODEOWNERS`](./CODEOWNERS):

| Path | Reviewers |
|------|-----------|
| `*` | `@notifications-team` |
| `src/sms/` | `@sms-leads`, `@notifications-team` |
| `src/webhook/` | `@notifications-team` |
| `src/email/` | `@email-leads`, `@notifications-team` (planned) |

- **Team:** PortIOPay Payments
- **On-call:** PagerDuty `portioapay-notifications-prod`

Pull requests require approval from the listed code owners.
