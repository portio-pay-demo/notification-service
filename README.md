# PortIOPay Notification Service

Multi-channel notification delivery service for [PortIOPay](https://github.com/portio-pay-demo). It handles outbound **SMS** (Twilio) and **webhook** delivery for payment events, exposes a small HTTP API for health checks and notification status, and processes work asynchronously through **BullMQ** queues backed by **Redis** — with idempotent SMS retries, webhook dead-letter queue support, and backpressure handling.

| | |
|---|---|
| **Repository** | `portio-pay-demo/notification-service` |
| **Runtime** | Node.js 20+ |
| **Language** | TypeScript |
| **Default port** | `3001` |

## Table of contents

- [Architecture](#architecture)
- [Tech stack](#tech-stack)
- [Prerequisites](#prerequisites)
- [Local development](#local-development)
- [Environment variables](#environment-variables)
- [API reference](#api-reference)
- [Background processors](#background-processors)
- [Project structure](#project-structure)
- [Scripts](#scripts)
- [Deployment](#deployment)
- [Ownership](#ownership)
- [Related fixes](#related-fixes)

## Architecture

The service exposes a small HTTP API for health checks and delivery status, and runs background workers that process notification jobs from BullMQ queues. SMS messages are sent via Twilio; webhooks are delivered over HTTP with automatic retries and DLQ routing on failure.

```
┌─────────────┐     HTTP      ┌──────────────────────────┐
│   Clients   │──────────────▶│  notification-service    │
│  / upstream │               │  (Express, port 3001)    │
└─────────────┘               └────────────┬─────────────┘
                                           │
                              ┌────────────┴─────────────┐
                              │         Redis            │
                              │  (BullMQ job queues)     │
                              └────────────┬─────────────┘
                                           │
                    ┌──────────────────────┼──────────────────────┐
                    ▼                      ▼                      ▼
             ┌────────────┐        ┌────────────┐        ┌────────────┐
             │ SmsProcessor│        │WebhookProc.│        │ webhooks-  │
             │  (Twilio)  │        │  (HTTP)    │        │    dlq     │
             └────────────┘        └────────────┘        └────────────┘
```

On startup, the service:

1. Starts **WebhookProcessor** and **SmsProcessor** workers (BullMQ).
2. Listens for HTTP traffic on `PORT` (default `3001`).

SMS and webhook jobs are enqueued programmatically via the processor classes (`enqueue` methods). Additional HTTP routes for send/register flows may be added under `src/shared/router.ts`.

## Tech stack

| Layer | Technology |
|-------|------------|
| Runtime | Node.js 20+ |
| Language | TypeScript 5 |
| HTTP server | [Express](https://expressjs.com/) 4 |
| Job queues | [BullMQ](https://docs.bullmq.io/) 5 + [ioredis](https://github.com/redis/ioredis) |
| SMS provider | [Twilio](https://www.twilio.com/) |
| Logging | [Pino](https://getpino.io/) |
| Validation | [Zod](https://zod.dev/) (dependencies) |

Additional libraries in `package.json` (SendGrid, PostgreSQL) support planned email and persistence features; the current implementation focuses on SMS and webhook delivery via Redis queues.

## Prerequisites

- **Node.js** 20 or later (`engines` in `package.json`)
- **Redis** 6+ reachable from the app (required for BullMQ)
- **Twilio** account credentials (required when processing SMS jobs)
- **npm** (ships with Node.js)

## Local development

### 1. Clone and install

```bash
git clone https://github.com/portio-pay-demo/notification-service.git
cd notification-service
npm install
```

### 2. Start Redis

If Redis is not already running locally:

```bash
docker run -d --name notification-redis -p 6379:6379 redis:7-alpine
```

Or install and start Redis via your OS package manager.

### 3. Configure environment

Create a `.env` file in the project root (see [Environment variables](#environment-variables)). At minimum for local SMS testing:

```bash
TWILIO_ACCOUNT_SID=your_account_sid
TWILIO_AUTH_TOKEN=your_auth_token
TWILIO_FROM_NUMBER=+15551234567
```

### 4. Run the service

**Development** (hot reload via `tsx`):

```bash
npm run dev
```

**Production build**:

```bash
npm run build
npm start
```

The service listens on `http://localhost:3001` by default.

### 5. Verify

```bash
curl http://localhost:3001/health
```

Expected response:

```json
{
  "status": "ok",
  "service": "notification-service",
  "version": "1.8.3"
}
```

## Environment variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `PORT` | No | `3001` | HTTP server port |
| `LOG_LEVEL` | No | `info` | Pino log level (`debug`, `info`, `warn`, `error`) |
| `REDIS_HOST` | No | `localhost` | Redis host for BullMQ |
| `REDIS_PORT` | No | `6379` | Redis port |
| `TWILIO_ACCOUNT_SID` | Yes (SMS) | — | Twilio account SID |
| `TWILIO_AUTH_TOKEN` | Yes (SMS) | — | Twilio auth token |
| `TWILIO_FROM_NUMBER` | Yes (SMS) | — | Twilio sender phone number (E.164) |

Store secrets in `.env` locally (`.env` and `.env.local` are gitignored). Do not commit credentials.

## API reference

Base path for versioned routes: `/api/v1`

### Implemented routes

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Liveness check; returns service name and version |
| `GET` | `/api/v1/notifications/:id/status` | Returns delivery status for a notification ID (stub returns `delivered`) |

**Example — notification status**

```bash
curl http://localhost:3001/api/v1/notifications/ntf_abc123/status
```

```json
{
  "id": "ntf_abc123",
  "status": "delivered"
}
```

### Planned / platform contract routes

The following endpoints are part of the PortIOPay notification platform contract and may be implemented in `src/shared/router.ts` as the API surface grows:

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/notifications/send` | Enqueue multi-channel notification (SMS, email, push) |
| `POST` | `/api/v1/webhooks/register` | Register merchant webhook endpoint |
| `POST` | `/api/v1/webhooks/test` | Send test payload to a registered webhook |

## Background processors

Most delivery work runs asynchronously via BullMQ workers started at boot:

| Processor | Queue | Purpose |
|-----------|-------|---------|
| `SmsProcessor` | `sms` | Sends SMS via Twilio with idempotency-key deduplication |
| `WebhookProcessor` | `webhooks` | POSTs JSON payloads to merchant webhook URLs |
| `WebhookProcessor` (DLQ) | `webhooks-dlq` | Stores webhooks that exhausted retry attempts |

Jobs are enqueued via `SmsProcessor.enqueue()` and `WebhookProcessor.enqueue()`. Upstream services (e.g. payment event bus) push jobs into Redis queues rather than calling send endpoints directly.

### SMS (`SmsProcessor`)

- **Idempotency:** Jobs use `idempotencyKey` as BullMQ `jobId`; Redis key `sms:delivered:{key}` prevents duplicate sends on retry (NP-2038)
- **Retries:** 3 attempts, exponential backoff starting at 1s
- **Concurrency:** 10 workers

```typescript
await smsProcessor.enqueue({
  to: '+15559876543',
  message: 'Your payment was received.',
  merchantId: 'merch_123',
  idempotencyKey: 'pay_evt_unique_id',
});
```

### Webhooks (`WebhookProcessor`)

- **Backpressure:** Refuses new jobs when queue depth ≥ 50,000 (NP-2033)
- **Retries:** 5 attempts, exponential backoff starting at 2s
- **Headers:** `X-PortIOPay-Delivery`, `X-PortIOPay-Event`
- **Timeout:** 10s per delivery attempt

```typescript
await webhookProcessor.enqueue({
  url: 'https://merchant.example/webhooks/portiopay',
  payload: { event: 'payment.completed', amount: 1000 },
  merchantId: 'merch_123',
  eventType: 'payment.completed',
  deliveryId: 'dlv_unique_id',
});
```

## Project structure

```
notification-service/
├── src/
│   ├── index.ts              # Express app entry point; starts workers
│   ├── shared/
│   │   ├── router.ts         # HTTP route definitions
│   │   ├── redis.ts          # Redis / BullMQ connection
│   │   └── logger.ts         # Structured logging (Pino)
│   ├── sms/
│   │   └── SmsProcessor.ts   # Twilio SMS worker
│   └── webhook/
│       └── WebhookProcessor.ts  # Webhook delivery worker + DLQ
├── CODEOWNERS
├── package.json
└── tsconfig.json
```

## Scripts

| Command | Description |
|---------|-------------|
| `npm run dev` | Start with hot reload (`tsx watch`) |
| `npm run build` | Compile TypeScript to `dist/` |
| `npm start` | Run compiled output (`node dist/index.js`) |
| `npm test` | Run Jest test suite |
| `npm run test:coverage` | Run tests with coverage report |
| `npm run lint` | ESLint on `src/` |

## Deployment

The service is a stateless Node.js process that requires a reachable Redis instance and Twilio credentials. This repository does not include a `Dockerfile` or Kubernetes manifests — deploy as a standard Node.js service (ECS, Kubernetes, VM, etc.).

### Build and run

```bash
npm ci
npm run build
npm start
```

Set `PORT`, `REDIS_HOST`, `REDIS_PORT`, and Twilio variables in your deployment environment (Kubernetes secrets, ECS task definition, etc.).

### Health checks

Configure your load balancer or orchestrator to probe:

```
GET /health
```

A `200` response with `"status":"ok"` indicates the process is running. For deeper readiness, ensure Redis connectivity (workers cannot process jobs if Redis is unavailable).

### Operational notes

- **Redis** must be highly available; all job state lives in Redis.
- **Concurrency**: SMS workers run 10 concurrent jobs; webhook workers run 50.
- **Backpressure**: Webhook intake pauses when queue depth reaches 50,000 jobs.
- **Idempotency**: SMS retries check a Redis delivery receipt (`sms:delivered:{key}`) to avoid duplicate sends.
- Run at least **2 replicas** for availability in production.
- Monitor BullMQ queue depth for `webhooks` and `sms`; alert on DLQ growth (`webhooks-dlq`).

### CI

Pull requests run the shared **Claude Code Review** workflow (`.github/workflows/claude-review.yml`) from `portio-pay-demo/platform-workflows`.

## Ownership

Defined in [`CODEOWNERS`](./CODEOWNERS):

| Path | Reviewers |
|------|-----------|
| `*` | `@notifications-team` |
| `src/sms/` | `@sms-leads`, `@notifications-team` |
| `src/email/` | `@email-leads`, `@notifications-team` |

| | |
|---|---|
| Team | PortIOPay Payments |
| On-call | PagerDuty service `portioapay-notifications-prod` |

Pull requests require approval from the relevant code owners listed in `CODEOWNERS`.

## Related fixes

Recent reliability improvements referenced in source:

- **NP-6 / NP-2033**: Webhook dead-letter queue and backpressure (no silent event drops)
- **NP-2032**: Memory-safe webhook processor (no closure leak on retry)
- **NP-2038**: SMS idempotency check before retry to prevent duplicate sends
