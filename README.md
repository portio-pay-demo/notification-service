# PortIOPay Notification Service

Multi-channel notification delivery service for [PortIOPay](https://github.com/portio-pay-demo). Handles SMS and webhook delivery for payment events, with Redis-backed job queues, idempotent retries, dead-letter queue support, and backpressure handling.

## Overview

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

## Tech Stack

| Layer | Technology |
|-------|------------|
| Runtime | Node.js 20+ |
| Language | TypeScript 5 |
| HTTP server | Express 4 |
| Job queues | BullMQ 5 + ioredis |
| SMS provider | Twilio |
| Logging | Pino |
| Validation | Zod (dependencies) |

Additional libraries in `package.json` (SendGrid, PostgreSQL) support planned email and persistence features; the current implementation focuses on SMS and webhook delivery via Redis queues.

## Prerequisites

- **Node.js** 20 or later
- **Redis** 6+ (required for BullMQ queues)
- **Twilio account** (required for SMS delivery)
- **npm** (ships with Node.js)

## Local Development

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

Create a `.env` file in the project root (see [Environment variables](#environment-variables)):

```bash
cp .env.example .env   # if .env.example exists; otherwise create .env manually
```

At minimum, set Twilio credentials for SMS processing.

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
{"status":"ok","service":"notification-service","version":"1.8.3"}
```

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `PORT` | No | `3001` | HTTP server port |
| `REDIS_HOST` | No | `localhost` | Redis host for BullMQ |
| `REDIS_PORT` | No | `6379` | Redis port |
| `TWILIO_ACCOUNT_SID` | Yes (SMS) | — | Twilio account SID |
| `TWILIO_AUTH_TOKEN` | Yes (SMS) | — | Twilio auth token |
| `TWILIO_FROM_NUMBER` | Yes (SMS) | — | Twilio sender phone number (E.164) |
| `LOG_LEVEL` | No | `info` | Pino log level (`debug`, `info`, `warn`, `error`) |

Store secrets in `.env` locally (gitignored). Do not commit credentials.

## API Endpoints

All routes are prefixed with `/api/v1` unless noted.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Liveness check; returns service name and version |
| `GET` | `/api/v1/notifications/:id/status` | Returns delivery status for a notification ID |

### Example

```bash
curl http://localhost:3001/api/v1/notifications/abc-123/status
```

```json
{"id":"abc-123","status":"delivered"}
```

### Background processors

Most delivery work runs asynchronously via BullMQ workers started at boot:

| Processor | Queue | Purpose |
|-----------|-------|---------|
| `SmsProcessor` | `sms` | Sends SMS via Twilio with idempotency-key deduplication |
| `WebhookProcessor` | `webhooks` | POSTs JSON payloads to merchant webhook URLs |
| `WebhookProcessor` (DLQ) | `webhooks-dlq` | Stores webhooks that exhausted retry attempts |

Jobs are enqueued programmatically via `SmsProcessor.enqueue()` and `WebhookProcessor.enqueue()`. Upstream services (e.g. payment event bus) push jobs into Redis queues rather than calling send endpoints directly.

#### SMS job payload

```typescript
{
  to: string;              // E.164 phone number
  message: string;
  merchantId: string;
  idempotencyKey: string;  // Used as BullMQ job ID to prevent duplicates
}
```

#### Webhook job payload

```typescript
{
  url: string;
  payload: Record<string, unknown>;
  merchantId: string;
  eventType: string;
  deliveryId: string;      // Used as BullMQ job ID
}
```

Webhook deliveries include headers `X-PortIOPay-Delivery` and `X-PortIOPay-Event`. Failed deliveries retry up to 5 times with exponential backoff before moving to the dead-letter queue.

## Project Structure

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
├── package.json
├── tsconfig.json
└── CODEOWNERS
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

The service is a stateless Node.js process that requires a reachable Redis instance and Twilio credentials.

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

A `200` response with `"status":"ok"` indicates the process is running. For deeper readiness, ensure Redis connectivity (workers fail to process jobs if Redis is unavailable).

### Operational notes

- **Redis** must be highly available; all job state lives in Redis.
- **Concurrency**: SMS workers run 10 concurrent jobs; webhook workers run 50.
- **Backpressure**: Webhook intake pauses when queue depth reaches 50,000 jobs.
- **Idempotency**: SMS retries check a Redis delivery receipt (`sms:delivered:{key}`) to avoid duplicate sends.

## Ownership

| | |
|---|---|
| Team | PortIOPay Payments |
| Code owners | See [CODEOWNERS](./CODEOWNERS) — `@notifications-team`, `@sms-leads`, `@email-leads` |
| On-call | PagerDuty service `portioapay-notifications-prod` |

Pull requests require approval from the relevant code owners listed in `CODEOWNERS`.

## Related Fixes

Recent reliability improvements referenced in source:

- **NP-6 / NP-2033**: Webhook dead-letter queue and backpressure (no silent event drops)
- **NP-2032**: Memory-safe webhook processor (no closure leak on retry)
- **NP-2038**: SMS idempotency check before retry to prevent duplicate sends
