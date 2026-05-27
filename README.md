# PortIOPay Notification Service

Multi-channel notification delivery service for [PortIOPay](https://github.com/portio-pay-demo). Handles **SMS** (Twilio) and **webhook** delivery for payment events via **BullMQ** queues on **Redis**, with idempotent SMS retries, webhook dead-letter queue support, and backpressure handling.

| | |
|---|---|
| **Repository** | `portio-pay-demo/notification-service` |
| **Runtime** | Node.js 20+ |
| **Language** | TypeScript |
| **Default port** | `3001` |

## Tech stack

| Layer | Technology |
|-------|------------|
| Runtime | Node.js 20+ |
| HTTP | Express 4 |
| Queues | BullMQ 5, ioredis |
| SMS | Twilio |
| Logging | Pino |

## Prerequisites

- Node.js 20+
- Redis 6+ (required for BullMQ)
- Twilio credentials (required when processing SMS jobs)

## Local development

```bash
git clone https://github.com/portio-pay-demo/notification-service.git
cd notification-service
npm install
cp .env.example .env   # fill in values
npm run dev
```

Build and run production output:

```bash
npm run build
npm start
```

## Environment variables

| Variable | Description | Default |
|----------|-------------|---------|
| `PORT` | HTTP listen port | `3001` |
| `LOG_LEVEL` | Pino log level | `info` |
| `REDIS_HOST` | Redis hostname | `localhost` |
| `REDIS_PORT` | Redis port | `6379` |
| `TWILIO_ACCOUNT_SID` | Twilio account SID | — |
| `TWILIO_AUTH_TOKEN` | Twilio auth token | — |
| `TWILIO_FROM_NUMBER` | Sender phone number | — |

Copy `.env.example` to `.env` for local development. Never commit secrets.

## API

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Service health check |
| `GET` | `/api/v1/notifications/:id/status` | Notification delivery status |

SMS and webhook jobs are enqueued through `SmsProcessor` and `WebhookProcessor` (BullMQ workers started at boot).

## Project structure

```
src/
  index.ts              # Express app entrypoint
  sms/SmsProcessor.ts   # Twilio SMS worker
  webhook/WebhookProcessor.ts
  shared/               # Router, Redis, logger
```

## Scripts

| Command | Description |
|---------|-------------|
| `npm run dev` | Start with hot reload (`tsx watch`) |
| `npm run build` | Compile TypeScript to `dist/` |
| `npm start` | Run compiled `dist/index.js` |
| `npm test` | Run Jest tests |
| `npm run lint` | ESLint on `src/` |

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

Pull requests require approval from the relevant code owners.
