# notification-service

Multi-channel notification delivery service for PortIOPay. Handles SMS, email, and webhook delivery for payment events with intelligent batching, retry logic, and dead-letter queue support.

## Overview

**Tech Stack:** Node.js 20, TypeScript, BullMQ (Redis), SendGrid, Twilio, PostgreSQL

## Key Features

- SMS via Twilio with idempotent retry (circuit breaker, deduplication key)
- Email via SendGrid with template rendering
- Webhook delivery with dead-letter queue (NP-2033 fix: backpressure handling)
- Intelligent batching: configurable per channel (email: 100, SMS: 50, push: 500)
- Memory-safe event processor — fixed closure leak (NP-2032)

## Local Development

```bash
npm install
npm run dev
```

Requires: Node.js 20+, Redis, PostgreSQL

```bash
docker compose up -d
npm run dev
```

## API

```
POST /api/v1/notifications/send
POST /api/v1/webhooks/register
POST /api/v1/webhooks/test
GET  /api/v1/notifications/{id}/status
GET  /health
```

## Ownership

- Team: **PortIOPay Payments**
- CODEOWNERS: `@notifications-team`, `@sms-leads`, `@email-leads`
- On-call: PagerDuty service `portioapay-notifications-prod`

- Test claude code review
