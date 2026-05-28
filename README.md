# PortIOPay Notification Service

Multi-channel notification delivery service for PortIOPay. Handles SMS and webhook delivery for payment events with queue-based processing, retry logic, and dead-letter queue support.

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
cp .env.example .env   # configure Redis and Twilio credentials
npm run dev
```

Requires: Node.js 20+, Redis

## API

```
POST /api/v1/notifications/send
POST /api/v1/webhooks/register
POST /api/v1/webhooks/test
GET  /api/v1/notifications/{id}/status
GET  /health
```

## Ownership

Code ownership is defined in [`CODEOWNERS`](./CODEOWNERS).

- Team: **PortIOPay Payments**
- On-call: PagerDuty service `portioapay-notifications-prod`

Pull requests require approval from the relevant code owners.
