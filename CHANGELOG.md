# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.8.3] - 2026-03-10

### Fixed

- **NP-2033:** Webhook queue backpressure — failed deliveries now route to a dead-letter queue instead of being dropped when queue depth exceeds limits.
- **NP-2032:** Memory leak in webhook retry loop — event payloads are no longer retained in closures; job data is serialized to Redis only.
- **NP-2038:** Duplicate SMS sends on retry — idempotency keys are checked via Redis delivery receipts before each send attempt.

### Changed

- Renamed CI workflow from `ci.yml` to `claude-review.yml`.

## [1.8.0] - 2026-03-07

### Added

- README documentation for local development, API endpoints, and team ownership.

## [1.0.0] - 2026-03-06

### Added

- Initial PortIOPay notification-service implementation.
- Multi-channel delivery: SMS (Twilio), email (SendGrid), and webhooks.
- BullMQ-backed job queues with Redis.
- REST API: `POST /api/v1/notifications/send`, webhook registration and test endpoints, notification status lookup, and `/health`.
- Intelligent batching configuration per channel (email, SMS, push).
- Express + TypeScript service scaffold with structured logging (Pino).
