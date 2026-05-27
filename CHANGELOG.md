# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.8.3] - 2026-05-27

### Added

- Initial multi-channel notification service (SMS, email, webhooks)
- Express API: `POST /api/v1/notifications/send`, webhook registration and status endpoints
- BullMQ-backed job processing with Redis
- SendGrid email and Twilio SMS integrations
- Webhook delivery with dead-letter queue support
- Intelligent per-channel batching (email, SMS, push)
- SMS idempotent retry with circuit breaker and deduplication

### Changed

- Claude Code Review workflow for pull requests

[Unreleased]: https://github.com/portio-pay-demo/notification-service/compare/main...HEAD
[1.8.3]: https://github.com/portio-pay-demo/notification-service/compare/260666f...main
