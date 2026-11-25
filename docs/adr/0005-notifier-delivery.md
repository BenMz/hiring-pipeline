# 5. Notifier: at-least-once consumption, deduplicated delivery

Status: accepted (2026-11-25)

## Context

The notifier (`services/notifier`, TypeScript) emails candidates when their
application is received or changes stage. Kafka consumer groups deliver at
least once: after a crash or rebalance, records since the last committed
offset are delivered again. The log itself can also contain the same event
twice. Sending an email is a side effect outside any transaction, so it can't
be made exactly-once by committing offsets cleverly.

## Decision

- **Its own consumer group** (`pipeline-notifier`) with Kafka-committed
  offsets, so its progress is independent of the projector's.
- **Deduplicate by event ID.** Event IDs are deterministic (UUIDv5 of stream and
  version), so a redelivered or re-produced event has the same ID. The
  notifier's delivery log, `notifier.deliveries`, has the event ID as its
  primary key.
- **Insert the delivery row, send, then commit, in one transaction.** A
  concurrent attempt for the same event (a redelivery during a rebalance)
  blocks on the row lock and then sees the row. A failed send rolls back, so the
  retry sends.
- **Transient failures block the partition; they never skip.** If Postgres or
  SMTP is down, the handler throws, kafkajs retries, and the offset doesn't
  move. Only a record that can't be decoded is logged and skipped.
- **State comes from the log, not from the read models.** Contacts and job
  titles are built from `ApplicationSubmitted` and `JobOpened` into the
  notifier's own `notifier` schema. An application's events share a partition,
  so its contact is always known before its stage changes. The job title may be
  on another partition and not seen yet, so templates fall back to "the role
  you applied for" rather than wait.
- **No emails about the past.** A new group starts from offset 0 to learn
  contacts. Events older than `NOTIFY_MAX_AGE` (default 24h) update state but
  aren't sent.
- **The rejection reason is never emailed.** It is a recruiter's internal note.
- **Snappy support.** franz-go compresses batches with snappy by default and
  kafkajs only ships gzip, so the notifier registers a snappy codec. This
  was found by running the stack: the first integration test produced with
  kafkajs, which writes uncompressed batches, and passed anyway. The
  test now produces with `rpk`, which is built on franz-go.

## Consequences

- **One remaining duplicate window.** If the mail server accepts the message
  and the notifier crashes before `COMMIT`, the retry sends it again. Closing
  that needs the provider's cooperation: an idempotency key on the send API
  (most transactional email APIs have one), or a mail system that drops repeated
  `Message-ID`s. The notifier sets a stable `Message-ID` and an
  `X-Pipeline-Event-Id` header to make that possible. Losing an email is
  worse than a rare duplicate, so this trade is deliberate.
- The Postgres transaction stays open for the duration of an SMTP send. That
  is fine at hiring volumes, but not for high-throughput notifications.
- **kafkajs is no longer actively maintained** (last release 2023). It was
  chosen per the project plan and works with Redpanda. The Kafka code is
  isolated in `src/consumer.ts` and `src/snappy.ts`, and a move to
  `@confluentinc/kafka-javascript` (librdkafka, near-identical API) would only
  touch those files.
- During the load test the notifier fell 51k events behind and stole Postgres
  CPU from the projector, which skewed the first rebuild timings. In production
  it should have its own database or at least separate resource limits.
