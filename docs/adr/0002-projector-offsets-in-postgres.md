# 2. Projector offsets live in PostgreSQL, next to the read models

Status: accepted (2026-11-24)

## Context

The projector reads `applications.events` and updates the read models
(`jobs`, `applications_by_job`, `stage_counts`). If it commits its Kafka offset
separately from the database write, a crash between the two either re-applies a
batch (offset committed after the write) or loses it (offset committed before
it). It must also be possible to throw the read models away and rebuild them
from offset 0.

## Decision

- **Offsets are stored in a `projector_offsets` table and written in the same
  transaction as the read-model changes.** Nothing is committed to Kafka. On
  start the projector reads its offsets from PostgreSQL and assigns itself the
  partitions at those positions.
- **Every statement is still idempotent.** Each application row stores the
  version of the last event applied to it. An event that isn't newer, or that
  doesn't start from the row's current stage, changes nothing, and funnel counts
  only change when the row did. Redelivery is harmless even without the
  transactional offsets.
- **Rebuild is a database operation.** `pipeline-projector rebuild` drops the
  read-model tables and the offsets, recreates the schema, replays from offset 0
  and exits once it has caught up.
- **One projector at a time.** The projector holds a PostgreSQL advisory lock
  for as long as it runs. A rebuild (or a second projector) fails fast with a
  clear error instead of interleaving with a live projector.

## Consequences

- The effect is exactly-once without Kafka transactions: a batch and its
  offsets are committed together or not at all.
- Because no consumer group is used, the projector does not scale out by adding
  instances. To scale it, partitions would be split between instances and each
  would keep its own offsets, the same way.
- The read models are eventually consistent with the commands. In practice the
  lag is a few milliseconds locally, but clients should not expect to read a
  write instantly. The end-to-end tests wait for the projector explicitly
  instead of sleeping.
- There are no foreign keys between `applications_by_job` and `jobs`: they come
  from different partitions, so an application can be projected before its job.
