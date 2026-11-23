# 1. Kafka as the event store

Status: accepted (2026-11-23)

## Context

Every change to a job or application is an event, and events are the source of
truth. The command service must (a) append events for one aggregate in order
and (b) rebuild an aggregate from its events before deciding the next command.
Kafka is good at (a) and has no native support for (b): it cannot fetch
"all records with key X", and it has no conditional append ("write only if the
stream is still at version N").

## Decision

- **One topic, `applications.events`, keyed by aggregate ID.** All events of an
  application (or job) go to one partition, so they are totally ordered. The
  partition is `fnv32a(id) % partitions`, set explicitly on each record.
- **Infinite retention, no compaction.** Compaction keeps only the latest record
  per key, which would delete an application's history.
- **The command service tails the whole topic into an in-memory index**
  (stream ID -> events). It replays the topic on startup before reporting
  healthy.
- **Load is read-your-writes.** Load fetches the partition's current end offset
  and waits until the index has consumed up to it. A Load therefore sees every
  acknowledged append, including appends from other processes, without polling.
- **Appends are guarded twice.** Commands on one stream are serialised by a
  per-stream lock, and Append checks the expected version against the index.
  The producer uses `acks=all` with idempotent writes.
- **Retries are made safe by idempotency keys, not by the transport.** The key is
  stored in every event, so after a restart the aggregate still recognises a
  retried command and returns the original result. New aggregate IDs are
  derived from the key (UUIDv5), so a retried `SubmitApplication` finds the
  application it created the first time. Event IDs are derived from
  (stream, version), so a duplicate record carries the same ID and consumers
  can drop it.

## Consequences

- Fast loads (memory lookup plus one `ListOffsets` round trip) at the cost of
  holding every event in memory. That is fine for this domain; snapshots are the
  next step if it isn't.
- **Single writer per partition.** Between the version check and the produce
  there is a window in which a second command-service process could append to
  the same stream. Running one instance (or assigning partitions to instances,
  e.g. through a consumer group) closes it. A store with conditional appends
  (EventStoreDB, or a `UNIQUE (stream_id, version)` constraint in PostgreSQL)
  would remove the restriction.
- The partition count must never change, because that would move streams to
  other partitions and break ordering.
- If a request times out after the record was sent but before the ack, the
  client cannot tell whether it was written. Retrying with the same idempotency
  key is always safe: either the first write happened and the retry is a no-op,
  or it didn't and the retry writes it.
