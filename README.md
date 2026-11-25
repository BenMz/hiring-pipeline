# hiring-pipeline

An event-sourced applicant pipeline for small businesses, in Go and TypeScript with gRPC and Kafka.

A company opens a job, candidates apply, and recruiters move each application through
`APPLIED → SCREENING → INTERVIEW → OFFER → HIRED` (or `REJECTED` from any open stage).
Every change is an event in the Kafka topic `applications.events`. Those events are the
source of truth. PostgreSQL holds read models only, and they can be rebuilt by replaying the topic.

Four services share one Protobuf schema: three in Go and a TypeScript notifier.

```
                    ┌──────────────────┐                          ┌─────────────────────┐
gRPC commands ───▶  │ pipeline-command │ ── append, keyed by ID ─▶│ Kafka (Redpanda)    │
  :50051            │ Go               │ ◀─ replay aggregates ────│ applications.events │
                    └──────────────────┘                          └───┬──────────────┬──┘
                                                consume; offsets kept │              │ consume; group
                                                in Postgres           ▼              ▼ pipeline-notifier
                    ┌──────────────────┐                  ┌────────────────────┐  ┌───────────────────┐
gRPC queries  ───▶  │ pipeline-query   │                  │ pipeline-projector │  │ pipeline-notifier │
  :50052            │ Go               │                  │ Go                 │  │ TypeScript        │
                    └────────┬─────────┘                  └───────────┬────────┘  └─────┬──────────┬──┘
                             │ read                                   │ write           │ state    │ SMTP
                             ▼                                        ▼                 ▼          ▼
                    ┌────────────────────────────────────────────────────────────────────────┐ ┌─────────┐
                    │ PostgreSQL: read models · notifier schema (contacts, delivery log)     │ │ Mailpit │
                    └────────────────────────────────────────────────────────────────────────┘ │ :8025   │
                                                                                               └─────────┘
```

- **pipeline-command** validates commands against the aggregate rebuilt from its events,
  then appends new events. Retries are safe: every command carries an idempotency key.
- **pipeline-projector** turns events into PostgreSQL read models. It can drop them and
  rebuild from offset 0, with identical results.
- **pipeline-query** serves the read models: paginated application lists and a per-job funnel.
- **pipeline-notifier** emails candidates when they apply or move stage. It runs in its own
  consumer group, and each event's email goes out once even when Kafka redelivers the event.

Decisions are recorded in [`docs/adr`](docs/adr):
[event sourcing](docs/adr/0003-event-sourcing.md),
[gRPC + Protobuf](docs/adr/0004-grpc.md),
[Kafka as the event store](docs/adr/0001-kafka-as-event-store.md),
[projector offsets](docs/adr/0002-projector-offsets-in-postgres.md),
[notifier delivery](docs/adr/0005-notifier-delivery.md).
One measurement: [rebuilding from 100k events takes about 10.5 s](docs/measurements/rebuild-100k.md),
and PostgreSQL is the bottleneck.

## Demo

```sh
docker compose up -d --build   # or: make up
make demo                      # needs grpcurl
```

[`scripts/demo.sh`](scripts/demo.sh) opens a job, submits two applications, retries a
command, tries an illegal stage skip, rejects a candidate, then reads the funnel and the
emails back. Abridged output:

```
== Move Ada to screening, then retry the same request (same key: no new event, same answer)
{ "version": "2" }
{ "version": "2" }

== Try to skip Ada straight to an offer (not allowed)
ERROR:
  Code: FailedPrecondition
  Message: invalid stage transition: SCREENING -> OFFER

== Funnel for the job (query service, from PostgreSQL read models)
  (JSON condensed) APPLIED 0 · SCREENING 0 · INTERVIEW 1 · OFFER 0 · HIRED 0 · REJECTED 1

== Emails the notifier sent (Mailpit, http://localhost:8025)
  ada.demo-…@example.com   ->  Interview invitation
  ada.demo-…@example.com   ->  We received your application for the Backend Engineer role
  ada.demo-…@example.com   ->  Your application is being reviewed
  alan.demo-…@example.com  ->  An update on your application
  alan.demo-…@example.com  ->  We received your application for the Backend Engineer role
```

Open <http://localhost:8025> to read the emails.

## Layout

```
proto/pipeline/v1/              events.proto, command.proto, query.proto
gen/go/                         generated Go code (committed; CI checks it is current)
services/command/
  cmd/pipeline-command/         main
  app/                          wiring (gRPC server, health, reflection, graceful shutdown)
  internal/application/, job/   aggregates: pure domain logic, no I/O
  internal/codec/               domain events <-> protobuf envelopes
  internal/eventstore/          Store interface; Kafka and in-memory implementations
  internal/server/              gRPC handlers, status-code mapping, deadline interceptor
services/projector/
  internal/projection/          idempotent SQL per event, offsets, reset
  internal/runner/              Kafka consumer; batch + offsets in one transaction
services/query/                 ListApplications, GetFunnel over PostgreSQL
migrations/                     read-model schema (embedded; applied by the projector)
internal/                       shared gRPC middleware and test helpers
test/integration/               Testcontainers: all services + real Redpanda + PostgreSQL
docs/adr/                       decision records
services/notifier/              TypeScript notifier (kafkajs, Jest, ESLint)
  src/notifier.ts               event -> state update + deduplicated email
  src/pg-store.ts               contacts, job titles and the delivery log (schema `notifier`)
  src/consumer.ts, snappy.ts    Kafka consumer group; snappy codec for franz-go's batches
  src/gen/                      generated TypeScript code
tools/loadgen/                  writes N events through the command API (for measuring)
scripts/demo.sh                 the demo above
docs/measurements/              the rebuild measurement
docker-compose.yml              Redpanda, PostgreSQL 16, Mailpit and all four services
```

## Domain rules

These are enforced by `services/command/internal/application` and written test-first:

- An application advances **one stage at a time**. Skipping, moving backwards and no-op moves are rejected.
- `RejectCandidate` is a separate command, allowed from any open stage. `HIRED` and `REJECTED` are terminal.
- Every command carries an **idempotency key**. A retry with the same key emits no new event and
  returns success. Reusing a key for a *different* command fails. Processed keys are rebuilt from
  the event stream itself, so retries are still recognised after a restart.
- `Load` replays a stream and rejects corrupt history (version gaps, events from another aggregate).

## How a command is handled

```
client ──gRPC──▶ pipeline-command
                   1. lock the stream (per aggregate ID)
                   2. Load: wait until the in-memory index has caught up to the
                      partition's end offset, then return the stream's events
                   3. replay events into the aggregate, let it decide
                   4. Append: version check, produce (acks=all, idempotent), keyed by ID
                 ──▶ Kafka topic applications.events
```

Why Kafka can serve as the event store, and where that stops working: [ADR 1](docs/adr/0001-kafka-as-event-store.md).

| Error                                          | gRPC status          |
|------------------------------------------------|----------------------|
| missing/invalid fields                         | `INVALID_ARGUMENT`   |
| unknown job or application                     | `NOT_FOUND`          |
| idempotency key reused for a different request | `ALREADY_EXISTS`     |
| stage move not allowed                         | `FAILED_PRECONDITION`|
| stream changed concurrently (safe to retry)    | `ABORTED`            |
| no response within the deadline                | `DEADLINE_EXCEEDED`  |

Every request gets a deadline of at most `REQUEST_TIMEOUT` (default 5s). A shorter
client deadline is kept.

## Read models and replay

The projector turns events into three tables: `jobs`, `applications_by_job` and
`stage_counts`. Its Kafka offsets are stored in PostgreSQL **in the same transaction**
as the changes they caused, and every statement is idempotent: each row remembers the
version of the last event applied to it. See [ADR 2](docs/adr/0002-projector-offsets-in-postgres.md).

To rebuild, drop the read models and replay the whole topic from offset 0:

```sh
make rebuild    # stops the projector, runs `pipeline-projector rebuild`, starts it again
```

A rebuild refuses to run while a projector is running (PostgreSQL advisory lock). The
integration test `TestRebuild_ReplayFromZeroProducesIdenticalReadModels` generates a random
workload with retried commands, snapshots every read-model row, rebuilds, and checks that
the result is identical row for row and matches the expected funnel.

**How long does a rebuild take?** For 100,110 events (34k applications), replay took a
median of 10.5 s (10.1–13.0 s over 6 runs, about 9,500 events/s) on a 2-vCPU Docker
Desktop VM. One PostgreSQL backend was at 100% CPU while the projector and Redpanda were
nearly idle. The [full write-up](docs/measurements/rebuild-100k.md) covers the method,
the discarded runs, what the number does *not* tell you, and untested ideas for speeding it up.

## Notifications

The notifier sends one email per event that concerns a candidate:
- the application was received
- the application moved to screening, interview, offer or hired
- the candidate was rejected (the recruiter's reason is never included)

It reads the same topic as the projector, but in its own consumer group, and keeps its
own state: contact details and job titles, both learned from the events themselves.

Kafka delivers at least once, so the notifier deduplicates by **event ID**. Event IDs
are deterministic, so a redelivered or duplicated record has the same ID. A
`notifier.deliveries` row is inserted, the email is sent, and the transaction commits.
If the send fails, everything rolls back and the retry sends. If two attempts race,
the second waits on the row lock and then skips.

One case can still produce a duplicate: the mail server accepts the email and the
process dies before the commit. Every email carries a stable `Message-ID` so a
downstream system can drop the repeat. [ADR 5](docs/adr/0005-notifier-delivery.md) covers
this and the other choices:
- Events older than `NOTIFY_MAX_AGE` (24h) are never emailed, so a new group doesn't
  email everyone about old history.
- Transient errors block the partition instead of skipping the record.
- A snappy codec is needed for the batches the Go producer writes.

```sh
cd services/notifier
npm ci
npm run lint               # ESLint (strict, type-aware) + tsc
npm test                   # Jest unit tests
npm run test:integration   # Testcontainers: Redpanda + PostgreSQL, records produced by rpk (franz-go)
```

## Run it

```sh
docker compose up -d --build   # Redpanda :19092, Postgres :5432, command :50051, query :50052, Mailpit :8025
make test                      # Go + TypeScript unit tests
make integration               # Go + TypeScript integration tests (needs Docker)
make lint                      # buf lint + format, golangci-lint, ESLint + tsc
make breaking                  # buf breaking against main (also runs in CI)
make generate                  # regenerate code after editing .proto files (needs buf)
make demo                      # scripted walkthrough (needs grpcurl)
make loadgen                   # write 100k events through the command API
```

The topic is created with infinite retention and `cleanup.policy=delete`. It is deliberately
**not** compacted, because compaction would keep only the latest event per application ID and
destroy the history.

## Try it

With the stack running and [grpcurl](https://github.com/fullstorydev/grpcurl) installed
(the server supports reflection, so no proto files are needed):

```sh
S=pipeline.v1.PipelineCommandService
grpcurl -plaintext -d '{"idempotency_key":"job-1","company_id":"acme","title":"Backend Engineer"}' \
  localhost:50051 $S/OpenJob
# -> {"jobId": "<JOB_ID>", "version": "1"}

grpcurl -plaintext -d '{"idempotency_key":"apply-1","job_id":"<JOB_ID>","candidate_name":"Ada Lovelace","candidate_email":"ada@example.com"}' \
  localhost:50051 $S/SubmitApplication
# -> {"applicationId": "<APP_ID>", "version": "1"}   (run it again: same answer, no new event)

grpcurl -plaintext -d '{"idempotency_key":"move-1","application_id":"<APP_ID>","to_stage":"STAGE_SCREENING"}' \
  localhost:50051 $S/MoveStage

docker compose exec redpanda rpk topic consume applications.events -f '%k p%p@%o\n'

# read side (eventually consistent, usually within milliseconds)
Q=pipeline.v1.PipelineQueryService
grpcurl -plaintext -d '{"job_id":"<JOB_ID>"}' localhost:50052 $Q/GetFunnel
grpcurl -plaintext -d '{"job_id":"<JOB_ID>","stage":"STAGE_SCREENING","page_size":20}' localhost:50052 $Q/ListApplications
```
