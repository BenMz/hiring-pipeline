# hiring-pipeline

An event-sourced applicant pipeline for small businesses, in Go with gRPC and Kafka.

A company opens a job, candidates apply, and recruiters move each application through
`APPLIED → SCREENING → INTERVIEW → OFFER → HIRED` (or `REJECTED` from any open stage).
Every change is an event in the Kafka topic `applications.events`. Those events are the
source of truth. PostgreSQL holds read models only, and they can be rebuilt by replaying the topic.

> Status: **part 3 of 4**. Commands, event store, projector with replay, and the query API
> are done. The TypeScript notifier comes next.

```
                    ┌──────────────────┐   append (keyed by ID)  ┌──────────────────────┐
  gRPC commands ──▶ │ pipeline-command │ ──────────────────────▶ │ Kafka                │
                    └──────────────────┘ ◀── replay aggregates ─ │ applications.events  │
                                                                 └──────────┬───────────┘
                                                                            │ consume
                    ┌──────────────────┐   read                  ┌──────────▼───────────┐
  gRPC queries  ──▶ │ pipeline-query   │ ──────────────────────▶ │ pipeline-projector   │
                    └──────────────────┘   PostgreSQL read models └──────────────────────┘
```

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
services/notifier/src/gen/      generated TypeScript code for the notifier
docker-compose.yml              Redpanda (Kafka API) + PostgreSQL 16
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

## Run it

```sh
docker compose up -d --build   # Redpanda :19092, Postgres :5432, command :50051, query :50052
make test                      # unit tests
make integration               # integration tests (needs Docker)
make lint                      # buf lint + format check, golangci-lint
make breaking                  # buf breaking against main (also runs in CI)
make generate                  # regenerate code after editing .proto files (needs buf)
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
