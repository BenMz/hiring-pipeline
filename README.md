# hiring-pipeline

An event-sourced applicant pipeline for small businesses, in Go with gRPC and Kafka.

A company opens a job, candidates apply, and recruiters move each application through
`APPLIED → SCREENING → INTERVIEW → OFFER → HIRED` (or `REJECTED` from any open stage).
Every change is an event in the Kafka topic `applications.events`. Those events are the
source of truth. PostgreSQL holds read models only, and they can be rebuilt by replaying the topic.

> Status: **part 1 of 4**. Contracts, aggregate, local infrastructure and CI are done.
> The command, projector, query and notifier services come next.

## Layout

```
proto/pipeline/v1/              events.proto, command.proto, query.proto
gen/go/                         generated Go code (committed; CI checks it is current)
services/command/internal/application/   the application aggregate (pure domain logic)
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

## Run it

```sh
docker compose up -d     # Redpanda on localhost:19092, Postgres on localhost:5432
make test                # unit tests
make lint                # buf lint + format check, golangci-lint
make generate            # regenerate code after editing .proto files (needs buf)
```

The topic is created with infinite retention and `cleanup.policy=delete`. It is deliberately
**not** compacted, because compaction would keep only the latest event per application ID and
destroy the history.
