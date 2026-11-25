# Measurement: rebuilding the read models from 100k events

**Result:** replaying 100,110 events into empty read models took a **median of
10.5 s** (range 10.1–13.0 s over 6 runs), about **9,500 events/s**. The whole
`rebuild` command, including container start and dropping and recreating the
tables, took 12.3–16.4 s. **PostgreSQL is the bottleneck**: one backend sits at
100% of a core while the projector and Redpanda are nearly idle.

Measured 2026-11-25 at commit `b6a234f` plus the part-4 working tree. The
projector code was unchanged from part 3.

## What was measured

`pipeline-projector rebuild` does the following:

1. takes the advisory lock
2. drops `jobs`, `applications_by_job`, `stage_counts` and `projector_offsets`, and recreates them
3. replays `applications.events` from offset 0 until every partition reaches its end offset

The **replay time** is the `duration` the command logs. It runs from the start
of step 3 until the projector has caught up and its last transaction has
committed. The **wall time** is the whole `docker compose run --rm
pipeline-projector rebuild` invocation, measured from the host.

## Setup

| | |
|---|---|
| Machine | Apple M1 Pro, 16 GB, macOS 14 |
| Docker | Docker Desktop, VM limited to **2 vCPUs, 3.8 GiB** |
| Kafka | Redpanda v24.2.7, `--mode=dev-container --smp=1`, 1 broker, 6 partitions |
| PostgreSQL | 16.16 (Alpine), **default config** (`shared_buffers=128MB`, `synchronous_commit=on`) |
| Projector | the compose image (Go 1.26, franz-go, pgx v5), one instance |

**Data.** The events were written through the real command API by
`tools/loadgen`, with 32 concurrent clients. It took 58 s, about 1,700 events/s.
That was not tuned; the command path waits for read-your-writes on every
command. The mix:

| Event | Count |
|---|---:|
| `JobOpened` | 53 |
| `ApplicationSubmitted` | 34,133 |
| `StageChanged` | 51,888 |
| `CandidateRejected` | 14,036 |
| **Total** | **100,110** (~16.7k per partition) |

That is 34,133 applications across 53 jobs. About 18% reach `HIRED`, which is
more than a real funnel. It matters because every stage move is a
read-modify-write, while a submit is a plain insert.

## Results

With only Redpanda, PostgreSQL, the idle command and query services and
Mailpit running:

| Run | Replay | Wall | Transactions |
|---:|---:|---:|---:|
| 1 | 10.52 s | 13.22 s | 35 |
| 2 | 12.98 s | 16.43 s | 19 |
| 3 | 12.32 s | 15.79 s | 19 |
| 4 | 10.50 s | 13.08 s | 19 |
| 5 | 10.06 s | 13.26 s | 41 |
| 6 | 10.10 s | 12.33 s | 18 |

- **Median** 10.51 s replay, about 9,500 events/s, about 105 µs per event.
- **Worst** 12.98 s, about 7,700 events/s.

"Transactions" is the change in `pg_stat_database.xact_commit` over the run,
minus the measuring query itself. With about 19 commits for 100k events, each
transaction carried about 5,000 events. Per-commit overhead (WAL flush,
offsets write) is therefore negligible, and the cost is in executing the
statements.

Afterwards the read models were consistent:

- 34,133 rows in `applications_by_job`
- `stage_counts` summing to 34,133
- `projector_offsets` summing to 100,110, the topic's total record count

The rebuild-equivalence integration test checks the stronger property: rows
are identical before and after a rebuild.

## Where the time goes

`docker stats` sampled during a clean replay:

| Container | CPU |
|---|---:|
| postgres | 97–108% |
| redpanda | 1–10% |
| projector | 0.5–1.7% |

The projector applies each batch on **one connection**. A pipelined `pgx.Batch`
holds one idempotent statement per event, and stage moves are data-modifying
CTEs that update `applications_by_job` and two `stage_counts` rows. A single
Postgres backend executes them serially, so one core does all the work, and
the other vCPU is idle.

Hypotheses for the roughly 105 µs/event, **none of them tested yet**:

- maintaining two secondary indexes on `applications_by_job` for every insert
  and update
- repeated updates to the same few hundred `stage_counts` rows (53 jobs × 6
  stages) within one transaction, each creating a new row version
- CTE planning per statement (they are not prepared server-side across batches)

Likely improvements, in order of expected payoff:

1. **Parallelism across partitions.** Partitions are independent, so six
   connections each applying their own partitions would use the idle CPU. It is
   bounded here by the 2-vCPU VM. It needs per-partition offsets, which already
   exist, and care with `stage_counts`: two applications for the same job can
   be on different partitions, so either keep the funnel counts commutative
   (they are increments) or compute them after the replay.
2. **Rebuild-specific bulk path.** Build `applications_by_job` with `COPY` into
   an unindexed table, derive `stage_counts` with one `GROUP BY`, then create
   indexes. This gives up the "same code path as live projection" property that
   the equivalence test relies on.

## What this does not tell you

- **Not live projection latency.** The live projector applies small batches as
  events arrive, so its cost is round trips, not throughput.
- **Not production hardware.** Docker Desktop on a laptop, with a 2-vCPU VM, a
  single Redpanda broker in developer mode, and untuned Postgres. A real
  database server will be faster per event and has more cores to use only if
  item 1 above is done.
- **Not a cold cache.** The topic and the tables fit in the VM's page cache, and
  run 1 was not after a reboot. Runs 2, 3 and 5 being slower than 1 suggests
  noise (VM scheduling, autovacuum on the just-dropped tables) dominates any
  warm-up effect.
- **Six runs is a small sample.** Expect ±15% between runs, as the 10.1–13.0 s
  range shows.

## Discarded runs

The first five runs measured 13.7–15.1 s. They are not counted, because the
notifier was running and had a backlog of the 100k load-test events. Its
deliveries wrote to the same PostgreSQL instance, and `xact_commit` rose by
about 14,000 during one rebuild, mostly from it. Stopping the notifier gave
the numbers above. Lesson: the notifier should not share a database with the
read models under load (ADR 5).

## Reproduce

```sh
make up
go run ./tools/loadgen -events 100000     # about 1 minute
docker compose stop pipeline-notifier     # or it competes for Postgres
make rebuild                              # logs: "rebuild complete" events=... duration=...
```
