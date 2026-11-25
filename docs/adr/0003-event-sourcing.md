# 3. Event sourcing for the hiring pipeline

Status: accepted (2026-11-25)

## Context

A small business needs to know where every candidate is, but also how they got
there: when they moved, who was rejected from which stage, how long the
interview stage takes. The questions change over time ("conversion from
screening to interview, per job, last quarter"), and the answers have to cover
data recorded before anyone thought to ask.

The usual design is a CRUD table, `applications(id, job_id, stage, ...)`,
updated in place, with an audit log on the side if someone remembers. An
update loses the previous stage. History then depends on a second write that
can drift from the first or be skipped.

## Decision

**The events are the source of truth.** Every change is an immutable event
(`JobOpened`, `ApplicationSubmitted`, `StageChanged`, `CandidateRejected`)
appended to `applications.events`. Current state is derived from them:

- The command side replays an aggregate's events to decide the next command
  (`services/command/internal/application`). The domain rules are pure functions
  of the history, with no I/O, and are tested without infrastructure.
- Read models (`jobs`, `applications_by_job`, `stage_counts`) are projections.
  They can be dropped and rebuilt from offset 0 (ADR 2).
- New consumers attach to the same log with their own offsets. The notifier
  was added in part 4 without touching the command service or the projector.
  It replays the history to learn every candidate's contact details.

## Consequences

Gains:

- **Complete history by construction.** The audit log can't drift, because
  nothing else exists.
- **New read models from old data.** A time-in-stage projection could be added
  today and backfilled from the first event ever written.
- **Retries are safe.** Idempotency keys live in the events, so a retried
  command is recognised even after a restart (ADR 1).
- **Testable domain.** `Load(history) -> Decide(command) -> events` needs no
  database.

Costs, and they are real:

- **Eventual consistency on the read side.** A client that writes and then
  queries may not see its write yet, usually for a few milliseconds. Callers
  that need read-your-writes must use the command response, which carries the
  new version.
- **Schema evolution is permanent.** Events written today must be readable
  forever. Protobuf with `buf breaking` in CI catches incompatible changes, but
  changing an event's *meaning* still needs a new event type or an upcaster.
- **Rebuilds cost time.** Replaying 100k events takes about 10–13 s on a laptop
  ([measurement](../measurements/rebuild-100k.md)). That is fine at this scale.
  At much larger scale it needs snapshots or parallel projection.
- **Deleting personal data is hard.** Candidate names and emails are inside
  immutable events with infinite retention. GDPR erasure would need
  crypto-shredding (encrypt PII per candidate, delete the key) or storing PII
  outside the log and referencing it. This is **not implemented**. It is the
  largest gap before real candidate data could go in.
- **More moving parts** than one table: a log, a projector, offsets, rebuild
  tooling. For an app whose only question is "what stage is this candidate
  in?", CRUD would be the better choice.
