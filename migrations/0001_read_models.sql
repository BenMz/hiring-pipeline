-- Read models for the hiring pipeline. Everything here is derived from the
-- applications.events topic and can be dropped and rebuilt at any time.
--
-- There are no foreign keys from applications to jobs on purpose: a job and
-- its applications live on different partitions, so the projector may see
-- ApplicationSubmitted before the JobOpened it refers to.

CREATE TABLE IF NOT EXISTS jobs (
    job_id      text PRIMARY KEY,
    company_id  text        NOT NULL,
    title       text        NOT NULL,
    opened_at   timestamptz NOT NULL,
    version     bigint      NOT NULL
);

CREATE TABLE IF NOT EXISTS applications_by_job (
    application_id  text PRIMARY KEY,
    job_id          text        NOT NULL,
    candidate_name  text        NOT NULL,
    candidate_email text        NOT NULL,
    stage           text        NOT NULL CHECK (stage IN ('APPLIED', 'SCREENING', 'INTERVIEW', 'OFFER', 'HIRED', 'REJECTED')),
    -- Version of the last event applied to this row. Events at or below it
    -- are duplicates and are ignored, which makes every upsert idempotent.
    version         bigint      NOT NULL,
    submitted_at    timestamptz NOT NULL,
    updated_at      timestamptz NOT NULL
);

-- Keyset pagination for ListApplications, optionally filtered by stage.
CREATE INDEX IF NOT EXISTS applications_by_job_page ON applications_by_job (job_id, submitted_at, application_id);
CREATE INDEX IF NOT EXISTS applications_by_job_stage ON applications_by_job (job_id, stage, submitted_at, application_id);

CREATE TABLE IF NOT EXISTS stage_counts (
    job_id  text   NOT NULL,
    stage   text   NOT NULL,
    count   bigint NOT NULL CHECK (count >= 0),
    PRIMARY KEY (job_id, stage)
);

-- Kafka offsets of the projector, committed in the same transaction as the
-- read-model changes they produced. A crash can therefore never apply a batch
-- without recording it, or record it without applying it.
CREATE TABLE IF NOT EXISTS projector_offsets (
    topic       text   NOT NULL,
    partition   int    NOT NULL,
    next_offset bigint NOT NULL,
    PRIMARY KEY (topic, partition)
);
