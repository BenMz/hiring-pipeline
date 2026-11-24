// Package projection turns events into the PostgreSQL read models.
//
// Every statement is idempotent: an event whose version is not newer than the
// row it touches changes nothing, so a redelivered batch is harmless. Apply
// sends one statement per event, pipelined in a single round trip, and is
// meant to run in the same transaction that saves the Kafka offsets.
package projection

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	pipelinev1 "github.com/BenMz/hiring-pipeline/gen/go/pipeline/v1"
	"github.com/BenMz/hiring-pipeline/migrations"
)

// DB is the subset of pgx used here; *pgxpool.Pool, *pgx.Conn and pgx.Tx satisfy it.
type DB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

// Migrate creates the read-model tables if they don't exist.
func Migrate(ctx context.Context, db DB) error {
	scripts, err := migrations.SQL()
	if err != nil {
		return err
	}
	for _, s := range scripts {
		if _, err := db.Exec(ctx, s); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}
	return nil
}

// Reset drops every read-model table and the stored offsets, then recreates
// the schema. Afterwards the projector replays the topic from offset 0.
func Reset(ctx context.Context, db DB) error {
	if _, err := db.Exec(ctx, "DROP TABLE IF EXISTS "+strings.Join(migrations.ReadModelTables, ", ")); err != nil {
		return fmt.Errorf("drop read models: %w", err)
	}
	return Migrate(ctx, db)
}

// StageName is how a stage is stored: "SCREENING" for STAGE_SCREENING.
func StageName(s pipelinev1.Stage) string {
	return strings.TrimPrefix(s.String(), "STAGE_")
}

const (
	insertJob = `
INSERT INTO jobs (job_id, company_id, title, opened_at, version)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (job_id) DO NOTHING`

	// The CTE inserts the application only if it is new; the funnel count is
	// incremented only for rows the CTE actually inserted.
	insertApplication = `
WITH inserted AS (
    INSERT INTO applications_by_job
        (application_id, job_id, candidate_name, candidate_email, stage, version, submitted_at, updated_at)
    VALUES ($1, $2, $3, $4, 'APPLIED', $5, $6, $6)
    ON CONFLICT (application_id) DO NOTHING
    RETURNING job_id
)
INSERT INTO stage_counts (job_id, stage, count)
SELECT job_id, 'APPLIED', 1 FROM inserted
ON CONFLICT (job_id, stage) DO UPDATE SET count = stage_counts.count + 1`

	// Moves the row only if the event is newer and starts from the row's
	// current stage, then moves one unit of the funnel from $5 to $2.
	moveApplication = `
WITH moved AS (
    UPDATE applications_by_job
       SET stage = $2, version = $3, updated_at = $4
     WHERE application_id = $1 AND version < $3 AND stage = $5
 RETURNING job_id
), left_stage AS (
    UPDATE stage_counts c
       SET count = c.count - 1
      FROM moved
     WHERE c.job_id = moved.job_id AND c.stage = $5
)
INSERT INTO stage_counts (job_id, stage, count)
SELECT job_id, $2, 1 FROM moved
ON CONFLICT (job_id, stage) DO UPDATE SET count = stage_counts.count + 1`

	saveOffset = `
INSERT INTO projector_offsets (topic, partition, next_offset)
VALUES ($1, $2, $3)
ON CONFLICT (topic, partition) DO UPDATE SET next_offset = EXCLUDED.next_offset`
)

// Apply projects events in order. Events with a payload this version does not
// know are skipped, so adding an event type never breaks an older projector.
func Apply(ctx context.Context, db DB, events []*pipelinev1.Event) error {
	b := &pgx.Batch{}
	for _, e := range events {
		m := e.GetMetadata()
		at := m.GetOccurredAt().AsTime()
		switch p := e.GetPayload().(type) {
		case *pipelinev1.Event_JobOpened:
			j := p.JobOpened
			b.Queue(insertJob, j.GetJobId(), j.GetCompanyId(), j.GetTitle(), at, m.GetVersion())
		case *pipelinev1.Event_ApplicationSubmitted:
			a := p.ApplicationSubmitted
			b.Queue(insertApplication, a.GetApplicationId(), a.GetJobId(), a.GetCandidateName(), a.GetCandidateEmail(), m.GetVersion(), at)
		case *pipelinev1.Event_StageChanged:
			s := p.StageChanged
			b.Queue(moveApplication, s.GetApplicationId(), StageName(s.GetToStage()), m.GetVersion(), at, StageName(s.GetFromStage()))
		case *pipelinev1.Event_CandidateRejected:
			r := p.CandidateRejected
			b.Queue(moveApplication, r.GetApplicationId(), StageName(pipelinev1.Stage_STAGE_REJECTED), m.GetVersion(), at, StageName(r.GetFromStage()))
		}
	}
	if b.Len() == 0 {
		return nil
	}
	if err := db.SendBatch(ctx, b).Close(); err != nil {
		return fmt.Errorf("apply %d events: %w", b.Len(), err)
	}
	return nil
}

// SaveOffsets records the next offset to read per partition.
func SaveOffsets(ctx context.Context, db DB, topic string, next map[int32]int64) error {
	if len(next) == 0 {
		return nil
	}
	b := &pgx.Batch{}
	for p, o := range next {
		b.Queue(saveOffset, topic, p, o)
	}
	if err := db.SendBatch(ctx, b).Close(); err != nil {
		return fmt.Errorf("save offsets: %w", err)
	}
	return nil
}

// LoadOffsets returns the next offset to read per partition. Partitions that
// were never projected are absent and start from the beginning.
func LoadOffsets(ctx context.Context, db DB, topic string) (map[int32]int64, error) {
	rows, err := db.Query(ctx, "SELECT partition, next_offset FROM projector_offsets WHERE topic = $1", topic)
	if err != nil {
		return nil, fmt.Errorf("load offsets: %w", err)
	}
	next := map[int32]int64{}
	var p int32
	var o int64
	_, err = pgx.ForEachRow(rows, []any{&p, &o}, func() error {
		next[p] = o
		return nil
	})
	return next, err
}
