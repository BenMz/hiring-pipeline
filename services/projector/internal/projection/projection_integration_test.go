//go:build integration

package projection_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/timestamppb"

	pipelinev1 "github.com/BenMz/hiring-pipeline/gen/go/pipeline/v1"
	"github.com/BenMz/hiring-pipeline/internal/testpg"
	"github.com/BenMz/hiring-pipeline/services/projector/internal/projection"
)

var adminURL string

func TestMain(m *testing.M) {
	u, stop, err := testpg.Start(context.Background())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	adminURL = u
	code := m.Run()
	stop()
	os.Exit(code)
}

var t0 = time.Date(2026, 11, 24, 9, 0, 0, 0, time.UTC)

func meta(id string, v int64) *pipelinev1.EventMetadata {
	return &pipelinev1.EventMetadata{AggregateId: id, Version: v, OccurredAt: timestamppb.New(t0.Add(time.Duration(v) * time.Minute))}
}

func jobOpened() *pipelinev1.Event {
	const job = "j1"
	return &pipelinev1.Event{Metadata: meta(job, 1), Payload: &pipelinev1.Event_JobOpened{
		JobOpened: &pipelinev1.JobOpened{JobId: job, CompanyId: "acme", Title: "Backend Engineer"}}}
}

func submitted(app string) *pipelinev1.Event {
	const job = "j1"
	return &pipelinev1.Event{Metadata: meta(app, 1), Payload: &pipelinev1.Event_ApplicationSubmitted{
		ApplicationSubmitted: &pipelinev1.ApplicationSubmitted{ApplicationId: app, JobId: job, CandidateName: "Ada", CandidateEmail: "ada@example.com"}}}
}

func moved(app string, v int64, from, to pipelinev1.Stage) *pipelinev1.Event {
	return &pipelinev1.Event{Metadata: meta(app, v), Payload: &pipelinev1.Event_StageChanged{
		StageChanged: &pipelinev1.StageChanged{ApplicationId: app, FromStage: from, ToStage: to}}}
}

func rejected(app string, v int64, from pipelinev1.Stage) *pipelinev1.Event {
	return &pipelinev1.Event{Metadata: meta(app, v), Payload: &pipelinev1.Event_CandidateRejected{
		CandidateRejected: &pipelinev1.CandidateRejected{ApplicationId: app, FromStage: from, Reason: "filled"}}}
}

func setup(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool := testpg.Pool(t, testpg.FreshDB(t, adminURL))
	if err := projection.Migrate(t.Context(), pool); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return pool
}

func apply(t *testing.T, pool *pgxpool.Pool, events ...*pipelinev1.Event) {
	t.Helper()
	err := pgx.BeginFunc(t.Context(), pool, func(tx pgx.Tx) error {
		return projection.Apply(t.Context(), tx, events)
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
}

func counts(t *testing.T, pool *pgxpool.Pool, job string) map[string]int64 {
	t.Helper()
	rows, err := pool.Query(t.Context(), "SELECT stage, count FROM stage_counts WHERE job_id = $1 AND count > 0", job)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]int64{}
	for rows.Next() {
		var s string
		var n int64
		if err := rows.Scan(&s, &n); err != nil {
			t.Fatal(err)
		}
		out[s] = n
	}
	return out
}

func stageOf(t *testing.T, pool *pgxpool.Pool, app string) (stage string, version int64) {
	t.Helper()
	if err := pool.QueryRow(t.Context(), "SELECT stage, version FROM applications_by_job WHERE application_id = $1", app).Scan(&stage, &version); err != nil {
		t.Fatalf("read %s: %v", app, err)
	}
	return stage, version
}

const (
	applied   = pipelinev1.Stage_STAGE_APPLIED
	screening = pipelinev1.Stage_STAGE_SCREENING
	interview = pipelinev1.Stage_STAGE_INTERVIEW
)

func TestApply_BuildsApplicationsAndFunnel(t *testing.T) {
	pool := setup(t)

	apply(t, pool,
		jobOpened(),
		submitted("a1"), submitted("a2"), submitted("a3"),
		moved("a1", 2, applied, screening), moved("a1", 3, screening, interview),
		moved("a2", 2, applied, screening),
		rejected("a3", 2, applied),
	)

	if s, v := stageOf(t, pool, "a1"); s != "INTERVIEW" || v != 3 {
		t.Fatalf("a1 = (%s, v%d), want (INTERVIEW, v3)", s, v)
	}
	if got := fmt.Sprint(counts(t, pool, "j1")); got != "map[INTERVIEW:1 REJECTED:1 SCREENING:1]" {
		t.Fatalf("funnel = %s", got)
	}
	var updated time.Time
	if err := pool.QueryRow(t.Context(), "SELECT updated_at FROM applications_by_job WHERE application_id = 'a1'").Scan(&updated); err != nil || !updated.Equal(t0.Add(3*time.Minute)) {
		t.Fatalf("a1.updated_at = %v (%v), want time of the last event", updated, err)
	}
}

func TestApply_DuplicateEventsAreIgnored(t *testing.T) {
	pool := setup(t)
	batch := []*pipelinev1.Event{jobOpened(), submitted("a1"), moved("a1", 2, applied, screening)}

	apply(t, pool, batch...)
	apply(t, pool, batch...)        // whole batch redelivered
	apply(t, pool, submitted("a1")) // a stale event after a newer one

	if s, v := stageOf(t, pool, "a1"); s != "SCREENING" || v != 2 {
		t.Fatalf("a1 = (%s, v%d), want (SCREENING, v2)", s, v)
	}
	if got := fmt.Sprint(counts(t, pool, "j1")); got != "map[SCREENING:1]" {
		t.Fatalf("funnel = %s, want map[SCREENING:1]", got)
	}
	var jobs int
	if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM jobs").Scan(&jobs); err != nil || jobs != 1 {
		t.Fatalf("jobs = %d (%v), want 1", jobs, err)
	}
}

func TestApply_ApplicationBeforeItsJob(t *testing.T) {
	// Jobs and applications are on different partitions, so this order happens.
	pool := setup(t)

	apply(t, pool, submitted("a1"))
	apply(t, pool, jobOpened())

	if got := fmt.Sprint(counts(t, pool, "j1")); got != "map[APPLIED:1]" {
		t.Fatalf("funnel = %s, want map[APPLIED:1]", got)
	}
}

func TestApply_UnknownPayloadIsSkipped(t *testing.T) {
	// An event type added later must not stop an older projector.
	pool := setup(t)

	apply(t, pool, &pipelinev1.Event{Metadata: meta("x", 1)}, submitted("a1"))

	if s, _ := stageOf(t, pool, "a1"); s != "APPLIED" {
		t.Fatalf("a1 stage = %s, want APPLIED", s)
	}
}

func TestOffsets_RoundTrip(t *testing.T) {
	pool := setup(t)

	err := pgx.BeginFunc(t.Context(), pool, func(tx pgx.Tx) error {
		if err := projection.SaveOffsets(t.Context(), tx, "events", map[int32]int64{0: 5, 3: 9}); err != nil {
			return err
		}
		return projection.SaveOffsets(t.Context(), tx, "events", map[int32]int64{3: 12})
	})
	if err != nil {
		t.Fatal(err)
	}

	got, err := projection.LoadOffsets(t.Context(), pool, "events")
	if err != nil || fmt.Sprint(got) != "map[0:5 3:12]" {
		t.Fatalf("LoadOffsets = %v (%v), want map[0:5 3:12]", got, err)
	}
}

func TestReset_DropsAllReadModelsAndOffsets(t *testing.T) {
	pool := setup(t)
	apply(t, pool, jobOpened(), submitted("a1"))
	if err := pgx.BeginFunc(t.Context(), pool, func(tx pgx.Tx) error {
		return projection.SaveOffsets(t.Context(), tx, "events", map[int32]int64{0: 2})
	}); err != nil {
		t.Fatal(err)
	}

	if err := projection.Reset(t.Context(), pool); err != nil {
		t.Fatalf("Reset: %v", err)
	}

	for _, table := range []string{"jobs", "applications_by_job", "stage_counts", "projector_offsets"} {
		var n int
		if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM "+table).Scan(&n); err != nil || n != 0 {
			t.Fatalf("%s has %d rows (%v) after Reset, want 0", table, n, err)
		}
	}
}
