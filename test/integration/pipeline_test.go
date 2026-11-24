//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	pipelinev1 "github.com/BenMz/hiring-pipeline/gen/go/pipeline/v1"
	"github.com/BenMz/hiring-pipeline/internal/testkafka"
	"github.com/BenMz/hiring-pipeline/internal/testpg"
	projector "github.com/BenMz/hiring-pipeline/services/projector/app"
	query "github.com/BenMz/hiring-pipeline/services/query/app"
)

func projectorConfig(topic, dbURL string) projector.Config {
	return projector.Config{Brokers: []string{broker}, Topic: topic, DatabaseURL: dbURL}
}

func catchUp(t *testing.T, cfg projector.Config) projector.Stats {
	t.Helper()
	stats, err := projector.CatchUp(t.Context(), cfg, slog.New(slog.NewTextHandler(t.Output(), nil)))
	if err != nil {
		t.Fatalf("projector catch-up: %v", err)
	}
	return stats
}

func startQuery(t *testing.T, dbURL string) pipelinev1.PipelineQueryServiceClient {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- query.Run(ctx, query.Config{DatabaseURL: dbURL, RequestTimeout: 5 * time.Second}, lis, slog.New(slog.NewTextHandler(t.Output(), nil)))
	}()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("query service exited with %v", err)
		}
	})
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	readyCtx, readyCancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer readyCancel()
	if _, err := healthpb.NewHealthClient(conn).Check(readyCtx, &healthpb.HealthCheckRequest{}, grpc.WaitForReady(true)); err != nil {
		t.Fatalf("query service not ready: %v", err)
	}
	return pipelinev1.NewPipelineQueryServiceClient(conn)
}

func funnel(t *testing.T, client pipelinev1.PipelineQueryServiceClient, jobID string) map[pipelinev1.Stage]int64 {
	t.Helper()
	res, err := client.GetFunnel(t.Context(), &pipelinev1.GetFunnelRequest{JobId: jobID})
	if err != nil {
		t.Fatalf("GetFunnel(%s): %v", jobID, err)
	}
	out := map[pipelinev1.Stage]int64{}
	for _, s := range res.GetStages() {
		if s.GetCount() > 0 {
			out[s.GetStage()] = s.GetCount()
		}
	}
	return out
}

// snapshot dumps every read-model row as sorted text, so two databases can be
// compared exactly.
func snapshot(t *testing.T, dbURL string) string {
	t.Helper()
	pool := testpg.Pool(t, dbURL)
	var out []string
	for _, q := range []string{
		"SELECT 'job', job_id, company_id, title, opened_at::text, version::text FROM jobs",
		"SELECT 'app', application_id, job_id, candidate_name, candidate_email, stage, version::text, submitted_at::text, updated_at::text FROM applications_by_job",
		"SELECT 'count', job_id, stage, count::text FROM stage_counts WHERE count > 0",
	} {
		out = append(out, rowsAsText(t, pool, q)...)
	}
	slices.Sort(out)
	return strings.Join(out, "\n")
}

func rowsAsText(t *testing.T, pool *pgxpool.Pool, q string) []string {
	t.Helper()
	rows, err := pool.Query(t.Context(), q)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, fmt.Sprint(vals...))
	}
	return out
}

func TestPipeline_CommandsShowUpInQueries(t *testing.T) {
	topic, dbURL := testkafka.CreateTopic(t, broker), testpg.FreshDB(t, adminDB)
	commands, _ := startService(t, topic)
	queries := startQuery(t, dbURL)
	ctx := t.Context()

	job, err := commands.OpenJob(ctx, &pipelinev1.OpenJobRequest{IdempotencyKey: "job-1", CompanyId: "acme", Title: "Backend Engineer"})
	if err != nil {
		t.Fatal(err)
	}
	var appIDs []string
	for i := range 3 {
		res, err := commands.SubmitApplication(ctx, &pipelinev1.SubmitApplicationRequest{
			IdempotencyKey: fmt.Sprintf("apply-%d", i), JobId: job.GetJobId(),
			CandidateName: fmt.Sprintf("Candidate %d", i), CandidateEmail: fmt.Sprintf("c%d@example.com", i),
		})
		if err != nil {
			t.Fatal(err)
		}
		appIDs = append(appIDs, res.GetApplicationId())
	}
	if _, err := commands.MoveStage(ctx, &pipelinev1.MoveStageRequest{IdempotencyKey: "m", ApplicationId: appIDs[0], ToStage: pipelinev1.Stage_STAGE_SCREENING}); err != nil {
		t.Fatal(err)
	}
	if _, err := commands.RejectCandidate(ctx, &pipelinev1.RejectCandidateRequest{IdempotencyKey: "r", ApplicationId: appIDs[1]}); err != nil {
		t.Fatal(err)
	}

	cfg := projectorConfig(topic, dbURL)
	if stats := catchUp(t, cfg); stats.Events != 6 {
		t.Fatalf("projected %d events, want 6", stats.Events)
	}

	got := fmt.Sprint(funnel(t, queries, job.GetJobId()))
	if want := "map[STAGE_APPLIED:1 STAGE_SCREENING:1 STAGE_REJECTED:1]"; got != want {
		t.Fatalf("funnel = %s, want %s", got, want)
	}
	list, err := queries.ListApplications(ctx, &pipelinev1.ListApplicationsRequest{JobId: job.GetJobId(), Stage: pipelinev1.Stage_STAGE_SCREENING})
	if err != nil || len(list.GetApplications()) != 1 || list.GetApplications()[0].GetApplicationId() != appIDs[0] {
		t.Fatalf("screening list = %v (%v), want only %s", list.GetApplications(), err, appIDs[0])
	}

	// Offsets live in PostgreSQL: a second run resumes instead of reapplying.
	if stats := catchUp(t, cfg); stats.Events != 0 {
		t.Fatalf("second catch-up applied %d events, want 0", stats.Events)
	}
}

func TestRebuild_ReplayFromZeroProducesIdenticalReadModels(t *testing.T) {
	topic, dbURL := testkafka.CreateTopic(t, broker), testpg.FreshDB(t, adminDB)
	commands, _ := startService(t, topic)
	queries := startQuery(t, dbURL)
	ctx := t.Context()
	rng := rand.New(rand.NewPCG(1, 2))

	// Random workload: 3 jobs, 60 applications, random walks through the
	// funnel, and some retried commands. The test tracks the expected funnel.
	expected := map[string]map[pipelinev1.Stage]int64{}
	var jobIDs []string
	for j := range 3 {
		res, err := commands.OpenJob(ctx, &pipelinev1.OpenJobRequest{IdempotencyKey: fmt.Sprintf("job-%d", j), CompanyId: "acme", Title: fmt.Sprintf("Role %d", j)})
		if err != nil {
			t.Fatal(err)
		}
		jobIDs = append(jobIDs, res.GetJobId())
		expected[res.GetJobId()] = map[pipelinev1.Stage]int64{}
	}
	written := int64(3)
	for i := range 60 {
		jobID := jobIDs[i%3]
		submit := &pipelinev1.SubmitApplicationRequest{
			IdempotencyKey: fmt.Sprintf("apply-%d", i), JobId: jobID,
			CandidateName: fmt.Sprintf("Candidate %d", i), CandidateEmail: fmt.Sprintf("c%d@example.com", i),
		}
		app, err := commands.SubmitApplication(ctx, submit)
		if err != nil {
			t.Fatal(err)
		}
		written++
		stage := pipelinev1.Stage_STAGE_APPLIED
		for step := 0; stage != pipelinev1.Stage_STAGE_HIRED && rng.IntN(3) > 0; step++ {
			key := fmt.Sprintf("app-%d-step-%d", i, step)
			if rng.IntN(5) == 0 {
				_, err = commands.RejectCandidate(ctx, &pipelinev1.RejectCandidateRequest{IdempotencyKey: key, ApplicationId: app.GetApplicationId()})
				stage = pipelinev1.Stage_STAGE_REJECTED
			} else {
				stage++
				move := &pipelinev1.MoveStageRequest{IdempotencyKey: key, ApplicationId: app.GetApplicationId(), ToStage: stage}
				_, err = commands.MoveStage(ctx, move)
				if err == nil && rng.IntN(4) == 0 {
					_, err = commands.MoveStage(ctx, move) // a retry: must not count twice
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			written++
			if stage == pipelinev1.Stage_STAGE_REJECTED {
				break
			}
		}
		expected[jobID][stage]++
	}

	cfg := projectorConfig(topic, dbURL)
	catchUp(t, cfg)
	for _, jobID := range jobIDs {
		if got, want := fmt.Sprint(funnel(t, queries, jobID)), fmt.Sprint(expected[jobID]); got != want {
			t.Fatalf("funnel of %s = %s, want %s", jobID, got, want)
		}
	}
	before := snapshot(t, dbURL)

	stats, err := projector.Rebuild(ctx, cfg, slog.New(slog.NewTextHandler(t.Output(), nil)))
	if err != nil {
		t.Fatalf("Rebuild: %v", err)
	}

	if stats.Events != written {
		t.Fatalf("rebuild replayed %d events, want all %d", stats.Events, written)
	}
	if after := snapshot(t, dbURL); after != before {
		t.Fatalf("read models differ after rebuild\nbefore:\n%s\n\nafter:\n%s", before, after)
	}
	t.Logf("rebuilt %d events in %v", stats.Events, stats.Duration)
}

func TestRebuild_RefusesWhileProjectorIsRunning(t *testing.T) {
	topic, dbURL := testkafka.CreateTopic(t, broker), testpg.FreshDB(t, adminDB)
	cfg := projectorConfig(topic, dbURL)
	log := slog.New(slog.NewTextHandler(t.Output(), nil))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- projector.Run(ctx, cfg, log) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("projector exited with %v", err)
		}
	}()

	// Wait until the running projector holds its advisory lock.
	pool := testpg.Pool(t, dbURL)
	deadline := time.Now().Add(20 * time.Second)
	for locked := false; !locked; {
		if err := pool.QueryRow(t.Context(), `SELECT EXISTS (SELECT 1 FROM pg_locks l JOIN pg_database d ON d.oid = l.database
		                                       WHERE l.locktype = 'advisory' AND d.datname = current_database())`).Scan(&locked); err != nil {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("projector never took its lock")
		}
		time.Sleep(50 * time.Millisecond)
	}

	_, err := projector.Rebuild(t.Context(), cfg, log)

	if !errors.Is(err, projector.ErrAlreadyRunning) {
		t.Fatalf("Rebuild = %v, want ErrAlreadyRunning while a projector runs", err)
	}
}
