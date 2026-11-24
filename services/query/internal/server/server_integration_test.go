//go:build integration

package server_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	pipelinev1 "github.com/BenMz/hiring-pipeline/gen/go/pipeline/v1"
	"github.com/BenMz/hiring-pipeline/internal/testpg"
	"github.com/BenMz/hiring-pipeline/migrations"
	"github.com/BenMz/hiring-pipeline/services/query/internal/server"
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

// setup creates the read-model schema with seed data and serves the query
// API over an in-memory connection.
func setup(t *testing.T) pipelinev1.PipelineQueryServiceClient {
	t.Helper()
	pool := testpg.Pool(t, testpg.FreshDB(t, adminURL))
	scripts, err := migrations.SQL()
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range scripts {
		exec(t, pool, s)
	}
	exec(t, pool, `INSERT INTO jobs VALUES ('j1', 'acme', 'Backend Engineer', $1, 1), ('j-empty', 'acme', 'Designer', $1, 1)`, t0)
	stages := []string{"APPLIED", "SCREENING", "SCREENING", "INTERVIEW", "REJECTED"}
	for i, s := range stages {
		exec(t, pool, `INSERT INTO applications_by_job VALUES ($1, 'j1', $2, $3, $4, 1, $5, $6)`,
			fmt.Sprintf("a%d", i+1), fmt.Sprintf("Candidate %d", i+1), fmt.Sprintf("c%d@example.com", i+1), s,
			t0.Add(time.Duration(i)*time.Hour), t0.Add(48*time.Hour))
	}
	exec(t, pool, `INSERT INTO stage_counts VALUES ('j1', 'APPLIED', 1), ('j1', 'SCREENING', 2), ('j1', 'INTERVIEW', 1), ('j1', 'OFFER', 0), ('j1', 'REJECTED', 1)`)

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	pipelinev1.RegisterPipelineQueryServiceServer(srv, server.New(pool, slog.New(slog.NewTextHandler(io.Discard, nil))))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return pipelinev1.NewPipelineQueryServiceClient(conn)
}

func exec(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(t.Context(), sql, args...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

func ids(apps []*pipelinev1.ApplicationSummary) string {
	out := ""
	for _, a := range apps {
		out += a.GetApplicationId() + " "
	}
	return out
}

func TestListApplications_ReturnsJobApplicationsOldestFirst(t *testing.T) {
	client := setup(t)

	res, err := client.ListApplications(t.Context(), &pipelinev1.ListApplicationsRequest{JobId: "j1"})
	if err != nil {
		t.Fatal(err)
	}

	if got := ids(res.GetApplications()); got != "a1 a2 a3 a4 a5 " || res.GetNextPageToken() != "" {
		t.Fatalf("ids = %q, next = %q; want all five and no next page", got, res.GetNextPageToken())
	}
	a := res.GetApplications()[3]
	if a.GetStage() != pipelinev1.Stage_STAGE_INTERVIEW || a.GetCandidateEmail() != "c4@example.com" ||
		!a.GetSubmittedAt().AsTime().Equal(t0.Add(3*time.Hour)) || !a.GetUpdatedAt().AsTime().Equal(t0.Add(48*time.Hour)) {
		t.Fatalf("a4 = %v", a)
	}
}

func TestListApplications_FiltersByStage(t *testing.T) {
	client := setup(t)

	res, err := client.ListApplications(t.Context(), &pipelinev1.ListApplicationsRequest{JobId: "j1", Stage: pipelinev1.Stage_STAGE_SCREENING})
	if err != nil {
		t.Fatal(err)
	}

	if got := ids(res.GetApplications()); got != "a2 a3 " {
		t.Fatalf("ids = %q, want a2 a3", got)
	}
}

func TestListApplications_PagesThroughEverything(t *testing.T) {
	client := setup(t)
	var all, token string
	pages := 0

	for {
		res, err := client.ListApplications(t.Context(), &pipelinev1.ListApplicationsRequest{JobId: "j1", PageSize: 2, PageToken: token})
		if err != nil {
			t.Fatal(err)
		}
		all += ids(res.GetApplications())
		pages++
		if token = res.GetNextPageToken(); token == "" {
			break
		}
	}

	if all != "a1 a2 a3 a4 a5 " || pages != 3 {
		t.Fatalf("paged ids = %q in %d pages, want all five in 3 pages", all, pages)
	}
}

func TestListApplications_JobWithoutApplicationsIsEmpty(t *testing.T) {
	client := setup(t)

	res, err := client.ListApplications(t.Context(), &pipelinev1.ListApplicationsRequest{JobId: "j-empty"})

	if err != nil || len(res.GetApplications()) != 0 {
		t.Fatalf("ListApplications = (%v, %v), want empty", res, err)
	}
}

func TestGetFunnel_ReturnsEveryStageInOrder(t *testing.T) {
	client := setup(t)

	res, err := client.GetFunnel(t.Context(), &pipelinev1.GetFunnelRequest{JobId: "j1"})
	if err != nil {
		t.Fatal(err)
	}

	got := ""
	for _, s := range res.GetStages() {
		got += fmt.Sprintf("%s=%d ", s.GetStage(), s.GetCount())
	}
	want := "STAGE_APPLIED=1 STAGE_SCREENING=2 STAGE_INTERVIEW=1 STAGE_OFFER=0 STAGE_HIRED=0 STAGE_REJECTED=1 "
	if got != want || res.GetJobId() != "j1" {
		t.Fatalf("funnel = %q, want %q", got, want)
	}
}

func TestStatusCodes(t *testing.T) {
	client := setup(t)
	ctx := t.Context()
	tests := map[string]struct {
		call func() error
		want codes.Code
	}{
		"list without job id": {func() error {
			_, err := client.ListApplications(ctx, &pipelinev1.ListApplicationsRequest{})
			return err
		}, codes.InvalidArgument},
		"negative page size": {func() error {
			_, err := client.ListApplications(ctx, &pipelinev1.ListApplicationsRequest{JobId: "j1", PageSize: -1})
			return err
		}, codes.InvalidArgument},
		"garbage page token": {func() error {
			_, err := client.ListApplications(ctx, &pipelinev1.ListApplicationsRequest{JobId: "j1", PageToken: "not-a-token"})
			return err
		}, codes.InvalidArgument},
		"unknown stage filter": {func() error {
			_, err := client.ListApplications(ctx, &pipelinev1.ListApplicationsRequest{JobId: "j1", Stage: 42})
			return err
		}, codes.InvalidArgument},
		"list for unknown job": {func() error {
			_, err := client.ListApplications(ctx, &pipelinev1.ListApplicationsRequest{JobId: "nope"})
			return err
		}, codes.NotFound},
		"funnel without job id": {func() error {
			_, err := client.GetFunnel(ctx, &pipelinev1.GetFunnelRequest{})
			return err
		}, codes.InvalidArgument},
		"funnel for unknown job": {func() error {
			_, err := client.GetFunnel(ctx, &pipelinev1.GetFunnelRequest{JobId: "nope"})
			return err
		}, codes.NotFound},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := status.Code(tt.call()); got != tt.want {
				t.Fatalf("status = %v, want %v", got, tt.want)
			}
		})
	}
}
