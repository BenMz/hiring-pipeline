//go:build integration

// Package integration runs the real command service against a real broker.
package integration

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	pipelinev1 "github.com/BenMz/hiring-pipeline/gen/go/pipeline/v1"
	"github.com/BenMz/hiring-pipeline/internal/testkafka"
	"github.com/BenMz/hiring-pipeline/services/command/app"
)

var broker string

func TestMain(m *testing.M) {
	b, stop, err := testkafka.Start(context.Background())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	broker = b
	code := m.Run()
	stop()
	os.Exit(code)
}

// startService runs pipeline-command in-process on a random port and returns
// a client plus a function that shuts the service down.
func startService(t *testing.T, topic string) (pipelinev1.PipelineCommandServiceClient, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	cfg := app.Config{Brokers: []string{broker}, Topic: topic, RequestTimeout: 10 * time.Second, StartupTimeout: 30 * time.Second}
	go func() { done <- app.Run(ctx, cfg, lis, slog.New(slog.NewTextHandler(t.Output(), nil))) }()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	readyCtx, readyCancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer readyCancel()
	if _, err := healthpb.NewHealthClient(conn).Check(readyCtx, &healthpb.HealthCheckRequest{}, grpc.WaitForReady(true)); err != nil {
		cancel()
		t.Fatalf("service not ready: %v (run: %v)", err, <-done)
	}

	var stopped bool
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		_ = conn.Close()
		cancel()
		if err := <-done; err != nil {
			t.Errorf("service exited with %v", err)
		}
	}
	t.Cleanup(stop)
	return pipelinev1.NewPipelineCommandServiceClient(conn), stop
}

// stream decodes the events of one aggregate from the topic.
func stream(t *testing.T, topic, id string) []*pipelinev1.Event {
	t.Helper()
	var events []*pipelinev1.Event
	partition := int32(-1)
	for _, r := range testkafka.ReadAll(t, broker, topic) {
		if string(r.Key) != id {
			continue
		}
		if partition != -1 && r.Partition != partition {
			t.Fatalf("stream %s is split across partitions %d and %d", id, partition, r.Partition)
		}
		partition = r.Partition
		e := &pipelinev1.Event{}
		if err := proto.Unmarshal(r.Value, e); err != nil {
			t.Fatal(err)
		}
		events = append(events, e)
	}
	// Records from one partition are returned in offset order.
	return events
}

func TestCommandFlow_EventsLandInKafka(t *testing.T) {
	topic := testkafka.CreateTopic(t, broker)
	client, _ := startService(t, topic)
	ctx := t.Context()

	job, err := client.OpenJob(ctx, &pipelinev1.OpenJobRequest{IdempotencyKey: "job-1", CompanyId: "acme", Title: "Backend Engineer"})
	if err != nil {
		t.Fatal(err)
	}
	app, err := client.SubmitApplication(ctx, &pipelinev1.SubmitApplicationRequest{
		IdempotencyKey: "apply-1", JobId: job.GetJobId(), CandidateName: "Ada Lovelace", CandidateEmail: "ada@example.com",
	})
	if err != nil {
		t.Fatal(err)
	}
	for i, to := range []pipelinev1.Stage{pipelinev1.Stage_STAGE_SCREENING, pipelinev1.Stage_STAGE_INTERVIEW} {
		if _, err := client.MoveStage(ctx, &pipelinev1.MoveStageRequest{
			IdempotencyKey: fmt.Sprintf("move-%d", i), ApplicationId: app.GetApplicationId(), ToStage: to,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := client.RejectCandidate(ctx, &pipelinev1.RejectCandidateRequest{
		IdempotencyKey: "reject-1", ApplicationId: app.GetApplicationId(), Reason: "position filled",
	}); err != nil {
		t.Fatal(err)
	}

	events := stream(t, topic, app.GetApplicationId())
	want := []string{"application_submitted", "stage_changed", "stage_changed", "candidate_rejected"}
	if len(events) != len(want) {
		t.Fatalf("application stream has %d events, want %d", len(events), len(want))
	}
	for i, e := range events {
		got := string(e.ProtoReflect().WhichOneof(e.ProtoReflect().Descriptor().Oneofs().ByName("payload")).Name())
		if got != want[i] || e.GetMetadata().GetVersion() != int64(i+1) {
			t.Fatalf("event %d = %s v%d, want %s v%d", i, got, e.GetMetadata().GetVersion(), want[i], i+1)
		}
	}
	if r := events[3].GetCandidateRejected(); r.GetFromStage() != pipelinev1.Stage_STAGE_INTERVIEW || r.GetReason() != "position filled" {
		t.Fatalf("rejection = %v, want from INTERVIEW with reason", r)
	}
	if n := len(stream(t, topic, job.GetJobId())); n != 1 {
		t.Fatalf("job stream has %d events, want 1", n)
	}
}

func TestRetriedCommand_WritesNoSecondEvent_EvenAfterRestart(t *testing.T) {
	topic := testkafka.CreateTopic(t, broker)
	submit := &pipelinev1.SubmitApplicationRequest{IdempotencyKey: "apply-1", CandidateName: "Ada Lovelace", CandidateEmail: "ada@example.com"}

	client, stop := startService(t, topic)
	job, err := client.OpenJob(t.Context(), &pipelinev1.OpenJobRequest{IdempotencyKey: "job-1", CompanyId: "acme", Title: "Backend Engineer"})
	if err != nil {
		t.Fatal(err)
	}
	submit.JobId = job.GetJobId()
	first, err := client.SubmitApplication(t.Context(), submit)
	if err != nil {
		t.Fatal(err)
	}
	move := &pipelinev1.MoveStageRequest{IdempotencyKey: "move-1", ApplicationId: first.GetApplicationId(), ToStage: pipelinev1.Stage_STAGE_SCREENING}
	if _, err := client.MoveStage(t.Context(), move); err != nil {
		t.Fatal(err)
	}
	stop()

	// A fresh process knows nothing but what it replays from the topic.
	client, _ = startService(t, topic)
	retried, err := client.SubmitApplication(t.Context(), submit)
	if err != nil {
		t.Fatalf("retried submit: %v", err)
	}
	retriedMove, err := client.MoveStage(t.Context(), move)
	if err != nil {
		t.Fatalf("retried move: %v", err)
	}
	next, err := client.MoveStage(t.Context(), &pipelinev1.MoveStageRequest{IdempotencyKey: "move-2", ApplicationId: first.GetApplicationId(), ToStage: pipelinev1.Stage_STAGE_INTERVIEW})
	if err != nil {
		t.Fatalf("new command after restart: %v", err)
	}

	if retried.GetApplicationId() != first.GetApplicationId() || retried.GetVersion() != 1 || retriedMove.GetVersion() != 2 {
		t.Fatalf("retries returned (%s v%d, move v%d), want the original (%s v1, move v2)",
			retried.GetApplicationId(), retried.GetVersion(), retriedMove.GetVersion(), first.GetApplicationId())
	}
	if next.GetVersion() != 3 {
		t.Fatalf("new move version = %d, want 3", next.GetVersion())
	}
	if n := len(stream(t, topic, first.GetApplicationId())); n != 3 {
		t.Fatalf("application stream has %d events, want 3 (submit, move, move)", n)
	}
}

func TestInvalidTransition_IsRejectedAndWritesNothing(t *testing.T) {
	topic := testkafka.CreateTopic(t, broker)
	client, _ := startService(t, topic)
	job, err := client.OpenJob(t.Context(), &pipelinev1.OpenJobRequest{IdempotencyKey: "job-1", CompanyId: "acme", Title: "Engineer"})
	if err != nil {
		t.Fatal(err)
	}
	app, err := client.SubmitApplication(t.Context(), &pipelinev1.SubmitApplicationRequest{
		IdempotencyKey: "apply-1", JobId: job.GetJobId(), CandidateName: "Ada", CandidateEmail: "ada@example.com",
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.MoveStage(t.Context(), &pipelinev1.MoveStageRequest{IdempotencyKey: "skip", ApplicationId: app.GetApplicationId(), ToStage: pipelinev1.Stage_STAGE_OFFER})

	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("status = %v, want FailedPrecondition", err)
	}
	if n := len(stream(t, topic, app.GetApplicationId())); n != 1 {
		t.Fatalf("application stream has %d events, want 1", n)
	}
}
