package server_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	pipelinev1 "github.com/BenMz/hiring-pipeline/gen/go/pipeline/v1"
	"github.com/BenMz/hiring-pipeline/services/command/internal/eventstore"
	"github.com/BenMz/hiring-pipeline/services/command/internal/server"
)

var now = time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)

type harness struct {
	client pipelinev1.PipelineCommandServiceClient
	store  *eventstore.Memory
}

func newHarness(t *testing.T, store eventstore.Store, maxTimeout time.Duration) pipelinev1.PipelineCommandServiceClient {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(server.Logging(log), server.Deadline(maxTimeout)))
	pipelinev1.RegisterPipelineCommandServiceServer(srv, server.New(store, func() time.Time { return now }, log))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return pipelinev1.NewPipelineCommandServiceClient(conn)
}

func setup(t *testing.T) harness {
	t.Helper()
	store := eventstore.NewMemory()
	return harness{client: newHarness(t, store, 5*time.Second), store: store}
}

func (h harness) openJob(t *testing.T) string {
	t.Helper()
	res, err := h.client.OpenJob(t.Context(), &pipelinev1.OpenJobRequest{IdempotencyKey: "job-key", CompanyId: "acme", Title: "Backend Engineer"})
	if err != nil {
		t.Fatalf("OpenJob: %v", err)
	}
	return res.GetJobId()
}

func (h harness) submit(t *testing.T, jobID string) *pipelinev1.SubmitApplicationResponse {
	t.Helper()
	res, err := h.client.SubmitApplication(t.Context(), submitReq(jobID, "k1"))
	if err != nil {
		t.Fatalf("SubmitApplication: %v", err)
	}
	return res
}

func (h harness) events(t *testing.T, streamID string) []*pipelinev1.Event {
	t.Helper()
	events, err := h.store.Load(t.Context(), streamID)
	if err != nil {
		t.Fatal(err)
	}
	return events
}

func submitReq(jobID, key string) *pipelinev1.SubmitApplicationRequest {
	return &pipelinev1.SubmitApplicationRequest{IdempotencyKey: key, JobId: jobID, CandidateName: "Ada Lovelace", CandidateEmail: "ada@example.com"}
}

func wantCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if got := status.Code(err); got != want {
		t.Fatalf("status = %v (%v), want %v", got, err, want)
	}
}

func TestOpenJob_WritesJobOpened(t *testing.T) {
	h := setup(t)

	jobID := h.openJob(t)

	events := h.events(t, jobID)
	if len(events) != 1 || events[0].GetJobOpened().GetTitle() != "Backend Engineer" {
		t.Fatalf("job stream = %v, want one JobOpened", events)
	}
}

func TestSubmitApplication_WritesApplicationSubmitted(t *testing.T) {
	h := setup(t)
	jobID := h.openJob(t)

	res := h.submit(t, jobID)

	if res.GetApplicationId() == "" || res.GetVersion() != 1 {
		t.Fatalf("response = %v, want an id at version 1", res)
	}
	events := h.events(t, res.GetApplicationId())
	if len(events) != 1 || events[0].GetApplicationSubmitted().GetJobId() != jobID {
		t.Fatalf("application stream = %v, want one ApplicationSubmitted", events)
	}
}

func TestRetriedCommands_WriteNoSecondEvent(t *testing.T) {
	h := setup(t)
	jobID := h.openJob(t)
	first := h.submit(t, jobID)
	appID := first.GetApplicationId()
	move := &pipelinev1.MoveStageRequest{IdempotencyKey: "k-move", ApplicationId: appID, ToStage: pipelinev1.Stage_STAGE_SCREENING}
	if _, err := h.client.MoveStage(t.Context(), move); err != nil {
		t.Fatal(err)
	}

	// Retry every command, as a client would after a timeout.
	if again := h.openJob(t); again != jobID {
		t.Fatalf("retried OpenJob returned job %q, want %q", again, jobID)
	}
	retry := h.submit(t, jobID)
	moveRetry, err := h.client.MoveStage(t.Context(), move)
	if err != nil {
		t.Fatalf("retried MoveStage: %v", err)
	}

	if retry.GetApplicationId() != appID || retry.GetVersion() != 1 {
		t.Fatalf("retried submit = %v, want the original (%s, v1)", retry, appID)
	}
	if moveRetry.GetVersion() != 2 {
		t.Fatalf("retried move version = %d, want 2", moveRetry.GetVersion())
	}
	if n := len(h.events(t, jobID)); n != 1 {
		t.Fatalf("job stream has %d events, want 1", n)
	}
	if n := len(h.events(t, appID)); n != 2 {
		t.Fatalf("application stream has %d events, want 2", n)
	}
}

func TestMoveStageAndReject_HappyPath(t *testing.T) {
	h := setup(t)
	appID := h.submit(t, h.openJob(t)).GetApplicationId()

	moved, err := h.client.MoveStage(t.Context(), &pipelinev1.MoveStageRequest{IdempotencyKey: "k2", ApplicationId: appID, ToStage: pipelinev1.Stage_STAGE_SCREENING})
	if err != nil {
		t.Fatal(err)
	}
	rejected, err := h.client.RejectCandidate(t.Context(), &pipelinev1.RejectCandidateRequest{IdempotencyKey: "k3", ApplicationId: appID, Reason: "position filled"})
	if err != nil {
		t.Fatal(err)
	}

	if moved.GetVersion() != 2 || rejected.GetVersion() != 3 {
		t.Fatalf("versions = %d, %d; want 2, 3", moved.GetVersion(), rejected.GetVersion())
	}
	events := h.events(t, appID)
	if got := events[2].GetCandidateRejected(); got.GetFromStage() != pipelinev1.Stage_STAGE_SCREENING || got.GetReason() != "position filled" {
		t.Fatalf("last event = %v, want CandidateRejected from SCREENING", events[2])
	}
}

func TestStatusCodes(t *testing.T) {
	h := setup(t)
	jobID := h.openJob(t)
	appID := h.submit(t, jobID).GetApplicationId()
	ctx := t.Context()

	tests := map[string]struct {
		call func() error
		want codes.Code
	}{
		"missing idempotency key": {func() error {
			_, err := h.client.SubmitApplication(ctx, submitReq(jobID, ""))
			return err
		}, codes.InvalidArgument},
		"invalid email": {func() error {
			req := submitReq(jobID, "k-new")
			req.CandidateEmail = "nope"
			_, err := h.client.SubmitApplication(ctx, req)
			return err
		}, codes.InvalidArgument},
		"missing application id": {func() error {
			_, err := h.client.MoveStage(ctx, &pipelinev1.MoveStageRequest{IdempotencyKey: "k", ToStage: pipelinev1.Stage_STAGE_SCREENING})
			return err
		}, codes.InvalidArgument},
		"unknown job": {func() error {
			_, err := h.client.SubmitApplication(ctx, submitReq("no-such-job", "k-new"))
			return err
		}, codes.NotFound},
		"unknown application": {func() error {
			_, err := h.client.MoveStage(ctx, &pipelinev1.MoveStageRequest{IdempotencyKey: "k", ApplicationId: "no-such-app", ToStage: pipelinev1.Stage_STAGE_SCREENING})
			return err
		}, codes.NotFound},
		"job id used as application id": {func() error {
			_, err := h.client.MoveStage(ctx, &pipelinev1.MoveStageRequest{IdempotencyKey: "k", ApplicationId: jobID, ToStage: pipelinev1.Stage_STAGE_SCREENING})
			return err
		}, codes.NotFound},
		"application id used as job id": {func() error {
			_, err := h.client.SubmitApplication(ctx, submitReq(appID, "k-new"))
			return err
		}, codes.NotFound},
		"invalid transition": {func() error {
			_, err := h.client.MoveStage(ctx, &pipelinev1.MoveStageRequest{IdempotencyKey: "k-skip", ApplicationId: appID, ToStage: pipelinev1.Stage_STAGE_OFFER})
			return err
		}, codes.FailedPrecondition},
		"idempotency key reused for a different request": {func() error {
			req := submitReq(jobID, "k1")
			req.CandidateEmail = "someone@else.com"
			_, err := h.client.SubmitApplication(ctx, req)
			return err
		}, codes.AlreadyExists},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			wantCode(t, tt.call(), tt.want)
		})
	}
}

func TestConcurrencyConflict_IsAborted(t *testing.T) {
	client := newHarness(t, conflictingStore{eventstore.NewMemory()}, 5*time.Second)

	_, err := client.OpenJob(t.Context(), &pipelinev1.OpenJobRequest{IdempotencyKey: "k", CompanyId: "acme", Title: "Engineer"})

	wantCode(t, err, codes.Aborted)
}

func TestDeadline_IsAppliedWhenClientSetsNone(t *testing.T) {
	client := newHarness(t, hangingStore{}, 50*time.Millisecond)
	start := time.Now()

	_, err := client.OpenJob(context.Background(), &pipelinev1.OpenJobRequest{IdempotencyKey: "k", CompanyId: "acme", Title: "Engineer"})

	wantCode(t, err, codes.DeadlineExceeded)
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("call took %v; the server deadline was not applied", elapsed)
	}
}

func TestDeadline_ClientDeadlineIsRespected(t *testing.T) {
	client := newHarness(t, hangingStore{}, time.Minute)
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	_, err := client.OpenJob(ctx, &pipelinev1.OpenJobRequest{IdempotencyKey: "k", CompanyId: "acme", Title: "Engineer"})

	wantCode(t, err, codes.DeadlineExceeded)
}

// hangingStore blocks until the request context ends, like an unreachable broker.
type hangingStore struct{}

func (hangingStore) Load(ctx context.Context, _ string) ([]*pipelinev1.Event, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (hangingStore) Append(ctx context.Context, _ string, _ int64, _ ...*pipelinev1.Event) error {
	<-ctx.Done()
	return ctx.Err()
}

// conflictingStore simulates another writer appending between Load and Append.
type conflictingStore struct{ *eventstore.Memory }

func (conflictingStore) Append(context.Context, string, int64, ...*pipelinev1.Event) error {
	return eventstore.ErrConcurrencyConflict
}
