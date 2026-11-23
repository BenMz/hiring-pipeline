// Package server implements PipelineCommandService: it loads an aggregate by
// replaying its stream, asks it to decide, and appends the resulting events.
package server

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pipelinev1 "github.com/BenMz/hiring-pipeline/gen/go/pipeline/v1"
	"github.com/BenMz/hiring-pipeline/services/command/internal/application"
	"github.com/BenMz/hiring-pipeline/services/command/internal/codec"
	"github.com/BenMz/hiring-pipeline/services/command/internal/eventstore"
	"github.com/BenMz/hiring-pipeline/services/command/internal/job"
)

var errJobNotFound = errors.New("job not found")

// Server is the gRPC command service.
type Server struct {
	pipelinev1.UnimplementedPipelineCommandServiceServer

	store eventstore.Store
	now   func() time.Time
	log   *slog.Logger
	locks keyedMutex
}

// New returns a command server writing to store.
func New(store eventstore.Store, now func() time.Time, log *slog.Logger) *Server {
	return &Server{store: store, now: now, log: log, locks: keyedMutex{locks: map[string]chan struct{}{}}}
}

var idNamespace = uuid.MustParse("5f0c6b1e-6a43-4b8a-9d4e-6b1f0f2b7c11")

// JobID derives a job's ID from the idempotency key of the request that
// opened it, so a retried OpenJob addresses the same stream.
func JobID(companyID, idempotencyKey string) string {
	return uuid.NewSHA1(idNamespace, []byte("job\x00"+companyID+"\x00"+idempotencyKey)).String()
}

// ApplicationID derives an application's ID from the idempotency key of the
// request that submitted it, so a retried SubmitApplication finds the
// original application instead of creating a second one.
func ApplicationID(jobID, idempotencyKey string) string {
	return uuid.NewSHA1(idNamespace, []byte("application\x00"+jobID+"\x00"+idempotencyKey)).String()
}

// OpenJob implements PipelineCommandServiceServer.
func (s *Server) OpenJob(ctx context.Context, req *pipelinev1.OpenJobRequest) (*pipelinev1.OpenJobResponse, error) {
	if req.GetIdempotencyKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "idempotency_key is required")
	}
	id := JobID(req.GetCompanyId(), req.GetIdempotencyKey())

	var version int64
	err := s.inStream(ctx, id, func(history []*pipelinev1.Event) ([]*pipelinev1.Event, error) {
		j, err := s.loadJob(id, history)
		if err != nil {
			return nil, err
		}
		events, err := j.Open(job.OpenJob{
			IdempotencyKey: req.GetIdempotencyKey(),
			JobID:          id,
			CompanyID:      req.GetCompanyId(),
			Title:          req.GetTitle(),
		}, s.now())
		if err != nil {
			return nil, err
		}
		version = resultVersion(len(events) > 0, j.Version(), j.ResultVersion, req.GetIdempotencyKey())
		return codec.JobEventsToProto(events), nil
	})
	if err != nil {
		return nil, s.toStatus(err)
	}
	return &pipelinev1.OpenJobResponse{JobId: id, Version: version}, nil
}

// SubmitApplication implements PipelineCommandServiceServer.
func (s *Server) SubmitApplication(ctx context.Context, req *pipelinev1.SubmitApplicationRequest) (*pipelinev1.SubmitApplicationResponse, error) {
	if req.GetIdempotencyKey() == "" || req.GetJobId() == "" {
		return nil, status.Error(codes.InvalidArgument, "idempotency_key and job_id are required")
	}
	// Jobs never change after opening, so reading without the stream lock is safe.
	jobHistory, err := s.store.Load(ctx, req.GetJobId())
	if err != nil {
		return nil, s.toStatus(err)
	}
	j, err := s.loadJob(req.GetJobId(), jobHistory)
	if err != nil {
		return nil, s.toStatus(err)
	}
	if !j.IsOpen() {
		return nil, s.toStatus(errJobNotFound)
	}

	id := ApplicationID(req.GetJobId(), req.GetIdempotencyKey())
	var version int64
	err = s.inStream(ctx, id, func(history []*pipelinev1.Event) ([]*pipelinev1.Event, error) {
		a, err := loadApplication(id, history)
		if err != nil {
			return nil, err
		}
		events, err := a.Submit(application.SubmitApplication{
			IdempotencyKey: req.GetIdempotencyKey(),
			ApplicationID:  id,
			JobID:          req.GetJobId(),
			CandidateName:  req.GetCandidateName(),
			CandidateEmail: req.GetCandidateEmail(),
		}, s.now())
		if err != nil {
			return nil, err
		}
		version = resultVersion(len(events) > 0, a.Version(), a.ResultVersion, req.GetIdempotencyKey())
		return codec.ApplicationEventsToProto(events), nil
	})
	if err != nil {
		return nil, s.toStatus(err)
	}
	return &pipelinev1.SubmitApplicationResponse{ApplicationId: id, Version: version}, nil
}

// MoveStage implements PipelineCommandServiceServer.
func (s *Server) MoveStage(ctx context.Context, req *pipelinev1.MoveStageRequest) (*pipelinev1.MoveStageResponse, error) {
	version, err := s.onApplication(ctx, req.GetApplicationId(), req.GetIdempotencyKey(), func(a *application.Application) ([]application.Event, error) {
		return a.MoveStage(application.MoveStage{
			IdempotencyKey: req.GetIdempotencyKey(),
			ApplicationID:  req.GetApplicationId(),
			To:             application.Stage(req.GetToStage()),
		}, s.now())
	})
	if err != nil {
		return nil, s.toStatus(err)
	}
	return &pipelinev1.MoveStageResponse{Version: version}, nil
}

// RejectCandidate implements PipelineCommandServiceServer.
func (s *Server) RejectCandidate(ctx context.Context, req *pipelinev1.RejectCandidateRequest) (*pipelinev1.RejectCandidateResponse, error) {
	version, err := s.onApplication(ctx, req.GetApplicationId(), req.GetIdempotencyKey(), func(a *application.Application) ([]application.Event, error) {
		return a.Reject(application.RejectCandidate{
			IdempotencyKey: req.GetIdempotencyKey(),
			ApplicationID:  req.GetApplicationId(),
			Reason:         req.GetReason(),
		}, s.now())
	})
	if err != nil {
		return nil, s.toStatus(err)
	}
	return &pipelinev1.RejectCandidateResponse{Version: version}, nil
}

// onApplication runs a command against an existing application.
func (s *Server) onApplication(ctx context.Context, id, key string, decide func(*application.Application) ([]application.Event, error)) (int64, error) {
	if id == "" || key == "" {
		return 0, status.Error(codes.InvalidArgument, "idempotency_key and application_id are required")
	}
	var version int64
	err := s.inStream(ctx, id, func(history []*pipelinev1.Event) ([]*pipelinev1.Event, error) {
		a, err := loadApplication(id, history)
		if err != nil {
			return nil, err
		}
		events, err := decide(a)
		if err != nil {
			return nil, err
		}
		version = resultVersion(len(events) > 0, a.Version(), a.ResultVersion, key)
		return codec.ApplicationEventsToProto(events), nil
	})
	return version, err
}

// inStream serialises commands on one stream within this process, then runs
// load -> decide -> append with an optimistic version check.
func (s *Server) inStream(ctx context.Context, streamID string, decide func([]*pipelinev1.Event) ([]*pipelinev1.Event, error)) error {
	unlock, err := s.locks.lock(ctx, streamID)
	if err != nil {
		return err
	}
	defer unlock()

	history, err := s.store.Load(ctx, streamID)
	if err != nil {
		return err
	}
	events, err := decide(history)
	if err != nil || len(events) == 0 {
		return err
	}
	return s.store.Append(ctx, streamID, int64(len(history)), events...)
}

func (s *Server) loadJob(id string, history []*pipelinev1.Event) (*job.Job, error) {
	events, err := codec.JobEventsFromProto(history)
	if errors.Is(err, codec.ErrWrongStream) {
		return nil, errJobNotFound
	}
	if err != nil {
		return nil, err
	}
	return job.Load(id, events)
}

func loadApplication(id string, history []*pipelinev1.Event) (*application.Application, error) {
	events, err := codec.ApplicationEventsFromProto(history)
	if errors.Is(err, codec.ErrWrongStream) {
		return nil, application.ErrNotSubmitted
	}
	if err != nil {
		return nil, err
	}
	return application.Load(id, events)
}

// resultVersion returns the new version after a write, or the version the
// original command produced when this was a retry.
func resultVersion(wrote bool, current int64, original func(string) (int64, bool), key string) int64 {
	if wrote {
		return current
	}
	if v, ok := original(key); ok {
		return v
	}
	return current
}

func (s *Server) toStatus(err error) error {
	if _, ok := status.FromError(err); ok {
		return err
	}
	var code codes.Code
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		code = codes.DeadlineExceeded
	case errors.Is(err, context.Canceled):
		code = codes.Canceled
	case errors.Is(err, application.ErrInvalidCommand), errors.Is(err, job.ErrInvalidCommand):
		code = codes.InvalidArgument
	case errors.Is(err, application.ErrNotSubmitted), errors.Is(err, errJobNotFound):
		code = codes.NotFound
	case errors.Is(err, application.ErrIdempotencyKeyReused), errors.Is(err, job.ErrIdempotencyKeyReused),
		errors.Is(err, application.ErrAlreadySubmitted), errors.Is(err, job.ErrAlreadyOpened):
		code = codes.AlreadyExists
	case errors.Is(err, application.ErrInvalidTransition):
		code = codes.FailedPrecondition
	case errors.Is(err, eventstore.ErrConcurrencyConflict):
		code = codes.Aborted
	default:
		// Corrupt history, broker failures, etc. Log the detail, don't leak it.
		s.log.Error("command failed", "err", err)
		return status.Error(codes.Internal, "internal error")
	}
	return status.Error(code, err.Error())
}
