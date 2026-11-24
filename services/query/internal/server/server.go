// Package server implements PipelineQueryService over the PostgreSQL read
// models. The read models are eventually consistent: a job or application
// appears here shortly after its event is written, once the projector has
// applied it.
package server

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	pipelinev1 "github.com/BenMz/hiring-pipeline/gen/go/pipeline/v1"
)

const (
	defaultPageSize = 50
	maxPageSize     = 500
)

// funnelOrder is the order stages are returned in by GetFunnel.
var funnelOrder = []pipelinev1.Stage{
	pipelinev1.Stage_STAGE_APPLIED,
	pipelinev1.Stage_STAGE_SCREENING,
	pipelinev1.Stage_STAGE_INTERVIEW,
	pipelinev1.Stage_STAGE_OFFER,
	pipelinev1.Stage_STAGE_HIRED,
	pipelinev1.Stage_STAGE_REJECTED,
}

// Server is the gRPC query service.
type Server struct {
	pipelinev1.UnimplementedPipelineQueryServiceServer
	pool *pgxpool.Pool
	log  *slog.Logger
}

// New returns a query server reading from pool.
func New(pool *pgxpool.Pool, log *slog.Logger) *Server {
	return &Server{pool: pool, log: log}
}

const listApplications = `
SELECT application_id, job_id, candidate_name, candidate_email, stage, submitted_at, updated_at
  FROM applications_by_job
 WHERE job_id = $1
   AND ($2::text = '' OR stage = $2)
   AND ($3::timestamptz IS NULL OR (submitted_at, application_id) > ($3, $4::text))
 ORDER BY submitted_at, application_id
 LIMIT $5`

// ListApplications implements PipelineQueryServiceServer.
func (s *Server) ListApplications(ctx context.Context, req *pipelinev1.ListApplicationsRequest) (*pipelinev1.ListApplicationsResponse, error) {
	if req.GetJobId() == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	size := int(req.GetPageSize())
	switch {
	case size < 0:
		return nil, status.Error(codes.InvalidArgument, "page_size must not be negative")
	case size == 0:
		size = defaultPageSize
	case size > maxPageSize:
		size = maxPageSize
	}
	stage := ""
	if req.GetStage() != pipelinev1.Stage_STAGE_UNSPECIFIED {
		if _, ok := pipelinev1.Stage_name[int32(req.GetStage())]; !ok {
			return nil, status.Errorf(codes.InvalidArgument, "unknown stage %d", req.GetStage())
		}
		stage = stageName(req.GetStage())
	}
	var after *time.Time
	var afterID string
	if req.GetPageToken() != "" {
		c, err := decodeCursor(req.GetPageToken())
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid page_token")
		}
		after, afterID = &c.SubmittedAt, c.ApplicationID
	}

	// Fetch one extra row to learn whether there is a next page.
	rows, err := s.pool.Query(ctx, listApplications, req.GetJobId(), stage, after, afterID, size+1)
	if err != nil {
		return nil, s.internal(ctx, err)
	}
	apps, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*pipelinev1.ApplicationSummary, error) {
		var a pipelinev1.ApplicationSummary
		var st string
		var submitted, updated time.Time
		if err := row.Scan(&a.ApplicationId, &a.JobId, &a.CandidateName, &a.CandidateEmail, &st, &submitted, &updated); err != nil {
			return nil, err
		}
		a.Stage = pipelinev1.Stage(pipelinev1.Stage_value["STAGE_"+st])
		a.SubmittedAt, a.UpdatedAt = timestamppb.New(submitted), timestamppb.New(updated)
		return &a, nil
	})
	if err != nil {
		return nil, s.internal(ctx, err)
	}

	res := &pipelinev1.ListApplicationsResponse{Applications: apps}
	if len(apps) > size {
		res.Applications = apps[:size]
		last := res.Applications[size-1]
		res.NextPageToken = encodeCursor(cursor{SubmittedAt: last.GetSubmittedAt().AsTime(), ApplicationID: last.GetApplicationId()})
	}
	if len(apps) == 0 && req.GetPageToken() == "" {
		if err := s.requireJob(ctx, req.GetJobId()); err != nil {
			return nil, err
		}
	}
	return res, nil
}

// GetFunnel implements PipelineQueryServiceServer.
func (s *Server) GetFunnel(ctx context.Context, req *pipelinev1.GetFunnelRequest) (*pipelinev1.GetFunnelResponse, error) {
	if req.GetJobId() == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	rows, err := s.pool.Query(ctx, "SELECT stage, count FROM stage_counts WHERE job_id = $1", req.GetJobId())
	if err != nil {
		return nil, s.internal(ctx, err)
	}
	counts := map[string]int64{}
	var st string
	var n int64
	if _, err := pgx.ForEachRow(rows, []any{&st, &n}, func() error { counts[st] = n; return nil }); err != nil {
		return nil, s.internal(ctx, err)
	}
	if len(counts) == 0 {
		if err := s.requireJob(ctx, req.GetJobId()); err != nil {
			return nil, err
		}
	}

	res := &pipelinev1.GetFunnelResponse{JobId: req.GetJobId()}
	for _, stage := range funnelOrder {
		res.Stages = append(res.Stages, &pipelinev1.StageCount{Stage: stage, Count: counts[stageName(stage)]})
	}
	return res, nil
}

func (s *Server) requireJob(ctx context.Context, jobID string) error {
	var exists bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM jobs WHERE job_id = $1)
	                               OR EXISTS (SELECT 1 FROM applications_by_job WHERE job_id = $1)`, jobID).Scan(&exists)
	if err != nil {
		return s.internal(ctx, err)
	}
	if !exists {
		return status.Errorf(codes.NotFound, "job %q not found", jobID)
	}
	return nil
}

func (s *Server) internal(ctx context.Context, err error) error {
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.Is(ctx.Err(), context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "deadline exceeded")
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "canceled")
	}
	s.log.Error("query failed", "err", err)
	return status.Error(codes.Internal, "internal error")
}

func stageName(s pipelinev1.Stage) string {
	return s.String()[len("STAGE_"):]
}
