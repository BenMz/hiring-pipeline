// Package codec maps domain events to and from the protobuf envelope that is
// stored in Kafka. The domain packages stay free of wire-format concerns.
package codec

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	pipelinev1 "github.com/BenMz/hiring-pipeline/gen/go/pipeline/v1"
	"github.com/BenMz/hiring-pipeline/services/command/internal/application"
	"github.com/BenMz/hiring-pipeline/services/command/internal/job"
)

// ErrWrongStream means a stream holds events of another aggregate type, for
// example when a job ID is used where an application ID is expected.
var ErrWrongStream = errors.New("stream belongs to another aggregate type")

var eventIDNamespace = uuid.MustParse("8d9b7c52-2a51-4c1e-9f0a-3e1d6f4b2a90")

// EventID is derived from the stream position, so the same event always has
// the same ID. Consumers can use it to drop duplicates.
func EventID(streamID string, version int64) string {
	return uuid.NewSHA1(eventIDNamespace, fmt.Appendf(nil, "%s\x00%d", streamID, version)).String()
}

// ApplicationEventsToProto converts application events to envelopes.
func ApplicationEventsToProto(events []application.Event) []*pipelinev1.Event {
	out := make([]*pipelinev1.Event, 0, len(events))
	for _, e := range events {
		m := e.Metadata()
		pe := &pipelinev1.Event{Metadata: metadata(m.ApplicationID, m.Version, m.IdempotencyKey, m.OccurredAt.UTC())}
		switch e := e.(type) {
		case application.ApplicationSubmitted:
			pe.Payload = &pipelinev1.Event_ApplicationSubmitted{ApplicationSubmitted: &pipelinev1.ApplicationSubmitted{
				ApplicationId:  m.ApplicationID,
				JobId:          e.JobID,
				CandidateName:  e.CandidateName,
				CandidateEmail: e.CandidateEmail,
			}}
		case application.StageChanged:
			pe.Payload = &pipelinev1.Event_StageChanged{StageChanged: &pipelinev1.StageChanged{
				ApplicationId: m.ApplicationID,
				FromStage:     pipelinev1.Stage(e.From),
				ToStage:       pipelinev1.Stage(e.To),
			}}
		case application.CandidateRejected:
			pe.Payload = &pipelinev1.Event_CandidateRejected{CandidateRejected: &pipelinev1.CandidateRejected{
				ApplicationId: m.ApplicationID,
				FromStage:     pipelinev1.Stage(e.From),
				Reason:        e.Reason,
			}}
		default:
			panic(fmt.Sprintf("codec: unknown application event %T", e))
		}
		out = append(out, pe)
	}
	return out
}

// ApplicationEventsFromProto converts envelopes back to application events.
func ApplicationEventsFromProto(events []*pipelinev1.Event) ([]application.Event, error) {
	out := make([]application.Event, 0, len(events))
	for _, pe := range events {
		md := pe.GetMetadata()
		m := application.Meta{
			ApplicationID:  md.GetAggregateId(),
			Version:        md.GetVersion(),
			IdempotencyKey: md.GetIdempotencyKey(),
			OccurredAt:     md.GetOccurredAt().AsTime(),
		}
		switch p := pe.GetPayload().(type) {
		case *pipelinev1.Event_ApplicationSubmitted:
			out = append(out, application.ApplicationSubmitted{
				Meta:           m,
				JobID:          p.ApplicationSubmitted.GetJobId(),
				CandidateName:  p.ApplicationSubmitted.GetCandidateName(),
				CandidateEmail: p.ApplicationSubmitted.GetCandidateEmail(),
			})
		case *pipelinev1.Event_StageChanged:
			out = append(out, application.StageChanged{
				Meta: m,
				From: application.Stage(p.StageChanged.GetFromStage()),
				To:   application.Stage(p.StageChanged.GetToStage()),
			})
		case *pipelinev1.Event_CandidateRejected:
			out = append(out, application.CandidateRejected{
				Meta:   m,
				From:   application.Stage(p.CandidateRejected.GetFromStage()),
				Reason: p.CandidateRejected.GetReason(),
			})
		default:
			return nil, fmt.Errorf("%w: %T in application stream %q", ErrWrongStream, p, m.ApplicationID)
		}
	}
	return out, nil
}

// JobEventsToProto converts job events to envelopes.
func JobEventsToProto(events []job.Event) []*pipelinev1.Event {
	out := make([]*pipelinev1.Event, 0, len(events))
	for _, e := range events {
		m := e.Metadata()
		pe := &pipelinev1.Event{Metadata: metadata(m.JobID, m.Version, m.IdempotencyKey, m.OccurredAt.UTC())}
		switch e := e.(type) {
		case job.Opened:
			pe.Payload = &pipelinev1.Event_JobOpened{JobOpened: &pipelinev1.JobOpened{
				JobId:     m.JobID,
				CompanyId: e.CompanyID,
				Title:     e.Title,
			}}
		default:
			panic(fmt.Sprintf("codec: unknown job event %T", e))
		}
		out = append(out, pe)
	}
	return out
}

// JobEventsFromProto converts envelopes back to job events.
func JobEventsFromProto(events []*pipelinev1.Event) ([]job.Event, error) {
	out := make([]job.Event, 0, len(events))
	for _, pe := range events {
		md := pe.GetMetadata()
		m := job.Meta{
			JobID:          md.GetAggregateId(),
			Version:        md.GetVersion(),
			IdempotencyKey: md.GetIdempotencyKey(),
			OccurredAt:     md.GetOccurredAt().AsTime(),
		}
		switch p := pe.GetPayload().(type) {
		case *pipelinev1.Event_JobOpened:
			out = append(out, job.Opened{Meta: m, CompanyID: p.JobOpened.GetCompanyId(), Title: p.JobOpened.GetTitle()})
		default:
			return nil, fmt.Errorf("%w: %T in job stream %q", ErrWrongStream, p, m.JobID)
		}
	}
	return out, nil
}

func metadata(streamID string, version int64, key string, at time.Time) *pipelinev1.EventMetadata {
	return &pipelinev1.EventMetadata{
		EventId:        EventID(streamID, version),
		AggregateId:    streamID,
		Version:        version,
		IdempotencyKey: key,
		OccurredAt:     timestamppb.New(at),
	}
}
