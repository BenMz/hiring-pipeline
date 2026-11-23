package codec_test

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/BenMz/hiring-pipeline/services/command/internal/application"
	"github.com/BenMz/hiring-pipeline/services/command/internal/codec"
	"github.com/BenMz/hiring-pipeline/services/command/internal/job"
)

var at = time.Date(2026, 11, 23, 9, 30, 0, 0, time.UTC)

func TestApplicationEvents_RoundTrip(t *testing.T) {
	events := []application.Event{
		application.ApplicationSubmitted{
			Meta:  application.Meta{ApplicationID: "a1", Version: 1, IdempotencyKey: "k1", OccurredAt: at},
			JobID: "j1", CandidateName: "Ada", CandidateEmail: "ada@example.com",
		},
		application.StageChanged{
			Meta: application.Meta{ApplicationID: "a1", Version: 2, IdempotencyKey: "k2", OccurredAt: at},
			From: application.Applied, To: application.Screening,
		},
		application.CandidateRejected{
			Meta: application.Meta{ApplicationID: "a1", Version: 3, IdempotencyKey: "k3", OccurredAt: at},
			From: application.Screening, Reason: "no visa",
		},
	}

	pb := codec.ApplicationEventsToProto(events)
	got, err := codec.ApplicationEventsFromProto(pb)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(got, events) {
		t.Fatalf("round trip = %#v, want %#v", got, events)
	}
	if pb[1].GetMetadata().GetEventId() != codec.EventID("a1", 2) {
		t.Fatalf("event id = %q, want deterministic id", pb[1].GetMetadata().GetEventId())
	}
	if pb[1].GetStageChanged().GetToStage().String() != "STAGE_SCREENING" {
		t.Fatalf("stage mapped to %v, want STAGE_SCREENING", pb[1].GetStageChanged().GetToStage())
	}
}

func TestJobEvents_RoundTrip(t *testing.T) {
	events := []job.Event{job.Opened{
		Meta:      job.Meta{JobID: "j1", Version: 1, IdempotencyKey: "k1", OccurredAt: at},
		CompanyID: "acme", Title: "Backend Engineer",
	}}

	got, err := codec.JobEventsFromProto(codec.JobEventsToProto(events))
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(got, events) {
		t.Fatalf("round trip = %#v, want %#v", got, events)
	}
}

func TestDecodingTheWrongStreamType_Fails(t *testing.T) {
	jobStream := codec.JobEventsToProto([]job.Event{job.Opened{Meta: job.Meta{JobID: "j1", Version: 1}}})

	if _, err := codec.ApplicationEventsFromProto(jobStream); !errors.Is(err, codec.ErrWrongStream) {
		t.Fatalf("err = %v, want ErrWrongStream", err)
	}
}

func TestEventID_IsDeterministicAndUnique(t *testing.T) {
	first, again := codec.EventID("a1", 1), codec.EventID("a1", 1)
	if first != again {
		t.Fatal("same position produced different ids")
	}
	if first == codec.EventID("a1", 2) || first == codec.EventID("a2", 1) {
		t.Fatal("different positions produced the same id")
	}
}
