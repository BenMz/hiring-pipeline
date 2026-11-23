package job_test

import (
	"errors"
	"testing"
	"time"

	"github.com/BenMz/hiring-pipeline/services/command/internal/job"
)

var now = time.Date(2026, 11, 23, 9, 0, 0, 0, time.UTC)

const jobID = "job-1"

func openCmd(key string) job.OpenJob {
	return job.OpenJob{IdempotencyKey: key, JobID: jobID, CompanyID: "acme", Title: "Backend Engineer"}
}

func opened(version int64) job.Event {
	return job.Opened{
		Meta:      job.Meta{JobID: jobID, Version: version, IdempotencyKey: "k1", OccurredAt: now},
		CompanyID: "acme",
		Title:     "Backend Engineer",
	}
}

func TestOpen_NewJob_EmitsOpened(t *testing.T) {
	j := job.New(jobID)

	events, err := j.Open(openCmd("k1"), now)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if len(events) != 1 || events[0] != opened(1) {
		t.Fatalf("events = %#v, want [%#v]", events, opened(1))
	}
	if !j.IsOpen() || j.Version() != 1 {
		t.Fatalf("state = (open=%v, v%d), want (true, v1)", j.IsOpen(), j.Version())
	}
}

func TestOpen_InvalidCommand(t *testing.T) {
	tests := map[string]func(*job.OpenJob){
		"missing idempotency key": func(c *job.OpenJob) { c.IdempotencyKey = "" },
		"missing company":         func(c *job.OpenJob) { c.CompanyID = "" },
		"blank title":             func(c *job.OpenJob) { c.Title = "   " },
		"wrong job id":            func(c *job.OpenJob) { c.JobID = "other" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			cmd := openCmd("k1")
			mutate(&cmd)

			events, err := job.New(jobID).Open(cmd, now)

			if !errors.Is(err, job.ErrInvalidCommand) || len(events) != 0 {
				t.Fatalf("Open = (%v, %v), want (no events, ErrInvalidCommand)", events, err)
			}
		})
	}
}

func TestOpen_Retry_EmitsNothing(t *testing.T) {
	j := mustLoad(t, opened(1))

	events, err := j.Open(openCmd("k1"), now)

	if err != nil || len(events) != 0 {
		t.Fatalf("retry = (%v, %v), want (no events, nil)", events, err)
	}
	if v, ok := j.ResultVersion("k1"); !ok || v != 1 {
		t.Fatalf("ResultVersion = (%d, %v), want (1, true)", v, ok)
	}
}

func TestOpen_KeyReusedWithDifferentTitle_Fails(t *testing.T) {
	j := mustLoad(t, opened(1))
	cmd := openCmd("k1")
	cmd.Title = "Frontend Engineer"

	if _, err := j.Open(cmd, now); !errors.Is(err, job.ErrIdempotencyKeyReused) {
		t.Fatalf("err = %v, want ErrIdempotencyKeyReused", err)
	}
}

func TestOpen_AlreadyOpen_WithNewKey_Fails(t *testing.T) {
	j := mustLoad(t, opened(1))

	if _, err := j.Open(openCmd("k2"), now); !errors.Is(err, job.ErrAlreadyOpened) {
		t.Fatalf("err = %v, want ErrAlreadyOpened", err)
	}
}

func TestLoad_RejectsCorruptHistory(t *testing.T) {
	tests := map[string][]job.Event{
		"not starting at v1": {opened(2)},
		"foreign aggregate":  {job.Opened{Meta: job.Meta{JobID: "other", Version: 1}}},
	}
	for name, history := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := job.Load(jobID, history); !errors.Is(err, job.ErrCorruptHistory) {
				t.Fatalf("err = %v, want ErrCorruptHistory", err)
			}
		})
	}
}

func mustLoad(t *testing.T, history ...job.Event) *job.Job {
	t.Helper()
	j, err := job.Load(jobID, history)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return j
}
