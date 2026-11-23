// Package job holds the job aggregate. A job must be open before candidates
// can apply to it.
package job

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Errors returned by command methods; the gRPC layer maps them to status codes.
var (
	ErrInvalidCommand       = errors.New("invalid command")
	ErrAlreadyOpened        = errors.New("job already opened")
	ErrIdempotencyKeyReused = errors.New("idempotency key reused for a different command")
	ErrCorruptHistory       = errors.New("corrupt event history")
)

// OpenJob asks to open a job for applications.
type OpenJob struct {
	IdempotencyKey string
	JobID          string
	CompanyID      string
	Title          string
}

// Event is a fact that happened to a job.
type Event interface {
	Metadata() Meta
}

// Meta is the envelope data shared by all job events.
type Meta struct {
	JobID          string
	Version        int64
	IdempotencyKey string
	OccurredAt     time.Time
}

// Metadata returns the event envelope.
func (m Meta) Metadata() Meta { return m }

// Opened starts a job's stream.
type Opened struct {
	Meta
	CompanyID string
	Title     string
}

// Job is the aggregate for one open position.
type Job struct {
	id        string
	companyID string
	title     string
	open      bool
	version   int64
	processed map[string]processedCommand
}

type processedCommand struct {
	fingerprint string
	version     int64
}

// New returns a job that has not been opened yet.
func New(id string) *Job {
	return &Job{id: id, processed: map[string]processedCommand{}}
}

// Load rebuilds a job by replaying its events in order.
func Load(id string, history []Event) (*Job, error) {
	j := New(id)
	for _, e := range history {
		m := e.Metadata()
		if m.JobID != id {
			return nil, fmt.Errorf("%w: event for %q in stream of %q", ErrCorruptHistory, m.JobID, id)
		}
		if m.Version != j.version+1 {
			return nil, fmt.Errorf("%w: got version %d after %d", ErrCorruptHistory, m.Version, j.version)
		}
		j.apply(e)
	}
	return j, nil
}

// IsOpen reports whether the job accepts applications.
func (j *Job) IsOpen() bool { return j.open }

// Version returns the version of the last applied event.
func (j *Job) Version() int64 { return j.version }

// ResultVersion returns the version of the event written by the command with
// the given idempotency key.
func (j *Job) ResultVersion(key string) (int64, bool) {
	p, ok := j.processed[key]
	return p.version, ok
}

// Open opens the job for applications.
func (j *Job) Open(cmd OpenJob, now time.Time) ([]Event, error) {
	title := strings.TrimSpace(cmd.Title)
	switch {
	case cmd.IdempotencyKey == "":
		return nil, fmt.Errorf("%w: idempotency key is required", ErrInvalidCommand)
	case cmd.JobID != j.id:
		return nil, fmt.Errorf("%w: command for %q sent to %q", ErrInvalidCommand, cmd.JobID, j.id)
	case cmd.CompanyID == "":
		return nil, fmt.Errorf("%w: company id is required", ErrInvalidCommand)
	case title == "":
		return nil, fmt.Errorf("%w: title is required", ErrInvalidCommand)
	}
	if seen, ok := j.processed[cmd.IdempotencyKey]; ok {
		if seen.fingerprint != openFingerprint(cmd.CompanyID, title) {
			return nil, fmt.Errorf("%w: %q", ErrIdempotencyKeyReused, cmd.IdempotencyKey)
		}
		return nil, nil
	}
	if j.open {
		return nil, ErrAlreadyOpened
	}
	e := Opened{
		Meta:      Meta{JobID: j.id, Version: j.version + 1, IdempotencyKey: cmd.IdempotencyKey, OccurredAt: now},
		CompanyID: cmd.CompanyID,
		Title:     title,
	}
	j.apply(e)
	return []Event{e}, nil
}

func (j *Job) apply(e Event) {
	var fingerprint string
	if e, ok := e.(Opened); ok {
		j.companyID, j.title, j.open = e.CompanyID, e.Title, true
		fingerprint = openFingerprint(e.CompanyID, e.Title)
	}
	m := e.Metadata()
	j.version = m.Version
	j.processed[m.IdempotencyKey] = processedCommand{fingerprint: fingerprint, version: m.Version}
}

func openFingerprint(companyID, title string) string {
	return "open\x00" + companyID + "\x00" + title
}
