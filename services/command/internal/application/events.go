package application

import (
	"fmt"
	"time"
)

// Event is a fact that happened to an application. Events are the source of
// truth; aggregate state is only ever derived from them.
type Event interface {
	Metadata() Meta
	// fingerprint identifies the command that produced the event, so a retried
	// command can be recognised after the aggregate is rebuilt from the log.
	fingerprint() string
}

// Meta is the envelope data shared by all events.
type Meta struct {
	ApplicationID  string
	Version        int64
	IdempotencyKey string
	OccurredAt     time.Time
}

// Metadata returns the event envelope.
func (m Meta) Metadata() Meta { return m }

// ApplicationSubmitted starts an application's stream.
type ApplicationSubmitted struct { //nolint:revive // matches the event name in events.proto
	Meta
	JobID          string
	CandidateName  string
	CandidateEmail string
}

// StageChanged records a move along the funnel.
type StageChanged struct {
	Meta
	From Stage
	To   Stage
}

// CandidateRejected ends an application without a hire.
type CandidateRejected struct {
	Meta
	From   Stage
	Reason string
}

func (e ApplicationSubmitted) fingerprint() string {
	return submitFingerprint(e.JobID, e.CandidateName, e.CandidateEmail)
}
func (e StageChanged) fingerprint() string      { return moveFingerprint(e.To) }
func (e CandidateRejected) fingerprint() string { return rejectFingerprint(e.Reason) }

func submitFingerprint(jobID, name, email string) string {
	return fmt.Sprintf("submit\x00%s\x00%s\x00%s", jobID, name, email)
}
func moveFingerprint(to Stage) string        { return fmt.Sprintf("move\x00%d", to) }
func rejectFingerprint(reason string) string { return "reject\x00" + reason }
