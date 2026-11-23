// Package application holds the application aggregate: the consistency
// boundary for one candidate's journey through one job's hiring funnel.
//
// Command methods validate against the current state and return new events
// without performing I/O; the caller appends them to the event store. The
// aggregate applies the events to itself, so it stays usable afterwards.
package application

import (
	"fmt"
	"net/mail"
	"strings"
	"time"
)

// Application is the aggregate for one candidate applying to one job.
type Application struct {
	id             string
	jobID          string
	candidateName  string
	candidateEmail string
	stage          Stage
	version        int64
	// processed maps each idempotency key seen in the stream to the
	// fingerprint of the command it was used for.
	processed map[string]string
}

// New returns an application that has not been submitted yet.
func New(id string) *Application {
	return &Application{id: id, processed: map[string]string{}}
}

// Load rebuilds an application by replaying its events in order.
func Load(id string, history []Event) (*Application, error) {
	a := New(id)
	for _, e := range history {
		m := e.Metadata()
		if m.ApplicationID != id {
			return nil, fmt.Errorf("%w: event for %q in stream of %q", ErrCorruptHistory, m.ApplicationID, id)
		}
		if m.Version != a.version+1 {
			return nil, fmt.Errorf("%w: got version %d after %d", ErrCorruptHistory, m.Version, a.version)
		}
		a.apply(e)
	}
	return a, nil
}

// ID returns the application ID, which is also its stream key.
func (a *Application) ID() string { return a.id }

// JobID returns the job the application was submitted to.
func (a *Application) JobID() string { return a.jobID }

// Stage returns the current funnel stage; Unspecified before submission.
func (a *Application) Stage() Stage { return a.stage }

// Version returns the version of the last applied event; 0 for a new stream.
func (a *Application) Version() int64 { return a.version }

// Submit records a new application at the APPLIED stage.
func (a *Application) Submit(cmd SubmitApplication, now time.Time) ([]Event, error) {
	name := strings.TrimSpace(cmd.CandidateName)
	if err := a.validate(cmd.IdempotencyKey, cmd.ApplicationID); err != nil {
		return nil, err
	}
	if cmd.JobID == "" {
		return nil, fmt.Errorf("%w: job id is required", ErrInvalidCommand)
	}
	if name == "" {
		return nil, fmt.Errorf("%w: candidate name is required", ErrInvalidCommand)
	}
	if addr, err := mail.ParseAddress(cmd.CandidateEmail); err != nil || addr.Address != cmd.CandidateEmail {
		return nil, fmt.Errorf("%w: candidate email %q is not a plain address", ErrInvalidCommand, cmd.CandidateEmail)
	}
	if done, err := a.alreadyProcessed(cmd.IdempotencyKey, submitFingerprint(cmd.JobID, name, cmd.CandidateEmail)); done || err != nil {
		return nil, err
	}
	if a.stage != Unspecified {
		return nil, ErrAlreadySubmitted
	}
	return a.emit(ApplicationSubmitted{
		Meta:           a.meta(cmd.IdempotencyKey, now),
		JobID:          cmd.JobID,
		CandidateName:  name,
		CandidateEmail: cmd.CandidateEmail,
	}), nil
}

// MoveStage advances the application exactly one stage along the funnel.
func (a *Application) MoveStage(cmd MoveStage, now time.Time) ([]Event, error) {
	if err := a.validate(cmd.IdempotencyKey, cmd.ApplicationID); err != nil {
		return nil, err
	}
	if done, err := a.alreadyProcessed(cmd.IdempotencyKey, moveFingerprint(cmd.To)); done || err != nil {
		return nil, err
	}
	if a.stage == Unspecified {
		return nil, ErrNotSubmitted
	}
	if !a.stage.canMoveTo(cmd.To) {
		return nil, fmt.Errorf("%w: %v -> %v", ErrInvalidTransition, a.stage, cmd.To)
	}
	return a.emit(StageChanged{Meta: a.meta(cmd.IdempotencyKey, now), From: a.stage, To: cmd.To}), nil
}

// Reject ends the application from any open stage.
func (a *Application) Reject(cmd RejectCandidate, now time.Time) ([]Event, error) {
	reason := strings.TrimSpace(cmd.Reason)
	if err := a.validate(cmd.IdempotencyKey, cmd.ApplicationID); err != nil {
		return nil, err
	}
	if done, err := a.alreadyProcessed(cmd.IdempotencyKey, rejectFingerprint(reason)); done || err != nil {
		return nil, err
	}
	if a.stage == Unspecified {
		return nil, ErrNotSubmitted
	}
	if a.stage.Terminal() {
		return nil, fmt.Errorf("%w: cannot reject from %v", ErrInvalidTransition, a.stage)
	}
	return a.emit(CandidateRejected{Meta: a.meta(cmd.IdempotencyKey, now), From: a.stage, Reason: reason}), nil
}

func (a *Application) validate(key, applicationID string) error {
	if key == "" {
		return fmt.Errorf("%w: idempotency key is required", ErrInvalidCommand)
	}
	if applicationID != a.id {
		return fmt.Errorf("%w: command for %q sent to %q", ErrInvalidCommand, applicationID, a.id)
	}
	return nil
}

// alreadyProcessed reports whether key was already used for this exact
// command, in which case the command is a retry and must emit nothing.
func (a *Application) alreadyProcessed(key, fingerprint string) (bool, error) {
	seen, ok := a.processed[key]
	if !ok {
		return false, nil
	}
	if seen != fingerprint {
		return false, fmt.Errorf("%w: %q", ErrIdempotencyKeyReused, key)
	}
	return true, nil
}

func (a *Application) meta(key string, now time.Time) Meta {
	return Meta{ApplicationID: a.id, Version: a.version + 1, IdempotencyKey: key, OccurredAt: now}
}

func (a *Application) emit(e Event) []Event {
	a.apply(e)
	return []Event{e}
}

func (a *Application) apply(e Event) {
	switch e := e.(type) {
	case ApplicationSubmitted:
		a.jobID, a.candidateName, a.candidateEmail = e.JobID, e.CandidateName, e.CandidateEmail
		a.stage = Applied
	case StageChanged:
		a.stage = e.To
	case CandidateRejected:
		a.stage = Rejected
	}
	m := e.Metadata()
	a.version = m.Version
	a.processed[m.IdempotencyKey] = e.fingerprint()
}
