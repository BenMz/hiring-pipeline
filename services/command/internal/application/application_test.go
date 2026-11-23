package application_test

import (
	"errors"
	"testing"
	"time"

	"github.com/BenMz/hiring-pipeline/services/command/internal/application"
)

var now = time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)

const appID = "app-1"

func submitCmd(key string) application.SubmitApplication {
	return application.SubmitApplication{
		IdempotencyKey: key,
		ApplicationID:  appID,
		JobID:          "job-1",
		CandidateName:  "Ada Lovelace",
		CandidateEmail: "ada@example.com",
	}
}

// given rebuilds an aggregate from a history of events, like the command
// service does after reading the stream from Kafka.
func given(t *testing.T, history ...application.Event) *application.Application {
	t.Helper()
	a, err := application.Load(appID, history)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return a
}

func submitted(version int64) application.Event {
	return application.ApplicationSubmitted{
		Meta:           application.Meta{ApplicationID: appID, Version: version, IdempotencyKey: "k-submit", OccurredAt: now},
		JobID:          "job-1",
		CandidateName:  "Ada Lovelace",
		CandidateEmail: "ada@example.com",
	}
}

func moved(version int64, key string, from, to application.Stage) application.Event {
	return application.StageChanged{
		Meta: application.Meta{ApplicationID: appID, Version: version, IdempotencyKey: key, OccurredAt: now},
		From: from,
		To:   to,
	}
}

// atStage returns a history that leads an application to the given stage.
func atStage(stage application.Stage) []application.Event {
	path := []application.Stage{application.Applied, application.Screening, application.Interview, application.Offer, application.Hired}
	history := []application.Event{submitted(1)}
	if stage == application.Rejected {
		return append(history, application.CandidateRejected{
			Meta: application.Meta{ApplicationID: appID, Version: 2, IdempotencyKey: "k-reject", OccurredAt: now},
			From: application.Applied,
		})
	}
	for i := 1; i < len(path) && path[i-1] != stage; i++ {
		history = append(history, moved(int64(i+1), "k-move-"+path[i].String(), path[i-1], path[i]))
	}
	return history
}

func TestSubmit_NewApplication_EmitsApplicationSubmitted(t *testing.T) {
	a := application.New(appID)

	events, err := a.Submit(submitCmd("k1"), now)
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	want := application.ApplicationSubmitted{
		Meta:           application.Meta{ApplicationID: appID, Version: 1, IdempotencyKey: "k1", OccurredAt: now},
		JobID:          "job-1",
		CandidateName:  "Ada Lovelace",
		CandidateEmail: "ada@example.com",
	}
	if len(events) != 1 || events[0] != want {
		t.Fatalf("events = %#v, want [%#v]", events, want)
	}
	if a.Stage() != application.Applied || a.Version() != 1 {
		t.Fatalf("state = (%v, v%d), want (APPLIED, v1)", a.Stage(), a.Version())
	}
}

func TestSubmit_InvalidCommand(t *testing.T) {
	tests := map[string]func(*application.SubmitApplication){
		"missing idempotency key": func(c *application.SubmitApplication) { c.IdempotencyKey = "" },
		"missing job":             func(c *application.SubmitApplication) { c.JobID = "" },
		"missing name":            func(c *application.SubmitApplication) { c.CandidateName = "  " },
		"invalid email":           func(c *application.SubmitApplication) { c.CandidateEmail = "not-an-email" },
		"email with display name": func(c *application.SubmitApplication) { c.CandidateEmail = "Ada <ada@example.com>" },
		"wrong application id":    func(c *application.SubmitApplication) { c.ApplicationID = "other" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			cmd := submitCmd("k1")
			mutate(&cmd)

			events, err := application.New(appID).Submit(cmd, now)

			if !errors.Is(err, application.ErrInvalidCommand) {
				t.Fatalf("err = %v, want ErrInvalidCommand", err)
			}
			if len(events) != 0 {
				t.Fatalf("events = %v, want none", events)
			}
		})
	}
}

func TestSubmit_AlreadySubmitted_WithNewKey_Fails(t *testing.T) {
	a := given(t, submitted(1))

	_, err := a.Submit(submitCmd("k-other"), now)

	if !errors.Is(err, application.ErrAlreadySubmitted) {
		t.Fatalf("err = %v, want ErrAlreadySubmitted", err)
	}
}

func TestMoveStage_AllowedTransitions(t *testing.T) {
	tests := []struct{ from, to application.Stage }{
		{application.Applied, application.Screening},
		{application.Screening, application.Interview},
		{application.Interview, application.Offer},
		{application.Offer, application.Hired},
	}
	for _, tt := range tests {
		t.Run(tt.from.String()+"->"+tt.to.String(), func(t *testing.T) {
			a := given(t, atStage(tt.from)...)
			before := a.Version()

			events, err := a.MoveStage(application.MoveStage{IdempotencyKey: "k-new", ApplicationID: appID, To: tt.to}, now)
			if err != nil {
				t.Fatalf("MoveStage: %v", err)
			}

			want := moved(before+1, "k-new", tt.from, tt.to)
			if len(events) != 1 || events[0] != want {
				t.Fatalf("events = %#v, want [%#v]", events, want)
			}
			if a.Stage() != tt.to {
				t.Fatalf("stage = %v, want %v", a.Stage(), tt.to)
			}
		})
	}
}

func TestMoveStage_InvalidTransitions(t *testing.T) {
	tests := []struct{ from, to application.Stage }{
		{application.Applied, application.Interview},   // skipping a stage
		{application.Interview, application.Screening}, // moving backwards
		{application.Screening, application.Screening}, // no-op move
		{application.Offer, application.Rejected},      // rejection has its own command
		{application.Applied, application.Unspecified}, // not a real stage
		{application.Hired, application.Offer},         // hired is terminal
		{application.Rejected, application.Screening},  // rejected is terminal
		{application.Applied, application.Stage(42)},   // unknown value
	}
	for _, tt := range tests {
		t.Run(tt.from.String()+"->"+tt.to.String(), func(t *testing.T) {
			a := given(t, atStage(tt.from)...)
			before := a.Version()

			events, err := a.MoveStage(application.MoveStage{IdempotencyKey: "k-new", ApplicationID: appID, To: tt.to}, now)

			if !errors.Is(err, application.ErrInvalidTransition) {
				t.Fatalf("err = %v, want ErrInvalidTransition", err)
			}
			if len(events) != 0 || a.Stage() != tt.from || a.Version() != before {
				t.Fatalf("aggregate changed on rejected command: events=%v stage=%v", events, a.Stage())
			}
		})
	}
}

func TestMoveStage_NotSubmitted_Fails(t *testing.T) {
	_, err := application.New(appID).MoveStage(application.MoveStage{IdempotencyKey: "k1", ApplicationID: appID, To: application.Screening}, now)

	if !errors.Is(err, application.ErrNotSubmitted) {
		t.Fatalf("err = %v, want ErrNotSubmitted", err)
	}
}

func TestReject_FromAnyOpenStage(t *testing.T) {
	for _, from := range []application.Stage{application.Applied, application.Screening, application.Interview, application.Offer} {
		t.Run(from.String(), func(t *testing.T) {
			a := given(t, atStage(from)...)
			before := a.Version()

			events, err := a.Reject(application.RejectCandidate{IdempotencyKey: "k-rej", ApplicationID: appID, Reason: "position filled"}, now)
			if err != nil {
				t.Fatalf("Reject: %v", err)
			}

			want := application.CandidateRejected{
				Meta:   application.Meta{ApplicationID: appID, Version: before + 1, IdempotencyKey: "k-rej", OccurredAt: now},
				From:   from,
				Reason: "position filled",
			}
			if len(events) != 1 || events[0] != want {
				t.Fatalf("events = %#v, want [%#v]", events, want)
			}
			if a.Stage() != application.Rejected {
				t.Fatalf("stage = %v, want REJECTED", a.Stage())
			}
		})
	}
}

func TestReject_FromTerminalStage_Fails(t *testing.T) {
	for _, from := range []application.Stage{application.Hired, application.Rejected} {
		t.Run(from.String(), func(t *testing.T) {
			a := given(t, atStage(from)...)

			_, err := a.Reject(application.RejectCandidate{IdempotencyKey: "k-new", ApplicationID: appID}, now)

			if !errors.Is(err, application.ErrInvalidTransition) {
				t.Fatalf("err = %v, want ErrInvalidTransition", err)
			}
		})
	}
}

func TestIdempotency_RetriedCommandEmitsNoSecondEvent(t *testing.T) {
	t.Run("submit", func(t *testing.T) {
		a := application.New(appID)
		if _, err := a.Submit(submitCmd("k1"), now); err != nil {
			t.Fatal(err)
		}

		events, err := a.Submit(submitCmd("k1"), now.Add(time.Second))

		if err != nil || len(events) != 0 {
			t.Fatalf("retry = (%v, %v), want (no events, nil)", events, err)
		}
		if a.Version() != 1 {
			t.Fatalf("version = %d, want 1", a.Version())
		}
	})

	t.Run("move stage", func(t *testing.T) {
		a := given(t, submitted(1))
		cmd := application.MoveStage{IdempotencyKey: "k-move", ApplicationID: appID, To: application.Screening}
		if _, err := a.MoveStage(cmd, now); err != nil {
			t.Fatal(err)
		}

		// Without idempotency this retry would fail as SCREENING -> SCREENING;
		// with it, the client sees the same success as the first attempt.
		events, err := a.MoveStage(cmd, now)

		if err != nil || len(events) != 0 {
			t.Fatalf("retry = (%v, %v), want (no events, nil)", events, err)
		}
	})

	t.Run("reject", func(t *testing.T) {
		a := given(t, submitted(1))
		cmd := application.RejectCandidate{IdempotencyKey: "k-rej", ApplicationID: appID, Reason: "no visa"}
		if _, err := a.Reject(cmd, now); err != nil {
			t.Fatal(err)
		}

		events, err := a.Reject(cmd, now)

		if err != nil || len(events) != 0 {
			t.Fatalf("retry = (%v, %v), want (no events, nil)", events, err)
		}
	})
}

func TestIdempotency_SurvivesReplay(t *testing.T) {
	// The key was processed by a previous process; this one only has the log.
	a := given(t, submitted(1), moved(2, "k-move", application.Applied, application.Screening))

	events, err := a.MoveStage(application.MoveStage{IdempotencyKey: "k-move", ApplicationID: appID, To: application.Screening}, now)

	if err != nil || len(events) != 0 {
		t.Fatalf("retry after replay = (%v, %v), want (no events, nil)", events, err)
	}
}

func TestIdempotency_KeyReusedForDifferentCommand_Fails(t *testing.T) {
	a := given(t, submitted(1), moved(2, "k-move", application.Applied, application.Screening))

	tests := map[string]func() ([]application.Event, error){
		"different target stage": func() ([]application.Event, error) {
			return a.MoveStage(application.MoveStage{IdempotencyKey: "k-move", ApplicationID: appID, To: application.Interview}, now)
		},
		"different command type": func() ([]application.Event, error) {
			return a.Reject(application.RejectCandidate{IdempotencyKey: "k-move", ApplicationID: appID}, now)
		},
		"submit key reused with other payload": func() ([]application.Event, error) {
			cmd := submitCmd("k-submit")
			cmd.CandidateEmail = "someone@else.com"
			return a.Submit(cmd, now)
		},
	}
	for name, run := range tests {
		t.Run(name, func(t *testing.T) {
			events, err := run()

			if !errors.Is(err, application.ErrIdempotencyKeyReused) {
				t.Fatalf("err = %v, want ErrIdempotencyKeyReused", err)
			}
			if len(events) != 0 {
				t.Fatalf("events = %v, want none", events)
			}
		})
	}
}

func TestLoad_RebuildsStateFromHistory(t *testing.T) {
	a := given(t, atStage(application.Offer)...)

	if a.Stage() != application.Offer || a.Version() != 4 || a.JobID() != "job-1" {
		t.Fatalf("state = (%v, v%d, %q), want (OFFER, v4, job-1)", a.Stage(), a.Version(), a.JobID())
	}
}

func TestLoad_RejectsCorruptHistory(t *testing.T) {
	tests := map[string][]application.Event{
		"version gap":        {submitted(1), moved(3, "k", application.Applied, application.Screening)},
		"not starting at v1": {submitted(2)},
		"foreign aggregate": {application.ApplicationSubmitted{
			Meta: application.Meta{ApplicationID: "someone-else", Version: 1},
		}},
	}
	for name, history := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := application.Load(appID, history); !errors.Is(err, application.ErrCorruptHistory) {
				t.Fatalf("err = %v, want ErrCorruptHistory", err)
			}
		})
	}
}
