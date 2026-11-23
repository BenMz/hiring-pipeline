package application

// Commands are requests to change an application. Each carries an
// idempotency key chosen by the client; retrying with the same key is safe.

// SubmitApplication asks to create the application.
type SubmitApplication struct {
	IdempotencyKey string
	ApplicationID  string
	JobID          string
	CandidateName  string
	CandidateEmail string
}

// MoveStage asks to advance the application to the next stage.
type MoveStage struct {
	IdempotencyKey string
	ApplicationID  string
	To             Stage
}

// RejectCandidate asks to end the application with a rejection.
type RejectCandidate struct {
	IdempotencyKey string
	ApplicationID  string
	Reason         string
}
