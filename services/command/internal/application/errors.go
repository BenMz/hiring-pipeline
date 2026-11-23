package application

import "errors"

// Errors returned by command methods; the gRPC layer maps them to status codes.
var (
	ErrInvalidCommand       = errors.New("invalid command")
	ErrNotSubmitted         = errors.New("application not submitted")
	ErrAlreadySubmitted     = errors.New("application already submitted")
	ErrInvalidTransition    = errors.New("invalid stage transition")
	ErrIdempotencyKeyReused = errors.New("idempotency key reused for a different command")
	ErrCorruptHistory       = errors.New("corrupt event history")
)
