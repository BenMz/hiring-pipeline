// Package eventstore persists event streams. Each stream (one aggregate) is
// identified by its ID, which is also the Kafka record key.
package eventstore

import (
	"context"
	"errors"

	pipelinev1 "github.com/BenMz/hiring-pipeline/gen/go/pipeline/v1"
)

// ErrConcurrencyConflict means the stream changed between Load and Append.
// The command can safely be retried with the same idempotency key.
var ErrConcurrencyConflict = errors.New("concurrency conflict: stream was modified")

// Store loads and appends event streams.
type Store interface {
	// Load returns all events of a stream in order; an unknown stream is empty.
	// It reflects every append acknowledged before Load was called.
	Load(ctx context.Context, streamID string) ([]*pipelinev1.Event, error)
	// Append writes events to the end of a stream, but only if the stream
	// still has expectedVersion events.
	Append(ctx context.Context, streamID string, expectedVersion int64, events ...*pipelinev1.Event) error
}
