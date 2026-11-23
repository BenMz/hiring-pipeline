package eventstore

import (
	"context"
	"fmt"
	"slices"
	"sync"

	pipelinev1 "github.com/BenMz/hiring-pipeline/gen/go/pipeline/v1"
)

// Memory is an in-process Store for unit tests.
type Memory struct {
	mu      sync.Mutex
	streams map[string][]*pipelinev1.Event
}

// NewMemory returns an empty in-memory store.
func NewMemory() *Memory {
	return &Memory{streams: map[string][]*pipelinev1.Event{}}
}

// Load implements Store.
func (m *Memory) Load(ctx context.Context, streamID string) ([]*pipelinev1.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.streams[streamID]), nil
}

// Append implements Store.
func (m *Memory) Append(ctx context.Context, streamID string, expectedVersion int64, events ...*pipelinev1.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if current := int64(len(m.streams[streamID])); current != expectedVersion {
		return fmt.Errorf("%w: %q is at version %d, expected %d", ErrConcurrencyConflict, streamID, current, expectedVersion)
	}
	m.streams[streamID] = append(m.streams[streamID], events...)
	return nil
}
