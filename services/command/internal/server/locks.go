package server

import (
	"context"
	"sync"
)

// keyedMutex is a per-key lock whose Lock respects context cancellation.
type keyedMutex struct {
	mu    sync.Mutex
	locks map[string]chan struct{}
	users map[string]int
}

func (k *keyedMutex) lock(ctx context.Context, key string) (unlock func(), err error) {
	k.mu.Lock()
	if k.users == nil {
		k.users = map[string]int{}
	}
	ch, ok := k.locks[key]
	if !ok {
		ch = make(chan struct{}, 1)
		k.locks[key] = ch
	}
	k.users[key]++
	k.mu.Unlock()

	release := func() {
		k.mu.Lock()
		if k.users[key]--; k.users[key] == 0 {
			delete(k.locks, key)
			delete(k.users, key)
		}
		k.mu.Unlock()
	}

	select {
	case ch <- struct{}{}:
		return func() { <-ch; release() }, nil
	case <-ctx.Done():
		release()
		return nil, ctx.Err()
	}
}
