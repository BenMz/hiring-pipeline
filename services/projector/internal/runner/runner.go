// Package runner feeds events from Kafka into the projection.
//
// Offsets are not committed to Kafka. They are stored in PostgreSQL in the
// same transaction as the read-model changes, so each batch is applied
// exactly once in effect, even across crashes. Resetting the projector is
// therefore a database operation: delete the offsets and it replays from 0.
package runner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	pipelinev1 "github.com/BenMz/hiring-pipeline/gen/go/pipeline/v1"
	"github.com/BenMz/hiring-pipeline/services/projector/internal/projection"
)

// Runner consumes the topic and keeps the read models up to date.
type Runner struct {
	pool   *pgxpool.Pool
	client *kgo.Client
	admin  *kadm.Client
	topic  string
	log    *slog.Logger

	mu       sync.Mutex
	progress chan struct{}
	next     map[int32]int64 // next offset per partition, as committed to PostgreSQL
	applied  int64           // events applied since start
}

// New positions a consumer at the offsets stored in PostgreSQL.
func New(ctx context.Context, pool *pgxpool.Pool, brokers []string, topic string, log *slog.Logger) (*Runner, error) {
	next, err := projection.LoadOffsets(ctx, pool, topic)
	if err != nil {
		return nil, err
	}
	probe, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, err
	}
	topics, err := kadm.NewClient(probe).ListTopics(ctx, topic)
	probe.Close()
	if err != nil {
		return nil, fmt.Errorf("describe topic %q: %w", topic, err)
	}
	detail := topics[topic]
	if detail.Err != nil || len(detail.Partitions) == 0 {
		return nil, fmt.Errorf("topic %q is not available: %w", topic, errors.Join(detail.Err, errors.New("no partitions")))
	}

	start := map[int32]kgo.Offset{}
	for p := range detail.Partitions {
		if o, ok := next[p]; ok {
			start[p] = kgo.NewOffset().At(o)
		} else {
			start[p] = kgo.NewOffset().AtStart()
			next[p] = 0
		}
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: start}),
	)
	if err != nil {
		return nil, err
	}
	return &Runner{
		pool: pool, client: client, admin: kadm.NewClient(client), topic: topic, log: log,
		progress: make(chan struct{}), next: next,
	}, nil
}

// Close releases the Kafka client.
func (r *Runner) Close() { r.client.Close() }

// Applied returns how many events were applied since the runner started.
func (r *Runner) Applied() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.applied
}

// Run projects until ctx is cancelled. A failed batch ends Run with an
// error: the consumer has already moved past it, so continuing would skip
// it. Restarting resumes from the offsets in PostgreSQL.
func (r *Runner) Run(ctx context.Context) error {
	for {
		fetches := r.client.PollFetches(ctx)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return nil
		}
		var fetchErr error
		fetches.EachError(func(topic string, p int32, err error) {
			fetchErr = errors.Join(fetchErr, fmt.Errorf("fetch %s/%d: %w", topic, p, err))
		})
		if fetchErr != nil {
			return fetchErr
		}
		if err := r.applyBatch(ctx, fetches); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
	}
}

func (r *Runner) applyBatch(ctx context.Context, fetches kgo.Fetches) error {
	events := make([]*pipelinev1.Event, 0, fetches.NumRecords())
	next := map[int32]int64{}
	fetches.EachRecord(func(rec *kgo.Record) {
		next[rec.Partition] = rec.Offset + 1
		e := &pipelinev1.Event{}
		if err := proto.Unmarshal(rec.Value, e); err != nil {
			// A read model can't do anything useful with it; the command side
			// refuses to work on such a stream, so it will be noticed there.
			r.log.Error("skipping undecodable event", "partition", rec.Partition, "offset", rec.Offset, "err", err)
			return
		}
		events = append(events, e)
	})
	if len(next) == 0 {
		return nil
	}
	err := pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		if err := projection.Apply(ctx, tx, events); err != nil {
			return err
		}
		return projection.SaveOffsets(ctx, tx, r.topic, next)
	})
	if err != nil {
		return err
	}

	r.mu.Lock()
	for p, o := range next {
		r.next[p] = o
	}
	r.applied += int64(len(events))
	close(r.progress)
	r.progress = make(chan struct{})
	r.mu.Unlock()
	return nil
}

// WaitCaughtUp blocks until every event that was in the topic when it was
// called has been projected and committed.
func (r *Runner) WaitCaughtUp(ctx context.Context) error {
	ends, err := r.admin.ListEndOffsets(ctx, r.topic)
	if err != nil {
		return fmt.Errorf("list end offsets: %w", err)
	}
	if err := ends.Error(); err != nil {
		return fmt.Errorf("list end offsets: %w", err)
	}
	for {
		r.mu.Lock()
		behind := false
		ends.Each(func(o kadm.ListedOffset) {
			if r.next[o.Partition] < o.Offset {
				behind = true
			}
		})
		progress := r.progress
		r.mu.Unlock()
		if !behind {
			return nil
		}
		select {
		case <-progress:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
