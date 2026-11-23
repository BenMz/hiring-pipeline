package eventstore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	pipelinev1 "github.com/BenMz/hiring-pipeline/gen/go/pipeline/v1"
)

// EventTypeHeader carries the payload type (e.g. "StageChanged") so consumers
// can skip events they don't care about without decoding them.
const EventTypeHeader = "event-type"

// Kafka is a Store backed by a Kafka topic.
//
// Kafka cannot look up records by key, so the store tails the whole topic in
// the background and keeps an in-memory index of stream ID -> events. Load is
// read-your-writes: it fetches the partition's current end offset and waits
// until the index has caught up to it, so it sees every acknowledged append,
// including appends made by other processes.
//
// The expected-version check in Append is exact within one process (the
// command server also serialises commands per stream). Across processes there
// is a small window between the check and the write, so the service is meant
// to run as a single writer per partition. See docs/adr.
type Kafka struct {
	client     *kgo.Client
	admin      *kadm.Client
	topic      string
	partitions int32
	log        *slog.Logger
	done       chan struct{}

	mu       sync.Mutex
	progress chan struct{} // closed and replaced whenever the index advances
	next     map[int32]int64
	streams  map[string][]*pipelinev1.Event
	corrupt  map[string]error
}

// OpenKafka connects to the topic and blocks until the index holds every
// event that was in the topic at the time of the call.
func OpenKafka(ctx context.Context, brokers []string, topic string, log *slog.Logger) (*Kafka, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		// Producer: acks=all + idempotent writes (the franz-go default), so
		// internal retries never duplicate or reorder records.
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		// Consumer: read every partition from the start, no consumer group.
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	admin := kadm.NewClient(client)

	topics, err := admin.ListTopics(ctx, topic)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("describe topic %q: %w", topic, err)
	}
	detail, ok := topics[topic]
	if !ok || len(detail.Partitions) == 0 {
		detail.Err = errors.New("no partitions")
	}
	if detail.Err != nil {
		client.Close()
		return nil, fmt.Errorf("topic %q is not available: %w", topic, detail.Err)
	}

	k := &Kafka{
		client:     client,
		admin:      admin,
		topic:      topic,
		partitions: int32(len(detail.Partitions)),
		log:        log,
		done:       make(chan struct{}),
		progress:   make(chan struct{}),
		next:       map[int32]int64{},
		streams:    map[string][]*pipelinev1.Event{},
		corrupt:    map[string]error{},
	}
	go k.tail()

	ends, err := k.endOffsets(ctx)
	if err == nil {
		for p, end := range ends {
			if err = k.waitFor(ctx, p, end); err != nil {
				break
			}
		}
	}
	if err != nil {
		k.Close()
		return nil, fmt.Errorf("initial replay of %q: %w", topic, err)
	}
	return k, nil
}

// Close stops the background consumer and closes the client.
func (k *Kafka) Close() {
	k.client.Close()
	<-k.done
}

// Load implements Store.
func (k *Kafka) Load(ctx context.Context, streamID string) ([]*pipelinev1.Event, error) {
	p := partitionFor(streamID, k.partitions)
	ends, err := k.endOffsets(ctx)
	if err != nil {
		return nil, err
	}
	if err := k.waitFor(ctx, p, ends[p]); err != nil {
		return nil, err
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if err := k.corrupt[streamID]; err != nil {
		return nil, err
	}
	return slices.Clone(k.streams[streamID]), nil
}

// Append implements Store.
func (k *Kafka) Append(ctx context.Context, streamID string, expectedVersion int64, events ...*pipelinev1.Event) error {
	if len(events) == 0 {
		return nil
	}
	k.mu.Lock()
	current := int64(len(k.streams[streamID]))
	k.mu.Unlock()
	if current != expectedVersion {
		return fmt.Errorf("%w: %q is at version %d, expected %d", ErrConcurrencyConflict, streamID, current, expectedVersion)
	}

	p := partitionFor(streamID, k.partitions)
	records := make([]*kgo.Record, 0, len(events))
	for _, e := range events {
		value, err := proto.Marshal(e)
		if err != nil {
			return fmt.Errorf("marshal event: %w", err)
		}
		records = append(records, &kgo.Record{
			Key:       []byte(streamID),
			Value:     value,
			Partition: p,
			Headers:   []kgo.RecordHeader{{Key: EventTypeHeader, Value: []byte(eventType(e))}},
		})
	}
	// If ctx expires after the records were sent but before the ack, the
	// write may or may not have happened. The client retries with the same
	// idempotency key, and the aggregate turns a duplicate into a no-op.
	results := k.client.ProduceSync(ctx, records...)
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("produce to %s/%d: %w", k.topic, p, err)
	}
	// Wait until our own write is in the index, so the next command on this
	// stream sees it without another round trip.
	last := results[len(results)-1].Record.Offset
	return k.waitFor(ctx, p, last+1)
}

func (k *Kafka) tail() {
	defer close(k.done)
	for {
		fetches := k.client.PollFetches(context.Background())
		if fetches.IsClientClosed() {
			return
		}
		fetches.EachError(func(topic string, p int32, err error) {
			k.log.Error("fetch failed", "topic", topic, "partition", p, "err", err)
		})

		k.mu.Lock()
		advanced := false
		fetches.EachRecord(func(r *kgo.Record) {
			k.index(r)
			k.next[r.Partition] = r.Offset + 1
			advanced = true
		})
		if advanced {
			close(k.progress)
			k.progress = make(chan struct{})
		}
		k.mu.Unlock()
	}
}

// index adds a record to its stream. A record that cannot be decoded marks the
// stream as corrupt rather than being skipped, because deciding commands on a
// partial history could break the aggregate's rules.
func (k *Kafka) index(r *kgo.Record) {
	id := string(r.Key)
	e := &pipelinev1.Event{}
	if err := proto.Unmarshal(r.Value, e); err != nil {
		k.log.Error("undecodable event", "stream", id, "partition", r.Partition, "offset", r.Offset, "err", err)
		k.corrupt[id] = fmt.Errorf("stream %q: undecodable event at %d/%d: %w", id, r.Partition, r.Offset, err)
		return
	}
	k.streams[id] = append(k.streams[id], e)
}

func (k *Kafka) waitFor(ctx context.Context, p int32, offset int64) error {
	for {
		k.mu.Lock()
		reached, progress := k.next[p] >= offset, k.progress
		k.mu.Unlock()
		if reached {
			return nil
		}
		select {
		case <-progress:
		case <-k.done:
			return errors.New("event store closed")
		case <-ctx.Done():
			return fmt.Errorf("waiting for partition %d to reach offset %d: %w", p, offset, ctx.Err())
		}
	}
}

func (k *Kafka) endOffsets(ctx context.Context) (map[int32]int64, error) {
	listed, err := k.admin.ListEndOffsets(ctx, k.topic)
	if err != nil {
		return nil, fmt.Errorf("list end offsets: %w", err)
	}
	ends := make(map[int32]int64, k.partitions)
	for p := range k.partitions {
		o, ok := listed.Lookup(k.topic, p)
		if !ok {
			return nil, fmt.Errorf("end offset of %s/%d: partition missing from response", k.topic, p)
		}
		if o.Err != nil {
			return nil, fmt.Errorf("end offset of %s/%d: %w", k.topic, p, o.Err)
		}
		ends[p] = o.Offset
	}
	return ends, nil
}

func eventType(e *pipelinev1.Event) string {
	m := e.ProtoReflect()
	if f := m.WhichOneof(m.Descriptor().Oneofs().ByName("payload")); f != nil {
		return string(f.Message().Name())
	}
	return ""
}
