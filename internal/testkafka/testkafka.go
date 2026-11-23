//go:build integration

// Package testkafka starts a throwaway Redpanda broker for integration tests.
package testkafka

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Image is the same Redpanda version as docker-compose.yml.
const Image = "docker.redpanda.com/redpandadata/redpanda:v24.2.7"

// Partitions matches the topic created by docker-compose.yml.
const Partitions = 6

// Start launches a broker and returns its address and a stop function.
func Start(ctx context.Context) (broker string, stop func(), err error) {
	c, err := redpanda.Run(ctx, Image)
	if err != nil {
		return "", nil, fmt.Errorf("start redpanda: %w", err)
	}
	stop = func() { _ = testcontainers.TerminateContainer(c) }
	if broker, err = c.KafkaSeedBroker(ctx); err != nil {
		stop()
		return "", nil, err
	}
	return broker, stop, nil
}

var unsafe = regexp.MustCompile(`[^a-zA-Z0-9._-]+`)

// CreateTopic creates a fresh topic configured like the production event
// store (infinite retention, no compaction), so tests don't share state.
func CreateTopic(t *testing.T, broker string) string {
	t.Helper()
	name := strings.ToLower(unsafe.ReplaceAllString(t.Name(), "-")) + "-" + uuid.NewString()[:8]
	admin := kadm.NewClient(newClient(t, broker))
	retention, cleanup := "-1", "delete"
	res, err := admin.CreateTopic(t.Context(), Partitions, 1, map[string]*string{
		"retention.ms":   &retention,
		"cleanup.policy": &cleanup,
	}, name)
	if err == nil {
		err = res.Err
	}
	if err != nil {
		t.Fatalf("create topic %q: %v", name, err)
	}
	return name
}

// ReadAll returns every record currently in the topic.
func ReadAll(t *testing.T, broker, topic string) []*kgo.Record {
	t.Helper()
	ends, err := kadm.NewClient(newClient(t, broker)).ListEndOffsets(t.Context(), topic)
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	ends.Each(func(o kadm.ListedOffset) { total += o.Offset })

	consumer := newClient(t, broker, kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	var records []*kgo.Record
	for int64(len(records)) < total {
		fetches := consumer.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			t.Fatalf("read %d of %d records from %q: %v", len(records), total, topic, err)
		}
		records = append(records, fetches.Records()...)
	}
	return records
}

func newClient(t *testing.T, broker string, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(broker)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}
