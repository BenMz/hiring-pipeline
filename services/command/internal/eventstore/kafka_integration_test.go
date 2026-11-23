//go:build integration

package eventstore_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"

	pipelinev1 "github.com/BenMz/hiring-pipeline/gen/go/pipeline/v1"
	"github.com/BenMz/hiring-pipeline/internal/testkafka"
	"github.com/BenMz/hiring-pipeline/services/command/internal/eventstore"
)

var broker string

func TestMain(m *testing.M) {
	b, stop, err := testkafka.Start(context.Background())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	broker = b
	code := m.Run()
	stop()
	os.Exit(code)
}

func open(t *testing.T, topic string) *eventstore.Kafka {
	t.Helper()
	k, err := eventstore.OpenKafka(t.Context(), []string{broker}, topic, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("OpenKafka: %v", err)
	}
	t.Cleanup(k.Close)
	return k
}

func stageChanged(stream string, version int64) *pipelinev1.Event {
	return &pipelinev1.Event{
		Metadata: &pipelinev1.EventMetadata{AggregateId: stream, Version: version},
		Payload: &pipelinev1.Event_StageChanged{StageChanged: &pipelinev1.StageChanged{
			ApplicationId: stream, ToStage: pipelinev1.Stage(version),
		}},
	}
}

func versions(events []*pipelinev1.Event) []int64 {
	out := make([]int64, len(events))
	for i, e := range events {
		out[i] = e.GetMetadata().GetVersion()
	}
	return out
}

func TestKafka_AppendThenLoad_ReturnsStreamInOrder(t *testing.T) {
	k := open(t, testkafka.CreateTopic(t, broker))

	for v := int64(1); v <= 3; v++ {
		if err := k.Append(t.Context(), "app-1", v-1, stageChanged("app-1", v)); err != nil {
			t.Fatalf("Append v%d: %v", v, err)
		}
	}
	if err := k.Append(t.Context(), "app-2", 0, stageChanged("app-2", 1)); err != nil {
		t.Fatal(err)
	}

	events, err := k.Load(t.Context(), "app-1")
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(versions(events)); got != "[1 2 3]" {
		t.Fatalf("versions = %s, want [1 2 3]", got)
	}
}

func TestKafka_Append_WithStaleVersion_Conflicts(t *testing.T) {
	k := open(t, testkafka.CreateTopic(t, broker))
	if err := k.Append(t.Context(), "app-1", 0, stageChanged("app-1", 1)); err != nil {
		t.Fatal(err)
	}

	err := k.Append(t.Context(), "app-1", 0, stageChanged("app-1", 1))

	if !errors.Is(err, eventstore.ErrConcurrencyConflict) {
		t.Fatalf("err = %v, want ErrConcurrencyConflict", err)
	}
}

func TestKafka_Load_SeesAppendsFromAnotherInstance(t *testing.T) {
	topic := testkafka.CreateTopic(t, broker)
	writer, reader := open(t, topic), open(t, topic)

	if err := writer.Append(t.Context(), "app-1", 0, stageChanged("app-1", 1)); err != nil {
		t.Fatal(err)
	}
	// No sleep or polling: Load waits for the partition's end offset itself.
	events, err := reader.Load(t.Context(), "app-1")

	if err != nil || len(events) != 1 {
		t.Fatalf("reader.Load = (%d events, %v), want 1 event", len(events), err)
	}
}

func TestKafka_Open_ReplaysExistingTopic(t *testing.T) {
	topic := testkafka.CreateTopic(t, broker)
	first := open(t, topic)
	for v := int64(1); v <= 2; v++ {
		if err := first.Append(t.Context(), "app-1", v-1, stageChanged("app-1", v)); err != nil {
			t.Fatal(err)
		}
	}
	first.Close()

	restarted := open(t, topic)
	events, err := restarted.Load(t.Context(), "app-1")

	if err != nil || fmt.Sprint(versions(events)) != "[1 2]" {
		t.Fatalf("after restart: versions = %v, err = %v; want [1 2]", versions(events), err)
	}
}

func TestKafka_Records_AreKeyedByStreamWithTypeHeader(t *testing.T) {
	topic := testkafka.CreateTopic(t, broker)
	k := open(t, topic)
	for v := int64(1); v <= 3; v++ {
		if err := k.Append(t.Context(), "app-1", v-1, stageChanged("app-1", v)); err != nil {
			t.Fatal(err)
		}
	}

	records := testkafka.ReadAll(t, broker, topic)

	if len(records) != 3 {
		t.Fatalf("topic has %d records, want 3", len(records))
	}
	for _, r := range records {
		if string(r.Key) != "app-1" || r.Partition != records[0].Partition {
			t.Fatalf("record key=%q partition=%d; want key app-1, all on partition %d", r.Key, r.Partition, records[0].Partition)
		}
		if len(r.Headers) != 1 || r.Headers[0].Key != eventstore.EventTypeHeader || string(r.Headers[0].Value) != "StageChanged" {
			t.Fatalf("headers = %v, want event-type: StageChanged", r.Headers)
		}
	}
}

func TestKafka_Open_UnknownTopic_Fails(t *testing.T) {
	_, err := eventstore.OpenKafka(t.Context(), []string{broker}, "does-not-exist", slog.New(slog.NewTextHandler(io.Discard, nil)))

	if err == nil {
		t.Fatal("OpenKafka succeeded for a missing topic")
	}
}
