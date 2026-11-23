package eventstore

import (
	"fmt"
	"testing"
)

func TestPartitionFor_IsStableAndSpreadsStreams(t *testing.T) {
	const partitions = 6
	seen := map[int32]int{}
	for i := range 600 {
		id := fmt.Sprintf("stream-%d", i)
		p := partitionFor(id, partitions)
		if p < 0 || p >= partitions {
			t.Fatalf("partitionFor(%q) = %d, out of range", id, p)
		}
		if again := partitionFor(id, partitions); again != p {
			t.Fatalf("partitionFor(%q) not stable: %d then %d", id, p, again)
		}
		seen[p]++
	}
	for p := range int32(partitions) {
		if seen[p] < 50 {
			t.Fatalf("partition %d got %d of 600 streams; distribution is skewed: %v", p, seen[p], seen)
		}
	}
}
