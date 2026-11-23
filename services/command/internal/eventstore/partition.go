package eventstore

import "hash/fnv"

// partitionFor maps a stream to a partition. All events of one stream go to
// the same partition, which gives them a total order. Changing the partition
// count of the topic would remap streams, so it is fixed at creation.
func partitionFor(streamID string, partitions int32) int32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(streamID))
	return int32(h.Sum32() % uint32(partitions))
}
