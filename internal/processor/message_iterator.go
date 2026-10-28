package processor

import (
	"aardappel/internal/hb_tracker"
	"aardappel/internal/types"
	"container/heap"
	"context"
	"time"
)

// MessageIterator orders consumption of bounded reader channels by local quorum.
// Enqueue and EnqueueHb may be called by producers concurrently. Other methods belong to
// the single processor goroutine. Channels stay open until context cancellation.
type MessageIterator struct {
	readers map[uint32]*messageReader
	queue   readerHeap
}

type queuedMessage struct {
	apply     func() error
	heartbeat *types.HbData
}

type messageReader struct {
	id         uint32
	messages   chan queuedMessage
	partitions int
	heartbeats map[int64]types.Position
	quorum     types.Position
	ready      bool
}

func NewMessageIterator(layout hb_tracker.TopicPartsCount, capacity int) *MessageIterator {
	it := &MessageIterator{readers: make(map[uint32]*messageReader)}
	for id, cfg := range layout.TopicPartsCountMap {
		reader := &messageReader{
			id:         uint32(id),
			messages:   make(chan queuedMessage, capacity),
			partitions: cfg.PartitionsCount,
			heartbeats: make(map[int64]types.Position),
		}
		it.readers[reader.id] = reader
		it.queue = append(it.queue, reader)
	}
	heap.Init(&it.queue)
	return it
}

func (it *MessageIterator) Enqueue(ctx context.Context, readerID uint32, apply func() error) {
	it.enqueue(ctx, readerID, queuedMessage{apply: apply})
}

func (it *MessageIterator) EnqueueHb(ctx context.Context, hb types.HbData, apply func() error) {
	it.enqueue(ctx, hb.StreamId.ReaderId, queuedMessage{apply: apply, heartbeat: &hb})
}

func (it *MessageIterator) enqueue(ctx context.Context, readerID uint32, message queuedMessage) {
	select {
	case it.readers[readerID].messages <- message:
	case <-ctx.Done():
	}
}

// WaitNext waits on the reader with the lowest local quorum.
// It returns nil on timeout or cancellation. Execute the returned event before
// requesting another one.
func (it *MessageIterator) WaitNext(ctx context.Context, timeout time.Duration) func() error {
	if ctx.Err() != nil || it == nil || len(it.queue) == 0 {
		return nil
	}
	messages := it.queue[0].messages
	var message queuedMessage

	// try now
	ok := false
	select {
	case message = <-messages:
		ok = true
	default:
	}

	if !ok {
		if timeout <= 0 {
			return nil
		}
		// try wait
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		select {
		case message = <-messages:
		case <-ctx.Done():
			return nil
		case <-timer.C:
			return nil
		}
	}

	if message.heartbeat != nil {
		it.advance(*message.heartbeat)
	}
	return message.apply
}

func (it *MessageIterator) advance(hb types.HbData) {
	reader := it.readers[hb.StreamId.ReaderId]
	pos := *types.NewPosition(hb)
	if old, ok := reader.heartbeats[hb.StreamId.PartitionId]; ok && !old.LessThan(pos) {
		return
	}
	reader.heartbeats[hb.StreamId.PartitionId] = pos
	if len(reader.heartbeats) != reader.partitions {
		return
	}
	quorum := pos
	for _, position := range reader.heartbeats {
		if position.LessThan(quorum) {
			quorum = position
		}
	}
	if reader.ready && !reader.quorum.LessThan(quorum) {
		return
	}
	reader.ready = true
	reader.quorum = quorum
	heap.Fix(&it.queue, 0)
}

type readerHeap []*messageReader

func (h readerHeap) Len() int { return len(h) }
func (h readerHeap) Less(i, j int) bool {
	a, b := h[i], h[j]
	if a.ready != b.ready {
		return !a.ready
	}
	if a.quorum != b.quorum {
		return a.quorum.LessThan(b.quorum)
	}
	// Keep equal-quorum readers in deterministic order.
	return a.id < b.id
}
func (h readerHeap) Swap(i, j int)           { h[i], h[j] = h[j], h[i] }
func (h *readerHeap) Push(value interface{}) { *h = append(*h, value.(*messageReader)) }
func (h *readerHeap) Pop() interface{} {
	last := len(*h) - 1
	value := (*h)[last]
	(*h)[last] = nil
	*h = (*h)[:last]
	return value
}
