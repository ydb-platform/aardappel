package processor

import (
	"aardappel/internal/hb_tracker"
	"aardappel/internal/types"
	"context"
)

type messageQueue interface {
	Enqueue(context.Context, uint32, func() error)
	EnqueueHb(context.Context, types.HbData, func() error)
	Next() func() error
	Len() int
}

func newMessageQueue(layout hb_tracker.TopicPartsCount, capacity int, legacy bool) messageQueue {
	if legacy {
		return &sharedMessageQueue{messages: make(chan func() error, capacity)}
	}
	return NewMessageIterator(layout, capacity)
}

// sharedMessageQueue preserves the original shared channel and its backpressure.
type sharedMessageQueue struct {
	messages chan func() error
}

func (q *sharedMessageQueue) Enqueue(ctx context.Context, _ uint32, apply func() error) {
	select {
	case q.messages <- apply:
	case <-ctx.Done():
	}
}

func (q *sharedMessageQueue) EnqueueHb(ctx context.Context, hb types.HbData, apply func() error) {
	q.Enqueue(ctx, hb.StreamId.ReaderId, apply)
}

func (q *sharedMessageQueue) Next() func() error {
	select {
	case apply := <-q.messages:
		return apply
	default:
		return nil
	}
}

func (q *sharedMessageQueue) Len() int { return len(q.messages) }
