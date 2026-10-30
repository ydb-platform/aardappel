package processor

import (
	"aardappel/internal/hb_tracker"
	"aardappel/internal/types"
	"context"
	"time"
)

type messageQueue interface {
	Enqueue(context.Context, uint32, func() error)
	EnqueueHb(context.Context, types.HbData, func() error)
	WaitNext(context.Context, time.Duration) func() error
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

// WaitNext returns nil on timeout or cancellation. Buffered events need no timer.
func (q *sharedMessageQueue) WaitNext(ctx context.Context, timeout time.Duration) func() error {
	if ctx.Err() != nil {
		return nil
	}
	select {
	case apply := <-q.messages:
		return apply
	default:
	}
	if timeout <= 0 {
		return nil
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case apply := <-q.messages:
		return apply
	case <-ctx.Done():
		return nil
	case <-timer.C:
		return nil
	}
}
