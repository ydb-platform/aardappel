package processor

import (
	"aardappel/internal/hb_tracker"
	"aardappel/internal/types"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWaitNext(t *testing.T) {
	layout := hb_tracker.TopicPartsCount{TotalPartsCount: 1, TopicPartsCountMap: map[int]hb_tracker.StreamCfg{0: {PartitionsCount: 1}}}
	for _, legacy := range []bool{false, true} {
		name := "iterator"
		if legacy {
			name = "legacy"
		}
		t.Run(name, func(t *testing.T) {
			q := newMessageQueue(layout, 2, legacy)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			received := make(chan func() error, 1)
			go func() { received <- q.WaitNext(ctx, time.Second) }()
			called := false
			q.Enqueue(ctx, 0, func() error { called = true; return nil })
			select {
			case apply := <-received:
				require.NotNil(t, apply)
				require.False(t, called, "dequeue must not execute the callback")
				require.NoError(t, apply())
				require.True(t, called)
			case <-ctx.Done():
				t.Fatal("WaitNext did not wake for an arriving event")
			}
			go func() { received <- q.WaitNext(ctx, time.Second) }()
			cancel()
			select {
			case apply := <-received:
				require.Nil(t, apply)
			case <-time.After(time.Second):
				t.Fatal("WaitNext did not wake on cancellation")
			}
		})
	}
}

func TestWaitNextPreservesReaderScheduling(t *testing.T) {
	layout := hb_tracker.TopicPartsCount{TotalPartsCount: 2, TopicPartsCountMap: map[int]hb_tracker.StreamCfg{0: {PartitionsCount: 1}, 1: {PartitionsCount: 1}}}
	it := NewMessageIterator(layout, 2)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var consumed []uint32
	// Reader 1 already has input, but reader 0 must be read first.
	it.Enqueue(ctx, 1, func() error { consumed = append(consumed, 1); return nil })
	received := make(chan func() error, 1)
	go func() { received <- it.WaitNext(ctx, time.Second) }()
	it.EnqueueHb(ctx, types.HbData{StreamId: types.ElementaryStreamId{ReaderId: 0}, Step: 10}, func() error {
		consumed = append(consumed, 0)
		return nil
	})
	select {
	case apply := <-received:
		require.NotNil(t, apply)
		require.NoError(t, apply())
	case <-ctx.Done():
		t.Fatal("WaitNext did not wake for the selected reader")
	}
	require.Equal(t, []uint32{0}, consumed)
	// Dequeuing the heartbeat advances scheduling to reader 1.
	apply := it.WaitNext(ctx, time.Second)
	require.NotNil(t, apply)
	require.NoError(t, apply())
	require.Equal(t, []uint32{0, 1}, consumed)
}

func TestWaitNextBufferedReadDoesNotAllocate(t *testing.T) {
	layout := hb_tracker.TopicPartsCount{TotalPartsCount: 1, TopicPartsCountMap: map[int]hb_tracker.StreamCfg{0: {PartitionsCount: 1}}}
	for _, legacy := range []bool{false, true} {
		q := newMessageQueue(layout, 1, legacy)
		ctx := context.Background()
		event := func() error { return nil }
		allocations := testing.AllocsPerRun(100, func() {
			q.Enqueue(ctx, 0, event)
			if q.WaitNext(ctx, time.Second) == nil {
				t.Fatal("buffered event was not returned")
			}
		})
		require.Zero(t, allocations, "legacy=%t", legacy)
	}
}

func TestWaitNextReturnsNilOnTimeout(t *testing.T) {
	layout := hb_tracker.TopicPartsCount{TotalPartsCount: 1, TopicPartsCountMap: map[int]hb_tracker.StreamCfg{0: {PartitionsCount: 1}}}
	for _, legacy := range []bool{false, true} {
		q := newMessageQueue(layout, 1, legacy)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		start := time.Now()
		require.Nil(t, q.WaitNext(ctx, 20*time.Millisecond))
		require.GreaterOrEqual(t, time.Since(start), 20*time.Millisecond)
		require.NoError(t, ctx.Err(), "queue timeout must not cancel the caller")
		cancel()
	}
}
