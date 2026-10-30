package processor

import (
	"aardappel/internal/hb_tracker"
	"aardappel/internal/tx_queue"
	"aardappel/internal/types"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newTestMessageIterator(capacity int) *MessageIterator {
	return NewMessageIterator(hb_tracker.TopicPartsCount{TopicPartsCountMap: map[int]hb_tracker.StreamCfg{0: {PartitionsCount: 1}}}, capacity)
}

func TestMessageIteratorLocalQuorumOrdering(t *testing.T) {
	it := NewMessageIterator(hb_tracker.TopicPartsCount{TopicPartsCountMap: map[int]hb_tracker.StreamCfg{
		0: {PartitionsCount: 2}, 1: {PartitionsCount: 1},
	}}, 10)
	ctx := context.Background()
	var consumed []string
	enqueue := func(reader uint32, label string, partition int64, step, txID uint64) {
		apply := func() error {
			consumed = append(consumed, label)
			return nil
		}
		if partition >= 0 {
			it.EnqueueHb(ctx, types.HbData{StreamId: types.ElementaryStreamId{ReaderId: reader, PartitionId: partition}, Step: step, TxId: txID}, apply)
		} else {
			it.Enqueue(ctx, reader, apply)
		}
	}
	enqueue(0, "a-data", -1, 0, 0)
	enqueue(0, "a-first-partition", 0, 10, 0)
	enqueue(0, "a-local-quorum", 1, 5, 0)
	enqueue(0, "a-advance", 1, 10, 0)
	enqueue(0, "a-tie-advance", 0, 10, 1)
	enqueue(0, "a-full-tie-advance", 1, 10, 1)
	enqueue(1, "b-first-quorum", 0, 7, 0)
	enqueue(1, "b-advance", 0, 10, 0)
	enqueue(1, "b-duplicate", 0, 10, 0)
	enqueue(1, "b-regression", 0, 9, 0)
	enqueue(1, "b-next", 0, 11, 0)
	for range 11 {
		apply := it.WaitNext(ctx, time.Second)
		require.NotNil(t, apply)
		require.NoError(t, apply())
	}
	require.Equal(t, []string{"a-data", "a-first-partition", "a-local-quorum", "b-first-quorum", "a-advance", "b-advance", "a-tie-advance", "a-full-tie-advance", "b-duplicate", "b-regression", "b-next"}, consumed)
	require.Nil(t, it.WaitNext(ctx, 0))
}

func TestMessageIteratorDoesNotSkipEmptySelectedReader(t *testing.T) {
	it := NewMessageIterator(hb_tracker.TopicPartsCount{TopicPartsCountMap: map[int]hb_tracker.StreamCfg{0: {PartitionsCount: 1}, 1: {PartitionsCount: 1}}}, 1)
	ctx := context.Background()
	it.Enqueue(ctx, 1, func() error { return nil })
	waitCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	apply := it.WaitNext(waitCtx, time.Second)
	require.Nil(t, apply)
	require.ErrorIs(t, waitCtx.Err(), context.DeadlineExceeded)
	it.EnqueueHb(ctx, types.HbData{StreamId: types.ElementaryStreamId{ReaderId: 0}, Step: 1}, func() error { return nil })
	apply = it.WaitNext(ctx, time.Second)
	require.NotNil(t, apply)
	require.NoError(t, apply())
	apply = it.WaitNext(ctx, time.Second)
	require.NotNil(t, apply)
	require.NoError(t, apply())
}

func TestMessageIteratorBackpressureCancellation(t *testing.T) {
	it := newTestMessageIterator(1)
	it.Enqueue(context.Background(), 0, func() error { return nil })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		it.Enqueue(ctx, 0, func() error { return nil })
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("blocked enqueue did not cancel")
	}
	apply := it.WaitNext(context.Background(), time.Second)
	require.NotNil(t, apply)
	require.NoError(t, apply())
	require.Nil(t, it.WaitNext(context.Background(), 0))
}

func TestProcessorConsumesLaggingReaderUntilLocalQuorumAdvances(t *testing.T) {
	layout := hb_tracker.TopicPartsCount{TotalPartsCount: 2, TopicPartsCountMap: map[int]hb_tracker.StreamCfg{0: {PartitionsCount: 1}, 1: {PartitionsCount: 1}}}
	p := &Processor{messages: NewMessageIterator(layout, 10), hbTracker: hb_tracker.NewHeartBeatTracker(layout), txQueue: tx_queue.NewTxQueue(), lastPosition: NewAtopmicPos()}
	p.lastPosition.Store(types.Position{})
	ctx := context.Background()
	hb := func(reader uint32, step uint64) {
		require.NoError(t, p.EnqueueHb(ctx, types.HbData{StreamId: types.ElementaryStreamId{ReaderId: reader}, Step: step, CommitTopic: func() error { return nil }}))
	}
	tx := func(reader uint32, step uint64) {
		require.NoError(t, p.EnqueueTx(ctx, types.TxData{TableId: reader, Step: step, CommitTopic: func() error { return nil }}))
	}
	hb(0, 100)
	tx(0, 150)
	hb(0, 200)
	hb(1, 10)
	tx(1, 20)
	for range 3 {
		require.NoError(t, p.doEvent(ctx))
	}
	txs := p.txQueue.PopTxsByCount(10)
	require.Len(t, txs, 1)
	p.txQueue.PushTx(txs[0])
	require.Equal(t, uint32(1), txs[0].TableId, "only the lagging reader's transaction was consumed")
	quorum, ok := p.hbTracker.GetQuorum()
	require.True(t, ok)
	require.Equal(t, uint64(10), quorum.Step)
	hb(1, 101)
	for range 3 {
		require.NoError(t, p.doEvent(ctx))
	}
	require.Len(t, p.txQueue.PopTxsByCount(10), 2)
	quorum, ok = p.hbTracker.GetQuorum()
	require.True(t, ok)
	require.Equal(t, uint64(101), quorum.Step)
}
