package processor

import (
	"aardappel/internal/hb_tracker"
	"aardappel/internal/tx_queue"
	"aardappel/internal/types"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMessageCommitModeDoesNotTrackOffsets(t *testing.T) {
	ctx := context.Background()
	stream := types.ElementaryStreamId{ReaderId: 0, PartitionId: 0}
	processor := Processor{
		commitOffsetMode: false,
		txChannel:        make(chan func() error, 3),
		txQueue:          tx_queue.NewTxQueue(),
		lastPosition:     NewAtopmicPos(),
		hbTracker: hb_tracker.NewHeartBeatTracker(hb_tracker.TopicPartsCount{
			TopicPartsCountMap: map[int]hb_tracker.StreamCfg{0: {PartitionsCount: 1}},
			TotalPartsCount:    1,
		}),
	}
	processor.lastPosition.Store(types.Position{})
	var txCommits, replacedHbCommits, currentHbCommits int
	require.NoError(t, processor.EnqueueTx(ctx, types.TxData{
		TableId:     stream.ReaderId,
		Step:        1,
		CommitTopic: func() error { txCommits++; return nil },
	}))
	require.NoError(t, processor.EnqueueHb(ctx, types.HbData{
		StreamId:    stream,
		Step:        2,
		CommitTopic: func() error { replacedHbCommits++; return nil },
	}))
	require.NoError(t, processor.EnqueueHb(ctx, types.HbData{
		StreamId:    stream,
		Step:        3,
		CommitTopic: func() error { currentHbCommits++; return nil },
	}))

	require.NoError(t, processor.doEvent(ctx))

	require.Nil(t, processor.offsetProgressByStream)
	require.Nil(t, processor.streamsWithOffsetsToCommit)
	require.Nil(t, processor.commitTasks)
	require.Equal(t, 1, replacedHbCommits)
	require.Zero(t, txCommits)
	require.Zero(t, currentHbCommits)
	require.Len(t, processor.txQueue.PopTxsByCount(1), 1)
	hb, ready := processor.hbTracker.GetQuorum()
	require.True(t, ready)
	require.Equal(t, uint64(3), hb.Step)
	require.Nil(t, hb.TopicOffset)
}
