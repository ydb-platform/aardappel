package processor

import (
	"aardappel/internal/hb_tracker"
	"aardappel/internal/tx_queue"
	"aardappel/internal/types"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

type offsetCommitterFunc func(context.Context, int64, int64) error

func (f offsetCommitterFunc) CommitOffset(ctx context.Context, partitionID int64, offset int64) error {
	return f(ctx, partitionID, offset)
}

func TestMarkOffsetProcessedWaitsForContiguousRange(t *testing.T) {
	processor := Processor{commitOffsetMode: true, offsetProgressByStream: make(map[types.ElementaryStreamId]*partitionOffsetProgress)}
	readerID := uint32(7)
	partitionID := int64(3)
	key := types.ElementaryStreamId{ReaderId: readerID, PartitionId: partitionID}
	processor.markOffsetProcessed(readerID, types.TopicOffset{
		PartitionId: partitionID,
		StartOffset: 10,
		Offset:      10,
	})
	progress := processor.offsetProgressByStream[key]
	if progress.readyToCommitOffset != 11 {
		t.Fatalf("ready-to-commit offset after first message: got %d, want 11", progress.readyToCommitOffset)
	}

	processor.markOffsetProcessed(readerID, types.TopicOffset{
		PartitionId: partitionID,
		StartOffset: 12,
		Offset:      12,
	})
	if progress.readyToCommitOffset != 11 {
		t.Fatalf("ready-to-commit offset crossed a gap: got %d, want 11", progress.readyToCommitOffset)
	}

	processor.markOffsetProcessed(readerID, types.TopicOffset{
		PartitionId: partitionID,
		StartOffset: 10,
		Offset:      10,
	})
	if progress.readyToCommitOffset != 11 {
		t.Fatalf("duplicate changed ready-to-commit offset: got %d, want 11", progress.readyToCommitOffset)
	}

	processor.markOffsetProcessed(readerID, types.TopicOffset{
		PartitionId: partitionID,
		StartOffset: 11,
		Offset:      11,
	})
	if progress.readyToCommitOffset != 13 {
		t.Fatalf("ready-to-commit offset after filling gap: got %d, want 13", progress.readyToCommitOffset)
	}
	if len(progress.pendingOffsets) != 0 {
		t.Fatalf("processed offsets left pending: %d", len(progress.pendingOffsets))
	}
}

func TestQueueReadyOffsetsDoesNotQueueSameOffsetTwice(t *testing.T) {
	ctx := context.Background()
	streamID := types.ElementaryStreamId{ReaderId: 1, PartitionId: 2}
	progress := &partitionOffsetProgress{
		readyToCommitOffset:  5,
		queuedToCommitOffset: 4,
	}
	processor := Processor{
		commitOffsetMode:           true,
		offsetProgressByStream:     map[types.ElementaryStreamId]*partitionOffsetProgress{streamID: progress},
		streamsWithOffsetsToCommit: map[types.ElementaryStreamId]struct{}{streamID: {}},
		commitTasks:                make(chan offsetCommitTask, 2),
	}

	processor.scheduleOffsetCommits(ctx)
	select {
	case task := <-processor.commitTasks:
		if task.offset != 5 {
			t.Fatalf("queued offset: got %d, want 5", task.offset)
		}
	case <-time.After(time.Second):
		t.Fatal("ready offset was not queued")
	}
	processor.scheduleOffsetCommits(ctx)
	select {
	case task := <-processor.commitTasks:
		t.Fatalf("same offset queued twice: %d", task.offset)
	default:
	}
	if progress.queuedToCommitOffset != 5 {
		t.Fatalf("recorded queued-to-commit offset: got %d, want 5", progress.queuedToCommitOffset)
	}
}

func TestOffsetCommitterCoalescesTasksPerPartition(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tasks := make(chan offsetCommitTask)
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	committed := make(chan int64, 3)
	stream := types.ElementaryStreamId{ReaderId: 1, PartitionId: 2}

	committer := offsetCommitterFunc(func(_ context.Context, _ int64, offset int64) error {
		if offset == 1 {
			close(firstStarted)
			<-releaseFirst
		}
		committed <- offset
		return nil
	})
	go runOffsetCommitter(ctx, tasks, []offsetCommitter{nil, committer}, 10)
	tasks <- offsetCommitTask{
		stream: stream,
		offset: 1,
	}
	<-firstStarted
	for _, offset := range []int64{2, 3} {
		offset := offset
		tasks <- offsetCommitTask{
			stream: stream,
			offset: offset,
		}
	}
	close(releaseFirst)

	for _, want := range []int64{1, 3} {
		select {
		case got := <-committed:
			if got != want {
				t.Fatalf("committed offset: got %d, want %d", got, want)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("offset %d was not committed", want)
		}
	}
}

func TestOffsetCommitterRunsDifferentPartitionsInParallel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tasks := make(chan offsetCommitTask)
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondStarted := make(chan struct{})

	committer := offsetCommitterFunc(func(ctx context.Context, partitionID int64, _ int64) error {
		if partitionID == 2 {
			close(secondStarted)
			return nil
		}
		close(firstStarted)
		select {
		case <-releaseFirst:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	go runOffsetCommitter(ctx, tasks, []offsetCommitter{nil, committer}, 10)
	tasks <- offsetCommitTask{
		stream: types.ElementaryStreamId{ReaderId: 1, PartitionId: 1},
		offset: 1,
	}
	<-firstStarted
	tasks <- offsetCommitTask{
		stream: types.ElementaryStreamId{ReaderId: 1, PartitionId: 2},
		offset: 1,
	}

	select {
	case <-secondStarted:
		close(releaseFirst)
	case <-time.After(3 * time.Second):
		t.Fatal("second partition waited for the first partition commit")
	}
}

func TestOffsetCommitterRetriesAfterError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tasks := make(chan offsetCommitTask, 1)
	committed := make(chan struct{}, 1)
	wantErr := errors.New("commit failed")
	commitCalls := 0

	committer := offsetCommitterFunc(func(context.Context, int64, int64) error {
		commitCalls++
		if commitCalls == 1 {
			return wantErr
		}
		committed <- struct{}{}
		return nil
	})
	go runOffsetCommitter(ctx, tasks, []offsetCommitter{nil, committer}, 1)
	tasks <- offsetCommitTask{
		stream: types.ElementaryStreamId{ReaderId: 1, PartitionId: 2},
		offset: 3,
	}

	select {
	case <-committed:
	case <-time.After(3 * time.Second):
		t.Fatal("offset was not committed after retry")
	}
}

func TestEnqueueOldTxMarksOffsetWithoutQueueingTx(t *testing.T) {
	ctx := context.Background()
	streamID := types.ElementaryStreamId{ReaderId: 0, PartitionId: 0}
	processor := Processor{
		commitOffsetMode:       true,
		txChannel:              make(chan func() error, 1),
		txQueue:                tx_queue.NewTxQueue(),
		lastPosition:           NewAtopmicPos(),
		offsetProgressByStream: make(map[types.ElementaryStreamId]*partitionOffsetProgress),
		commitTasks:            make(chan offsetCommitTask, 1),
	}
	processor.lastPosition.Store(types.Position{Step: 2})
	_ = processor.EnqueueTx(ctx, types.TxData{
		Step:    1,
		TableId: streamID.ReaderId,
		TopicOffset: &types.TopicOffset{
			PartitionId: streamID.PartitionId,
			StartOffset: 0,
			Offset:      0,
		},
	})
	if err := processor.doEvent(ctx); err != nil {
		t.Fatal(err)
	}

	if txs := processor.txQueue.PopTxsByCount(1); len(txs) != 0 {
		t.Fatal("old transaction was queued for destination DB")
	}
	if progress := processor.offsetProgressByStream[streamID]; progress.readyToCommitOffset != 1 {
		t.Fatalf("ready-to-commit offset: got %d, want 1", progress.readyToCommitOffset)
	}
	select {
	case task := <-processor.commitTasks:
		if task.offset != 1 {
			t.Fatalf("queued offset: got %d, want 1", task.offset)
		}
	case <-time.After(time.Second):
		t.Fatal("old transaction offset was not queued for commit")
	}
}

func TestEnqueueOldHbMarksOffsetWithoutTrackingHeartbeat(t *testing.T) {
	ctx := context.Background()
	streamID := types.ElementaryStreamId{ReaderId: 0, PartitionId: 0}
	processor := Processor{
		commitOffsetMode: true,
		txChannel:        make(chan func() error, 1),
		hbTracker: hb_tracker.NewHeartBeatTracker(hb_tracker.TopicPartsCount{
			TopicPartsCountMap: map[int]hb_tracker.StreamCfg{0: {PartitionsCount: 1}},
			TotalPartsCount:    1,
		}),
		lastPosition:           NewAtopmicPos(),
		offsetProgressByStream: make(map[types.ElementaryStreamId]*partitionOffsetProgress),
		commitTasks:            make(chan offsetCommitTask, 1),
	}
	processor.lastPosition.Store(types.Position{Step: 2})
	_ = processor.EnqueueHb(ctx, types.HbData{
		StreamId: streamID,
		Step:     1,
		TopicOffset: &types.TopicOffset{
			PartitionId: streamID.PartitionId,
			StartOffset: 0,
			Offset:      0,
		},
	})
	if err := processor.doEvent(ctx); err != nil {
		t.Fatal(err)
	}

	if _, ready := processor.hbTracker.GetQuorum(); ready {
		t.Fatal("old heartbeat was retained for quorum")
	}
	if progress := processor.offsetProgressByStream[streamID]; progress.readyToCommitOffset != 1 {
		t.Fatalf("ready-to-commit offset: got %d, want 1", progress.readyToCommitOffset)
	}
	select {
	case task := <-processor.commitTasks:
		if task.offset != 1 {
			t.Fatalf("queued offset: got %d, want 1", task.offset)
		}
	case <-time.After(time.Second):
		t.Fatal("old heartbeat offset was not queued for commit")
	}
}

func TestDoReplicationDoesNotQueueOffsetWhenDBCommitFails(t *testing.T) {
	ctx := context.Background()
	streamID := types.ElementaryStreamId{ReaderId: 0, PartitionId: 0}
	processor := Processor{
		commitOffsetMode: true,
		txChannel:        make(chan func() error, 1),
		hbTracker: hb_tracker.NewHeartBeatTracker(hb_tracker.TopicPartsCount{
			TopicPartsCountMap: map[int]hb_tracker.StreamCfg{0: {PartitionsCount: 1}},
			TotalPartsCount:    1,
		}),
		txQueue:                tx_queue.NewTxQueue(),
		lastPosition:           NewAtopmicPos(),
		stage:                  STAGE_RUN,
		offsetProgressByStream: make(map[types.ElementaryStreamId]*partitionOffsetProgress),
		commitTasks:            make(chan offsetCommitTask, 1),
	}
	processor.lastPosition.Store(types.Position{})
	_ = processor.EnqueueHb(ctx, types.HbData{
		StreamId: streamID,
		Step:     1,
		TopicOffset: &types.TopicOffset{
			PartitionId: 0,
			StartOffset: 0,
			Offset:      0,
		},
	})

	wantErr := errors.New("DB commit failed")
	_, err := processor.DoReplication(ctx, nil,
		func(func(context.Context, table.Session, table.Transaction) error) error { return wantErr })
	if !errors.Is(err, wantErr) {
		t.Fatalf("replication error: got %v, want %v", err, wantErr)
	}
	if len(processor.commitTasks) != 0 {
		t.Fatal("topic offset was queued after DB commit failure")
	}
	if progress := processor.offsetProgressByStream[streamID]; progress.readyToCommitOffset != 0 {
		t.Fatalf("ready-to-commit offset advanced after DB commit failure: %d", progress.readyToCommitOffset)
	}
}

func TestInitialScanDoesNotMarkHeartbeatOffsetWhenDBCommitFails(t *testing.T) {
	ctx := context.Background()
	processor := Processor{
		commitOffsetMode: true,
		hbTracker: hb_tracker.NewHeartBeatTracker(hb_tracker.TopicPartsCount{
			TopicPartsCountMap: map[int]hb_tracker.StreamCfg{0: {PartitionsCount: 1}},
			TotalPartsCount:    1,
		}),
		txQueue: tx_queue.NewTxQueue(),
		stage:   STAGE_INITIAL_SCAN,
		initialScanPos: &types.HbData{
			StreamId: types.ElementaryStreamId{ReaderId: 0, PartitionId: 0},
			Step:     1,
			TopicOffset: &types.TopicOffset{
				PartitionId: 0,
				StartOffset: 0,
				Offset:      0,
			},
		},
		offsetProgressByStream: make(map[types.ElementaryStreamId]*partitionOffsetProgress),
		commitTasks:            make(chan offsetCommitTask, 1),
	}
	wantErr := errors.New("DB commit failed")

	_, err := processor.DoInitialScan(ctx, nil,
		func(func(context.Context, table.Session, table.Transaction) error) error { return wantErr })
	if !errors.Is(err, wantErr) {
		t.Fatalf("initial scan error: got %v, want %v", err, wantErr)
	}
	if len(processor.offsetProgressByStream) != 0 {
		t.Fatal("heartbeat offset was registered after DB commit failure")
	}
	if len(processor.commitTasks) != 0 {
		t.Fatal("heartbeat offset was queued after DB commit failure")
	}
	if processor.stage != STAGE_INITIAL_SCAN {
		t.Fatalf("stage changed after DB commit failure: %s", processor.stage)
	}
}
