package tx_queue

import (
	"aardappel/internal/hb_tracker"
	"aardappel/internal/types"
	"context"
	"fmt"
	"testing"
)

func CreateDummyMap() hb_tracker.TopicPartsCount {
	topicPartsCountMap := make(map[int]hb_tracker.StreamCfg)
	//for i := 0; i < 3; i++ {
	//	topicPartsCountMap[i] = hb_tracker.StreamCfg{1, ""}
	//}
	topicPartsCountMap[0] = hb_tracker.StreamCfg{1, ""}
	topicPartsCountMap[1] = hb_tracker.StreamCfg{1, ""}
	topicPartsCountMap[2] = hb_tracker.StreamCfg{2, ""}
	return hb_tracker.TopicPartsCount{topicPartsCountMap, 4}
}

func TestNotAllPart(t *testing.T) {
	ctx := context.Background()
	tracker := hb_tracker.NewHeartBeatTracker(CreateDummyMap())
	_, _ = tracker.AddHb(ctx, types.HbData{StreamId: types.ElementaryStreamId{ReaderId: 0, PartitionId: 0}, Step: 0})

	if _, ready := tracker.GetMaxPosition(); ready {
		t.Fatal("maximum ready without every partition")
	}
	_, ready := tracker.GetQuorum()
	if ready {
		t.Error("Expect ready status - we haven't add heartbeat for each part")
	}
}

func TestAddHbReturnsObsoleteHeartbeat(t *testing.T) {
	ctx := context.Background()
	tracker := hb_tracker.NewHeartBeatTracker(CreateDummyMap())
	stream := types.ElementaryStreamId{ReaderId: 0, PartitionId: 0}
	commitCalled := false

	_, err := tracker.AddHb(ctx, types.HbData{
		StreamId:    stream,
		Step:        3,
		CommitTopic: func() error { commitCalled = true; return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	obsolete, err := tracker.AddHb(ctx, types.HbData{StreamId: stream, Step: 5})
	if err != nil || obsolete == nil || obsolete.Step != 3 {
		t.Fatalf("replaced heartbeat: obsolete=%v, err=%v", obsolete, err)
	}
	if commitCalled {
		t.Fatal("obsolete heartbeat was completed")
	}
}

func TestGetLowestHb(t *testing.T) {
	ctx := context.Background()
	tracker := hb_tracker.NewHeartBeatTracker(CreateDummyMap())
	var c1, c2, c3, c4, c5, c6, c7 bool
	f1 := func() error { c1 = true; return nil }
	f2 := func() error { c2 = true; return nil }
	f3 := func() error { c3 = true; return nil }
	f4 := func() error { c4 = true; return nil }
	f5 := func() error { c5 = true; return nil }
	f6 := func() error { c6 = true; return nil }
	f7 := func() error { c7 = true; return nil }
	_, _ = tracker.AddHb(ctx, types.HbData{StreamId: types.ElementaryStreamId{ReaderId: 0, PartitionId: 0}, Step: 3, CommitTopic: f1})
	_, _ = tracker.AddHb(ctx, types.HbData{StreamId: types.ElementaryStreamId{ReaderId: 0, PartitionId: 0}, Step: 5, CommitTopic: f2})
	_, _ = tracker.AddHb(ctx, types.HbData{StreamId: types.ElementaryStreamId{ReaderId: 0, PartitionId: 0}, Step: 6, CommitTopic: f3})

	_, _ = tracker.AddHb(ctx, types.HbData{StreamId: types.ElementaryStreamId{ReaderId: 0, PartitionId: 1}, Step: 2, CommitTopic: f4})
	_, _ = tracker.AddHb(ctx, types.HbData{StreamId: types.ElementaryStreamId{ReaderId: 0, PartitionId: 1}, Step: 7, CommitTopic: f5})

	_, _ = tracker.AddHb(ctx, types.HbData{StreamId: types.ElementaryStreamId{ReaderId: 1, PartitionId: 1}, Step: 4, CommitTopic: f6})
	_, _ = tracker.AddHb(ctx, types.HbData{StreamId: types.ElementaryStreamId{ReaderId: 1, PartitionId: 0}, Step: 5, CommitTopic: f7})

	hb, ready := tracker.GetQuorum()
	if !ready {
		t.Error("Expect ready status - we have added heartbeat for each part")
	}
	if hb.Step != 4 {
		t.Errorf("Unexpected timestamp, got: %d", hb.Step)
	}

	taken := tracker.TakeHBsAt(hb)
	if len(taken) != 1 {
		t.Errorf("Unxpected commit fail, got: %d", hb.Step)
	}

	_, ready = tracker.GetQuorum()
	if ready {
		t.Error("Expect ready status - we haven't add heartbeat for each part")
	}

	_, _ = tracker.AddHb(ctx, types.HbData{StreamId: types.ElementaryStreamId{ReaderId: 1, PartitionId: 1}, Step: 5})

	hb, ready = tracker.GetQuorum()
	if !ready {
		t.Error("Expect ready status - we have added heartbeat for each part")
	}
	if hb.Step != 5 {
		t.Errorf("Unexpected timestamp, got: %d", hb.Step)
	}

	if c1 || c2 || c3 || c4 || c5 || c6 || c7 {
		t.Error("unexpected commit from heartbeat tracker")
	}
}

func TestTakeHBsAtSharedQuorum(t *testing.T) {
	tracker := hb_tracker.NewHeartBeatTracker(CreateDummyMap())
	positions := []types.Position{
		{Step: 10, TxId: 1},
		{Step: 10, TxId: 1},
		{Step: 10, TxId: 2},
		{Step: 11},
	}
	for i, position := range positions {
		_, err := tracker.AddHb(context.Background(), types.HbData{
			StreamId: types.ElementaryStreamId{ReaderId: uint32(i / 2), PartitionId: int64(i % 2)},
			Step:     position.Step, TxId: position.TxId,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	quorum, ready := tracker.GetQuorum()
	if !ready || quorum != positions[0] {
		t.Fatalf("unexpected quorum: %v, ready=%t", quorum, ready)
	}
	if got, ready := tracker.GetMaxPosition(); !ready || got != positions[3] {
		t.Fatalf("unexpected max: %v, ready=%t", got, ready)
	}
	if got := tracker.TakeHBsAt(quorum); len(got) != 2 {
		t.Fatalf("got %d HBs, want 2", len(got))
	}
	if got := tracker.TakeHBsAt(quorum); len(got) != 0 {
		t.Fatal("heartbeats taken twice")
	}
	if _, ready := tracker.GetQuorum(); ready {
		t.Fatal("quorum ready without every partition")
	}
	if _, ready := tracker.GetMaxPosition(); ready {
		t.Fatal("maximum ready after removing partitions")
	}
	if got := tracker.TakeHBsAt(positions[2]); len(got) != 1 {
		t.Fatal("later HB was removed")
	}
}

func TestAddHbReturnsObsoleteAndKeepsLatest(t *testing.T) {
	for _, incoming := range []types.Position{{Step: 9}, {Step: 10, TxId: 1}, {Step: 10, TxId: 2}} {
		t.Run(fmt.Sprintf("%d/%d", incoming.Step, incoming.TxId), func(t *testing.T) {
			tracker := hb_tracker.NewHeartBeatTracker(hb_tracker.TopicPartsCount{TotalPartsCount: 1})
			stored := types.HbData{Step: 10, TxId: 1, TopicOffset: &types.TopicOffset{Offset: 1}, CommitTopic: func() error { t.Fatal("tracker completed stored HB"); return nil }}
			obsolete, err := tracker.AddHb(context.Background(), stored)
			if err != nil || obsolete != nil {
				t.Fatalf("first HB: obsolete=%v, err=%v", obsolete, err)
			}
			next := types.HbData{Step: incoming.Step, TxId: incoming.TxId, TopicOffset: &types.TopicOffset{Offset: 2}, CommitTopic: func() error { t.Fatal("tracker completed incoming HB"); return nil }}
			obsolete, err = tracker.AddHb(context.Background(), next)
			if err != nil || obsolete == nil {
				t.Fatalf("next HB: obsolete=%v, err=%v", obsolete, err)
			}
			kept, removed := next, stored
			if incoming.LessThan(*types.NewPosition(stored)) {
				kept, removed = stored, next
			}
			if obsolete.TopicOffset != removed.TopicOffset {
				t.Fatal("returned wrong obsolete HB")
			}
			quorum, ready := tracker.GetQuorum()
			if !ready || quorum != *types.NewPosition(kept) {
				t.Fatalf("unexpected quorum: %v, ready=%t", quorum, ready)
			}
			hbs := tracker.TakeHBsAt(quorum)
			if len(hbs) != 1 || hbs[0].TopicOffset != kept.TopicOffset {
				t.Fatal("kept wrong HB")
			}
		})
	}
}
