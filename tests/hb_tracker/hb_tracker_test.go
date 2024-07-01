package tx_queue

import (
	"aardappel/internal/hb_tracker"
	"aardappel/internal/types"
	"testing"
)

func TestNotAllPart(t *testing.T) {
	tracker := hb_tracker.NewHeartBeatTracker(3)
	_ = tracker.AddHb(types.HbData{StreamId: types.StreamId{ReaderId: 0, PartitionId: 0}, Step: 0})

	_, ready := tracker.GetReady()
	if ready {
		t.Error("Expect ready status - we haven't add heartbeat for each part")
	}
}

func TestGetLowestHb(t *testing.T) {
	tracker := hb_tracker.NewHeartBeatTracker(3)
	var c1, c2, c3, c4, c5, c6 bool
	f1 := func() error { c1 = true; return nil }
	f2 := func() error { c2 = true; return nil }
	f3 := func() error { c3 = true; return nil }
	f4 := func() error { c4 = true; return nil }
	f5 := func() error { c5 = true; return nil }
	f6 := func() error { c6 = true; return nil }
	_ = tracker.AddHb(types.HbData{StreamId: types.StreamId{ReaderId: 0, PartitionId: 0}, Step: 3, CommitTopic: f1})
	_ = tracker.AddHb(types.HbData{StreamId: types.StreamId{ReaderId: 0, PartitionId: 0}, Step: 5, CommitTopic: f2})
	_ = tracker.AddHb(types.HbData{StreamId: types.StreamId{ReaderId: 0, PartitionId: 0}, Step: 6, CommitTopic: f3})

	_ = tracker.AddHb(types.HbData{StreamId: types.StreamId{ReaderId: 0, PartitionId: 1}, Step: 2, CommitTopic: f4})
	_ = tracker.AddHb(types.HbData{StreamId: types.StreamId{ReaderId: 0, PartitionId: 1}, Step: 7, CommitTopic: f5})

	_ = tracker.AddHb(types.HbData{StreamId: types.StreamId{ReaderId: 1, PartitionId: 1}, Step: 4, CommitTopic: f6})

	hb, ready := tracker.GetReady()
	if !ready {
		t.Error("Expect ready status - we have added heartbeat for each part")
	}
	if hb.Step != 4 {
		t.Errorf("Unexpected timestamp, got: %d", hb.Step)
	}

	ok := tracker.Commit(hb)
	if !ok {
		t.Errorf("Unxpected commit fail, got: %d", hb.Step)
	}

	_, ready = tracker.GetReady()
	if ready {
		t.Error("Expect ready status - we haven't add heartbeat for each part")
	}

	_ = tracker.AddHb(types.HbData{StreamId: types.StreamId{ReaderId: 1, PartitionId: 1}, Step: 5})

	hb, ready = tracker.GetReady()
	if !ready {
		t.Error("Expect ready status - we have added heartbeat for each part")
	}
	if hb.Step != 5 {
		t.Errorf("Unexpected timestamp, got: %d", hb.Step)
	}

	if c1 && c2 && c4 == false {
		t.Error("missed commit from heartbeat tracker")
	}

	if c3 || c5 || c6 == true {
		t.Error("unexpected commit from heartbeat tracker")
	}
}
