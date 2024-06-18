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
	_ = tracker.AddHb(types.HbData{StreamId: types.StreamId{ReaderId: 0, PartitionId: 0}, Step: 3})
	_ = tracker.AddHb(types.HbData{StreamId: types.StreamId{ReaderId: 0, PartitionId: 0}, Step: 5})
	_ = tracker.AddHb(types.HbData{StreamId: types.StreamId{ReaderId: 0, PartitionId: 0}, Step: 6})

	_ = tracker.AddHb(types.HbData{StreamId: types.StreamId{ReaderId: 0, PartitionId: 1}, Step: 2})
	_ = tracker.AddHb(types.HbData{StreamId: types.StreamId{ReaderId: 0, PartitionId: 1}, Step: 7})

	_ = tracker.AddHb(types.HbData{StreamId: types.StreamId{ReaderId: 1, PartitionId: 1}, Step: 4})

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
}
