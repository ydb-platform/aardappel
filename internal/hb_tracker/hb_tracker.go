package hb_tracker

import (
	"aardappel/internal/types"
	"fmt"
)

type HeartBeatTracker struct {
	streams         map[types.StreamId]types.HbData
	totalStreamsNum int
}

type Feeder interface {
	AddHb(data types.HbData) error
}

func NewHeartBeatTracker(total int) *HeartBeatTracker {
	if total == 0 {
		return nil
	}

	var hbt HeartBeatTracker
	hbt.streams = make(map[types.StreamId]types.HbData)
	hbt.totalStreamsNum = total
	return &hbt
}

func (ht *HeartBeatTracker) AddHb(data types.HbData) error {
	hb, ok := ht.streams[data.StreamId]
	if ok {
		if hb.Step < data.Step {
			// Got new heartbeat for stream - we can commit previous one
			err := hb.CommitTopic()
			if err != nil {
				return err
			}
			hb = data
		}
	} else {
		hb = data
	}
	ht.streams[data.StreamId] = hb

	if len(ht.streams) > ht.totalStreamsNum {
		return fmt.Errorf("Resulted stream count: %d grather than total count: %d",
			len(ht.streams), ht.totalStreamsNum)
	}
	return nil
}

func (ht *HeartBeatTracker) GetReady() (types.HbData, bool) {
	var resHb types.HbData

	if len(ht.streams) != ht.totalStreamsNum {
		return resHb, false
	}

	var inited bool
	for _, v := range ht.streams {
		if !inited {
			resHb = v
			inited = true
		} else {
			if v.Step < resHb.Step {
				resHb = v
			}
		}
	}

	return resHb, true
}

func (ht *HeartBeatTracker) Commit(data types.HbData) bool {
	hb, ok := ht.streams[data.StreamId]
	if !ok {
		return true
	}
	if hb.Step > data.Step {
		return false
	} else {
		delete(ht.streams, data.StreamId)
		return true
	}
}
