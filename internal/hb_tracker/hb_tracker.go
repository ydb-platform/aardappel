package hb_tracker

import (
	"aardappel/internal/types"
	"aardappel/internal/util/xlog"
	"fmt"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"sync"
	"sync/atomic"
	"time"
)

type HeartBeatTracker struct {
	streams         map[types.StreamId]types.HbData
	totalStreamsNum int
	lastFullHbTime  atomic.Int64
	lock            sync.Mutex
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

func findMissed(streams map[types.StreamId]types.HbData) []uint32 {
	var missed []uint32
	var expectedReader uint32
	for _, v := range streams {
		if v.StreamId.ReaderId > expectedReader {
			missed = append(missed, expectedReader)
		} else if v.StreamId.ReaderId == expectedReader {
			expectedReader++
		}
	}
	return missed
}

func (ht *HeartBeatTracker) guardLoop(ctx context.Context, timeout uint32, streamDbgInfos []string) {
	for {
		// give chance to get heartbeat at the start time
		time.Sleep(time.Duration(timeout) * time.Second)
		if time.Now().Unix()-ht.lastFullHbTime.Load() > int64(timeout) {
			// We need to lock to get proper state of tracker
			ht.lock.Lock()
			// there is a little chance that we got full heartbeat just before lock
			// so double check it to prevent mess in logs
			lastSeenHb := ht.lastFullHbTime.Load()
			if time.Now().Unix()-lastSeenHb > int64(timeout) {
				missed := findMissed(ht.streams)
				var missedReaders []string
				for i := range missed {
					missedReaders = append(missedReaders, streamDbgInfos[i])
				}

				xlog.Warn(ctx, "No heartbeat since "+
					time.Unix(lastSeenHb, 0).Format(time.DateTime),
					zap.Int("expected streams", ht.totalStreamsNum),
					zap.Int("streams with heartbeat", len(ht.streams)),
					zap.Strings("missed readers", missedReaders))
			}
			ht.lock.Unlock()
		}

	}
}

func (ht *HeartBeatTracker) StartHbGuard(ctx context.Context, timeout uint32, streamDbgInfos []string) {
	go ht.guardLoop(ctx, timeout, streamDbgInfos)
}

func (ht *HeartBeatTracker) AddHb(data types.HbData) error {
	ht.lock.Lock()
	defer ht.lock.Unlock()
	hb, ok := ht.streams[data.StreamId]
	if ok {
		if hb.Step < data.Step {
			// Got new heartbeat for stream - we can commit previous one
			err := hb.CommitTopic()
			if err != nil {
				return fmt.Errorf("unable to commit topic during update HB %w, stepId: %d", err,
					hb.Step)
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
	} else if len(ht.streams) == ht.totalStreamsNum {
		ht.lastFullHbTime.Store(time.Now().Unix())
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
	ht.lock.Lock()
	defer ht.lock.Unlock()
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
