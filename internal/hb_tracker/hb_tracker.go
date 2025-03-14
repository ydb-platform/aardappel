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

func (ht *HeartBeatTracker) guardLoop(ctx context.Context, timeout uint32) {
	for ctx.Err() == nil {
		// give chance to get heartbeat at the start time
		time.Sleep(time.Duration(timeout) * time.Second)
		if time.Now().Unix()-ht.lastFullHbTime.Load() > int64(timeout) {
			// We need to lock to get proper state of tracker
			ht.lock.Lock()
			// there is a little chance that we got full heartbeat just before lock
			// so double check it to prevent mess in logs
			lastSeenHb := ht.lastFullHbTime.Load()
			if time.Now().Unix()-lastSeenHb > int64(timeout) {
				xlog.Warn(ctx, "No heartbeat since "+
					time.Unix(lastSeenHb, 0).Format(time.DateTime),
					zap.Int("expected streams", ht.totalStreamsNum),
					zap.Int("streams with heartbeat", len(ht.streams)))
			}
			ht.lock.Unlock()
		}
	}
}

func (ht *HeartBeatTracker) StartHbGuard(ctx context.Context, timeout uint32) {
	go ht.guardLoop(ctx, timeout)
}

func (ht *HeartBeatTracker) AddHb(data types.HbData) error {
	ht.lock.Lock()
	defer ht.lock.Unlock()
	hb, ok := ht.streams[data.StreamId]
	if ok {
		if types.NewPosition(hb).LessThan(*types.NewPosition(data)) {
			// Got new heartbeat for stream - we can commit previous one
			err := hb.CommitTopic()
			if err != nil {
				return fmt.Errorf("unable to commit topic during update HB %w, stepId: %d, txId: %d", err,
					hb.Step, hb.TxId)
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

func (ht *HeartBeatTracker) GetReady() bool {
	return len(ht.streams) == ht.totalStreamsNum
}

func (ht *HeartBeatTracker) GetQuorum() (types.HbData, bool) {
	var resHb types.HbData
	if !ht.GetReady() {
		return resHb, false
	}

	var inited bool
	for _, v := range ht.streams {
		if !inited {
			resHb = v
			inited = true
		} else {
			if types.NewPosition(v).LessThan(*types.NewPosition(resHb)) {
				resHb = v
			}
		}
	}

	return resHb, true
}

func (ht *HeartBeatTracker) GetMaxHb() types.HbData {
	var resHb types.HbData

	var inited bool
	for _, v := range ht.streams {
		if !inited {
			resHb = v
			inited = true
		} else {
			if types.NewPosition(resHb).LessThan(*types.NewPosition(v)) {
				resHb = v
			}
		}
	}

	return resHb
}

func (ht *HeartBeatTracker) GetQuorumAfter(hb types.HbData) (types.HbData, bool) {
	resHb, ok := ht.GetQuorum()
	if !ok {
		return resHb, false
	}

	if types.NewPosition(hb).LessThan(*types.NewPosition(resHb)) {
		return resHb, true
	}

	return types.HbData{}, false
}

func (ht *HeartBeatTracker) Commit(data types.HbData) bool {
	ht.lock.Lock()
	defer ht.lock.Unlock()
	hb, ok := ht.streams[data.StreamId]
	if !ok {
		return true
	}
	if types.NewPosition(data).LessThan(*types.NewPosition(hb)) {
		return false
	} else {
		delete(ht.streams, data.StreamId)
		return true
	}
}
