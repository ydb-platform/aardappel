package hb_tracker

import (
	"aardappel/internal/types"
	"aardappel/internal/util/misc"
	"aardappel/internal/util/xlog"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"golang.org/x/net/context"
)

type HeartBeatTracker struct {
	streams         map[types.ElementaryStreamId]types.HbData
	totalStreamsNum int
	streamLayout    map[int]StreamCfg
	lastFullHbTime  atomic.Int64
	lock            sync.Mutex
}

type StreamCfg struct {
	PartitionsCount int
	MonTag          string
}

type TopicPartsCount struct {
	TopicPartsCountMap map[int]StreamCfg
	TotalPartsCount    int
}

func NewTopicPartsCount() *TopicPartsCount {
	var topics TopicPartsCount
	topics.TopicPartsCountMap = make(map[int]StreamCfg)
	topics.TotalPartsCount = 0
	return &topics
}

type GuardMetrics interface {
	TopicWithoutHB(noHb bool, tag string)
}

func NewHeartBeatTracker(streamLayout TopicPartsCount) *HeartBeatTracker {
	total := streamLayout.TotalPartsCount
	if total == 0 {
		return nil
	}

	var hbt HeartBeatTracker
	hbt.streams = make(map[types.ElementaryStreamId]types.HbData)
	hbt.totalStreamsNum = total
	hbt.streamLayout = streamLayout.TopicPartsCountMap
	return &hbt
}

func (ht *HeartBeatTracker) findMissed() []int {
	missed := make([]int, ht.totalStreamsNum)
	for readerId := 0; readerId < ht.totalStreamsNum; readerId++ {
		partitionsCount := ht.streamLayout[readerId].PartitionsCount
		for partitionId := 0; partitionId < partitionsCount; partitionId++ {
			_, ok := ht.streams[types.ElementaryStreamId{ReaderId: uint32(readerId), PartitionId: int64(partitionId)}]
			if !ok {
				missed[readerId]++
			}
		}
	}
	return missed
}

func (ht *HeartBeatTracker) guardLoop(ctx context.Context, timeout uint32, metrics GuardMetrics) {
	for ctx.Err() == nil {
		// give chance to get heartbeat at the start time
		time.Sleep(time.Duration(timeout) * time.Second)
		resetMon := false
		if time.Now().Unix()-ht.lastFullHbTime.Load() > int64(timeout) {
			// We need to lock to get proper state of tracker
			ht.lock.Lock()
			// there is a little chance that we got full heartbeat just before lock
			// so double check it to prevent mess in logs
			lastSeenHb := ht.lastFullHbTime.Load()
			if time.Now().Unix()-lastSeenHb > int64(timeout) {
				missed := ht.findMissed()
				var missedStr string
				for i := 0; i < ht.totalStreamsNum; i++ {
					monTag := ht.streamLayout[i].MonTag
					if metrics != nil {
						metrics.TopicWithoutHB(misc.TernaryIf(missed[i] > 0, true, false), monTag)
					}
					if missed[i] > 0 {
						missedStr += monTag
						missedStr += ", "
					}
					if i == ht.totalStreamsNum-1 && len(missedStr) > 2 {
						missedStr = missedStr[:len(missedStr)-2]
					}
				}
				xlog.Warn(ctx, "No heartbeat since "+
					time.Unix(lastSeenHb, 0).Format(time.DateTime),
					zap.Int("expected streams", ht.totalStreamsNum),
					zap.Int("streams with heartbeat", len(ht.streams)),
					zap.String("no heartbeas in streams", missedStr))
			} else {
				resetMon = true
			}
			ht.lock.Unlock()
		} else {
			resetMon = true
		}
		if metrics != nil && resetMon {
			for i := 0; i < ht.totalStreamsNum; i++ {
				monTag := ht.streamLayout[i].MonTag
				metrics.TopicWithoutHB(false, monTag)
			}
		}
	}
}

func (ht *HeartBeatTracker) StartHbGuard(ctx context.Context, timeout uint32, metrics GuardMetrics) {
	go ht.guardLoop(ctx, timeout, metrics)
}

// AddHb keeps the latest heartbeat per stream and returns the obsolete heartbeat
// for the caller to complete. It never commits topic offsets.
func (ht *HeartBeatTracker) AddHb(ctx context.Context, data types.HbData) (*types.HbData, error) {
	ht.lock.Lock()
	defer ht.lock.Unlock()
	stored, exists := ht.streams[data.StreamId]
	if !exists && len(ht.streams) >= ht.totalStreamsNum {
		return nil, fmt.Errorf("heartbeat stream count would exceed total count: %d", ht.totalStreamsNum)
	}
	var obsolete *types.HbData
	if exists {
		if types.NewPosition(data).LessThan(*types.NewPosition(stored)) {
			xlog.Warn(ctx, "AddHb: hb tracker received hb older than current hb",
				zap.Uint32("reader_id", data.StreamId.ReaderId),
				zap.Int64("partition_id", data.StreamId.PartitionId),
				zap.Uint64("stored_step", stored.Step),
				zap.Uint64("stored_tx_id", stored.TxId),
				zap.Uint64("new_step", data.Step),
				zap.Uint64("new_tx_id", data.TxId))
			return &data, nil
		}
		obsolete = &stored
	}
	ht.streams[data.StreamId] = data
	if len(ht.streams) == ht.totalStreamsNum {
		ht.lastFullHbTime.Store(time.Now().Unix())
	}
	return obsolete, nil
}

func (ht *HeartBeatTracker) GetQuorum() (types.Position, bool) {
	if len(ht.streams) != ht.totalStreamsNum {
		return types.Position{}, false
	}
	var result types.Position
	initialized := false
	for _, hb := range ht.streams {
		position := *types.NewPosition(hb)
		if !initialized || position.LessThan(result) {
			result = position
			initialized = true
		}
	}
	return result, true
}

func (ht *HeartBeatTracker) GetMaxPosition() (types.Position, bool) {
	if len(ht.streams) != ht.totalStreamsNum {
		return types.Position{}, false
	}
	var result types.Position
	for _, hb := range ht.streams {
		position := *types.NewPosition(hb)
		if result.LessThan(position) {
			result = position
		}
	}
	return result, true
}

func (ht *HeartBeatTracker) TakeHBsAt(position types.Position) []types.HbData {
	ht.lock.Lock()
	defer ht.lock.Unlock()
	var heartbeats []types.HbData
	for stream, hb := range ht.streams {
		if *types.NewPosition(hb) == position {
			heartbeats = append(heartbeats, hb)
			delete(ht.streams, stream)
		}
	}
	return heartbeats
}
