package reader

import (
	"aardappel/internal/processor"
	"aardappel/internal/types"
	rd "aardappel/internal/util/reader"
	"aardappel/internal/util/xlog"
	"context"
	"encoding/json"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
	"go.uber.org/zap"
	"io"
	"sync"
)

type TopicData struct {
	Update   json.RawMessage `json:"update"`
	Erase    json.RawMessage `json:"erase"`
	Resolved json.RawMessage `json:"resolved"`
}

func ReadTopic(ctx context.Context, readerId uint32, reader *topicreader.Reader, channel processor.Channel) {
	var mu sync.Mutex
	lastHb := make(map[int64]uint64)
	verifyStream := func(part int64, data types.TxData) {
		serializeKey := func(key []json.RawMessage) string {
			data, err := json.Marshal(key)
			if err != nil {
				return "underfined"
			}
			return string(data)
		}
		hb := lastHb[part]
		if hb != 0 && data.Step < hb {
			errString := fmt.Sprintf("Unexpected step_id in stream, last hb step_id: %v,"+
				"got tx {\"key\":%v,\"ts\":[%v,%v]}",
				lastHb[part], serializeKey(data.KeyValues), data.Step, data.TxId)
			stopErr := channel.SaveReplicationState(ctx, processor.REPLICATION_FATAL_ERROR, errString)
			if stopErr != nil {
				xlog.Fatal(ctx, errString,
					zap.NamedError("this issue was not stored in the state table due to double error", stopErr))
			} else {
				xlog.Fatal(ctx, errString)
			}
		}
	}
	defer func() {
		err := reader.Close(ctx)
		xlog.Error(ctx, "stop reader call returns", zap.Error(err))
	}()
	for ctx.Err() == nil {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			xlog.Error(ctx, "Unable to read message", zap.Error(err))
			return
		}
		jsonData, err := io.ReadAll(msg)
		if err != nil {
			xlog.Error(ctx, "Unable to read all", zap.Error(err))
			return
		}

		var topicData TopicData
		err = json.Unmarshal(jsonData, &topicData)
		if err != nil {
			xlog.Error(ctx, "Error parsing topic data", zap.Error(err))
			return
		}
		if topicData.Update != nil || topicData.Erase != nil {
			data, err := rd.ParseTxData(ctx, jsonData, readerId)
			if err != nil {
				xlog.Error(ctx, "ParseTxData: Error parsing tx data", zap.Error(err))
				return
			}
			verifyStream(msg.PartitionID(), data)
			data.CommitTopic = func() error {
				mu.Lock()
				ret := reader.Commit(msg.Context(), msg)
				mu.Unlock()
				return ret
			}
			channel.EnqueueTx(ctx, data)
			// Add tx to txQueue
		} else if topicData.Resolved != nil {
			data, err := rd.ParseHBData(ctx, jsonData, types.StreamId{readerId, msg.PartitionID()})
			if err != nil {
				xlog.Error(ctx, "ParseTxData: Error parsing hb data", zap.Error(err))
				return
			}
			lastHb[msg.PartitionID()] = data.Step
			data.CommitTopic = func() error {
				mu.Lock()
				ret := reader.Commit(msg.Context(), msg)
				mu.Unlock()
				return ret
			}
			channel.EnqueueHb(ctx, data)
			// Update last hb for partition
		} else {
			xlog.Error(ctx, "Unknown format of topic message")
			return
		}
	}
}
