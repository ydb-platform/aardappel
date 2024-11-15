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

func serializeKey(key []json.RawMessage) string {
	data, err := json.Marshal(key)
	if err != nil {
		return "underfined"
	}
	return string(data)
}

func WriteAllProblemTxsUntilNextHb(ctx context.Context, topicPath string, readerId uint32, reader *topicreader.Reader, channel processor.Channel, lastHb map[int64]uint64, hb uint64, partsCount int) {
	var partsIsDone map[int64]bool
	for part, partHb := range lastHb {
		if partHb >= hb {
			partsIsDone[part] = true
		}
	}
	for ctx.Err() == nil && len(partsIsDone) < partsCount {
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

			if partHb, ok := lastHb[msg.PartitionID()]; ok && data.Step < partHb {
				errString := fmt.Sprintf("Unexpected step_id in stream, last hb step_id: %v,"+
					"got tx {\"topic\":\"%v\",\"key\":%v,\"ts\":[%v,%v]}",
					hb, topicPath, serializeKey(data.KeyValues), data.Step, data.TxId)
				xlog.Error(ctx, errString)
			}
		} else if topicData.Resolved != nil {
			data, err := rd.ParseHBData(ctx, jsonData, types.StreamId{readerId, msg.PartitionID()})
			if err != nil {
				xlog.Error(ctx, "ParseTxData: Error parsing hb data", zap.Error(err))
				return
			}
			lastHb[msg.PartitionID()] = data.Step
			if data.Step >= hb {
				partsIsDone[msg.PartitionID()] = true
			}
		}
	}
}

func ReadTopic(ctx context.Context, topicPath string, readerId uint32, reader *topicreader.Reader, channel processor.Channel, partsCount int, handler processor.ConflictHandler) {
	var mu sync.Mutex
	lastHb := make(map[int64]uint64)
	// returns true - pass item, false - skip item
	verifyStream := func(part int64, data types.TxData) bool {
		hb := lastHb[part]

		if hb != 0 && data.Step < hb {

			key := serializeKey(data.KeyValues)

			if handler == nil {
				xlog.Error(ctx, "Command topic is not configured, unable to receive external instructions on actions, stopping processing.")
			} else {
				rv := handler.Handle(ctx, topicPath, key, data.Step, data.TxId)
				if rv >= 0 {
					if rv == 0 {
						xlog.Info(ctx, "skip message", zap.String("topic", topicPath),
							zap.String("key", key),
							zap.Uint64("step_id", data.Step),
							zap.Uint64("tx_id", data.TxId))
						return false
					} else {
						xlog.Info(ctx, "apply out of order message", zap.String("topic", topicPath),
							zap.String("key", key),
							zap.Uint64("step_id", data.Step),
							zap.Uint64("tx_id", data.TxId))
						return true
					}
				}
			}

			errString := fmt.Sprintf("Unexpected step_id in stream, last hb step_id: %v,"+
				"got tx {\"topic\":\"%v\",\"key\":%v,\"ts\":[%v,%v]}",
				lastHb[part], topicPath, key, data.Step, data.TxId)
			stopErr := channel.SaveReplicationState(ctx, processor.REPLICATION_FATAL_ERROR, errString)
			WriteAllProblemTxsUntilNextHb(ctx, topicPath, readerId, reader, channel, lastHb, hb, partsCount)
			if stopErr != nil {
				xlog.Fatal(ctx, errString,
					zap.NamedError("this issue was not stored in the state table due to double error", stopErr))
			} else {
				xlog.Fatal(ctx, errString)
			}
		}
		return true
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
			rv := verifyStream(msg.PartitionID(), data)
			data.CommitTopic = func() error {
				mu.Lock()
				ret := reader.Commit(msg.Context(), msg)
				mu.Unlock()
				return ret
			}
			if rv == true {
				channel.EnqueueTx(ctx, data)
			} else {
				err := data.CommitTopic()
				if err != nil {
					xlog.Error(ctx, "unable to commit topic during skip",
						zap.NamedError("topic commit error", err))
				}
			}

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
