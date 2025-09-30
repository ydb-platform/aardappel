package reader

import (
	"aardappel/internal/processor"
	"aardappel/internal/types"
	"aardappel/internal/util/errors"
	rd "aardappel/internal/util/reader"
	"aardappel/internal/util/xlog"
	client "aardappel/internal/util/ydb"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"go.uber.org/zap"
)

type StreamInfo struct {
	Id              uint32
	TopicPath       string
	PartCount       int
	ProblemStrategy string
}

type TopicData struct {
	Update   json.RawMessage `json:"update"`
	Erase    json.RawMessage `json:"erase"`
	Resolved json.RawMessage `json:"resolved"`
}

type TopicReaderGuard struct {
	lastPosition map[int64]int64
	lock         sync.Mutex
}

type UpdateOffsetFunc func(offset int64, partitionID int64)

func MakeTopicReaderGuard() (topicoptions.GetPartitionStartOffsetFunc, UpdateOffsetFunc) {
	var guard TopicReaderGuard
	guard.lastPosition = make(map[int64]int64)
	updateOffsetFunc := func(offset int64, partitionID int64) {
		guard.lock.Lock()
		defer func() {
			guard.lock.Unlock()
		}()
		guard.lastPosition[partitionID] = offset
	}

	getPartStartOffsetFunc := func(ctx context.Context,
		req topicoptions.GetPartitionStartOffsetRequest) (topicoptions.GetPartitionStartOffsetResponse, error) {

		guard.lock.Lock()
		defer func() {
			guard.lock.Unlock()
		}()

		var resp topicoptions.GetPartitionStartOffsetResponse
		offset, ok := guard.lastPosition[req.PartitionID]

		if ok {
			xlog.Info(ctx, "Start partition reading from offset",
				zap.String("topic", req.Topic),
				zap.Int64("PartitionID", req.PartitionID),
				zap.Int64("offset", offset))

			resp.StartFrom(offset)
		} else {
			xlog.Info(ctx, "Start partition reading from begin",
				zap.String("topic", req.Topic),
				zap.Int64("PartitionID", req.PartitionID))
		}

		return resp, nil

	}

	return getPartStartOffsetFunc, updateOffsetFunc
}

func serializeKey(key []json.RawMessage) string {
	data, err := json.Marshal(key)
	if err != nil {
		return "undefined"
	}
	return string(data)
}

func WriteAllProblemTxsUntilNextHb(ctx context.Context, streamInfo StreamInfo, reader *client.TopicReader, channel processor.Channel, lastHb map[int64]types.Position, hb types.Position, dlQueue *processor.DLQueue) {
	partsIsDone := make(map[int64]bool)
	for part, partHb := range lastHb {
		if hb.LessThan(partHb) {
			partsIsDone[part] = true
		}
	}
	for ctx.Err() == nil && len(partsIsDone) < streamInfo.PartCount {
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
			data, err := rd.ParseTxData(ctx, jsonData, streamInfo.Id)
			if err != nil {
				xlog.Error(ctx, "ParseTxData: Error parsing tx data", zap.Error(err))
				return
			}

			if partHb, ok := lastHb[msg.PartitionID()]; ok && (types.Position{data.Step, data.TxId}.LessThan(partHb)) {
				txInfo := fmt.Sprintf("{\"topic\":\"%v\",\"key\":%v,\"ts\":[%v,%v]}", streamInfo.TopicPath, serializeKey(data.KeyValues), data.Step, data.TxId)
				errString := fmt.Sprintf("Unexpected timestamp in stream, last hb timestamp:[%v,%v],"+
					"got tx %v", hb.Step, hb.TxId, txInfo)
				xlog.Error(ctx, errString)

				if dlQueue != nil {
					err := dlQueue.Writer.Write(ctx, errString)
					if err != nil {
						xlog.Error(ctx, "Unable to write into dead letter queue", zap.Error(err), zap.String("tx", txInfo))
					}
				}
			}
		} else if topicData.Resolved != nil {
			data, err := rd.ParseHBData(ctx, jsonData, types.ElementaryStreamId{streamInfo.Id, msg.PartitionID()})
			if err != nil {
				xlog.Error(ctx, "ParseTxData: Error parsing hb data", zap.Error(err))
				return
			}
			lastHb[msg.PartitionID()] = *types.NewPosition(data)
			if hb.LessThan(*types.NewPosition(data)) {
				partsIsDone[msg.PartitionID()] = true
			}
		}
	}
}

func ReadTopic(ctx context.Context, streamInfo StreamInfo, reader *client.TopicReader, channel processor.Channel,
	handler processor.ConflictHandler, updateOffsetCb UpdateOffsetFunc, dlQueue *processor.DLQueue) error {
	var mu sync.Mutex
	lastHb := make(map[int64]types.Position)
	// returns true - pass item, false - skip item
	verifyStream := func(part int64, data types.TxData) bool {
		if hb, ok := lastHb[part]; ok && (types.Position{data.Step, data.TxId}.LessThan(hb)) {
			key := serializeKey(data.KeyValues)

			if handler == nil {
				xlog.Error(ctx, "Command topic is not configured, unable to receive external instructions on actions, stopping processing.")
			} else {
				rv := handler.Handle(ctx, streamInfo.TopicPath, key, data.Step, data.TxId)
				if rv >= 0 {
					if rv == 0 {
						xlog.Info(ctx, "skip message", zap.String("topic", streamInfo.TopicPath),
							zap.String("key", key),
							zap.Uint64("step_id", data.Step),
							zap.Uint64("tx_id", data.TxId))
						return false
					} else {
						xlog.Info(ctx, "apply out of order message", zap.String("topic", streamInfo.TopicPath),
							zap.String("key", key),
							zap.Uint64("step_id", data.Step),
							zap.Uint64("tx_id", data.TxId))
						return true
					}
				}
			}

			txInfo := fmt.Sprintf("{\"topic\":\"%v\",\"key\":%v,\"ts\":[%v,%v]}", streamInfo.TopicPath, key, data.Step, data.TxId)
			errString := fmt.Sprintf("Unexpected timestamp in stream, last hb ts:[%v,%v], "+
				"got tx %v", lastHb[part].Step, lastHb[part].TxId, txInfo)
			xlog.Error(ctx, errString)
			if streamInfo.ProblemStrategy == types.ProblemStrategyContinue {
				if dlQueue != nil {
					err := dlQueue.Write(ctx, errString)
					if err != nil {
						xlog.Fatal(ctx, "Unable to write into dead letter queue", zap.Error(err), zap.String("tx", txInfo))
					}
				}
				xlog.Info(ctx, "skip message", zap.String("topic", streamInfo.TopicPath),
					zap.String("key", key),
					zap.Uint64("step_id", data.Step),
					zap.Uint64("tx_id", data.TxId))
				return false
			}
			stopErr := channel.SaveReplicationState(ctx, processor.REPLICATION_FATAL_ERROR, errString)
			if stopErr != nil {
				xlog.Fatal(ctx, errString,
					zap.NamedError("this issue was not stored in the state table due to double error", stopErr))
			}
			if dlQueue != nil {
				err := dlQueue.Write(ctx, errString)
				if err != nil {
					xlog.Fatal(ctx, "Unable to write into dead letter queue", zap.Error(err), zap.String("tx", txInfo))
				}
			}
			WriteAllProblemTxsUntilNextHb(ctx, streamInfo, reader, channel, lastHb, hb, dlQueue)
			xlog.Fatal(ctx, errString)
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
			if ctx.Err() != nil {
				xlog.Fatal(ctx, "Unable to read message", zap.Error(err))
			} else {
				xlog.Error(ctx, "Unable to read message, fatal error ctx is not cancelled", zap.Error(err))
				return errors.NewYDBConnectionError("reading topic message", err)
			}
			return err
		}

		updateOffsetCb(msg.Offset, msg.PartitionID())

		jsonData, err := io.ReadAll(msg)
		if err != nil {
			xlog.Error(ctx, "Unable to read all", zap.Error(err))
			return nil
		}

		var topicData TopicData
		err = json.Unmarshal(jsonData, &topicData)
		if err != nil {
			xlog.Error(ctx, "Error parsing topic data", zap.Error(err))
			return nil
		}
		if topicData.Update != nil || topicData.Erase != nil {
			data, err := rd.ParseTxData(ctx, jsonData, streamInfo.Id)
			if err != nil {
				xlog.Error(ctx, "ParseTxData: Error parsing tx data", zap.Error(err))
				return err
			}
			rv := verifyStream(msg.PartitionID(), data)
			data.CommitTopic = func() error {
				mu.Lock()
				ret := reader.Commit(msg.Context(), msg)
				mu.Unlock()
				if ctx.Err() != nil {
					xlog.Fatal(ctx, "Unable to commit message", zap.Error(err))
				} else {
					xlog.Error(ctx, "Unable to commit message, fatal error ctx is not cancelled", zap.Error(err))
					return errors.NewYDBConnectionError("reading topic message", err)
				}
				return ret
			}
			if rv {
				channel.EnqueueTx(ctx, data)
			} else {
				err := data.CommitTopic()
				if err != nil {
					xlog.Fatal(ctx, "unable to commit topic during skip",
						zap.NamedError("topic commit error", err))
				}
			}

			// Add tx to txQueue
		} else if topicData.Resolved != nil {
			data, err := rd.ParseHBData(ctx, jsonData, types.ElementaryStreamId{streamInfo.Id, msg.PartitionID()})
			if err != nil {
				xlog.Error(ctx, "ParseTxData: Error parsing hb data", zap.Error(err))
				return err
			}
			lastHb[msg.PartitionID()] = *types.NewPosition(data)
			data.CommitTopic = func() error {
				mu.Lock()
				ret := reader.Commit(msg.Context(), msg)
				mu.Unlock()
				if ctx.Err() != nil {
					xlog.Fatal(ctx, "Unable to commit message", zap.Error(err))
				} else {
					xlog.Error(ctx, "Unable to commit message, fatal error ctx is not cancelled", zap.Error(err))
					return errors.NewYDBConnectionError("reading topic message", err)
				}
				return ret
			}
			channel.EnqueueHb(ctx, data)
			// Update last hb for partition
		} else {
			xlog.Error(ctx, "Unknown format of topic message")
			return fmt.Errorf("Unknown format of topic message")
		}
	}

	return nil
}
