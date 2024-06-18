package reader

import (
	"aardappel/internal/types"
	"aardappel/internal/util/xlog"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
	"go.uber.org/zap"
	"io"
)

type TopicTxData struct {
	Update map[string]interface{} `json:"update"`
	Erase  map[string]interface{} `json:"erase"`
	Key    []interface{}          `json:"key"`
	TS     []uint64               `json:"ts"`
}

type TopicResolvedData struct {
	Resolved []uint64 `json:"resolved"`
}

type TopicData struct {
	Update   json.RawMessage `json:"update"`
	Erase    json.RawMessage `json:"erase"`
	Resolved json.RawMessage `json:"resolved"`
}

func ParseTxData(ctx context.Context, jsonData []byte) (types.TxData, error) {
	var txData TopicTxData
	err := json.Unmarshal(jsonData, &txData)
	if err != nil {
		xlog.Error(ctx, "Unable to parse tx data", zap.Error(err))
		return types.TxData{}, fmt.Errorf("error parse tx data: %w", err)
	}

	if txData.Update != nil && txData.Erase != nil {
		xlog.Error(ctx, "Unknown format of topic message. Only one of update and erase can be existed")
		return types.TxData{}, errors.New("error parse tx data: unknown format of tx data")
	}

	var data types.TxData
	if txData.Update != nil {
		data.ColumnValues = txData.Update
		data.OperationType = types.TxOperationUpdate
	}
	if txData.Erase != nil {
		data.ColumnValues = txData.Erase
		data.OperationType = types.TxOperationErase
	}
	data.KeyValues = txData.Key

	if len(txData.TS) != 2 {
		xlog.Error(ctx, "Unable to get step and tx_id from tx data",
			zap.Int("ts_len", len(txData.TS)))
		return types.TxData{}, errors.New("error parse tx data: len of ts in tx data is not 2")
	}
	data.Step = txData.TS[0]
	data.TxId = txData.TS[1]

	xlog.Debug(ctx, "Parsed tx data",
		zap.Any("column_values", data.ColumnValues),
		zap.String("operation_type", data.OperationType.String()),
		zap.Any("key", data.KeyValues),
		zap.Uint64("step", data.Step),
		zap.Uint64("tx_id", data.TxId))

	return data, nil
}

func ParseHBData(ctx context.Context, jsonData []byte, streamId types.StreamId) (types.HbData, error) {
	var resolvedData TopicResolvedData
	err := json.Unmarshal(jsonData, &resolvedData)
	if err != nil {
		xlog.Error(ctx, "Unable to parse resolved data", zap.Error(err))
		return types.HbData{}, fmt.Errorf("error parse hb data: %w", err)
	}

	if len(resolvedData.Resolved) != 2 {
		xlog.Error(ctx, "Unable to get step from resolved data",
			zap.Int("resolved_len", len(resolvedData.Resolved)))
		return types.HbData{}, errors.New("error parse hb data: len of resolved in hb data is not 2")
	}

	var data types.HbData
	data.StreamId = streamId
	data.Step = resolvedData.Resolved[0]

	//xlog.Debug(ctx, "Parsed hb data",
	//	zap.Uint64("step", data.Step),
	//	zap.Int64("partition_id", data.StreamId.PartitionId),
	//	zap.Uint8("partition_id", data.StreamId.ReaderId))

	return data, nil
}

func ReadTopic(ctx context.Context, readerId uint8, reader *topicreader.Reader) {
	for {
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
			ParseTxData(ctx, jsonData)
			// Add tx to txQueue
		} else if topicData.Resolved != nil {
			ParseHBData(ctx, jsonData, types.StreamId{readerId, msg.PartitionID()})
			// Update last hb for partition
		} else {
			xlog.Error(ctx, "Unknown format of topic message")
			return
		}

		err = reader.Commit(msg.Context(), msg)
		if err != nil {
			xlog.Error(ctx, "Unable to commit", zap.Error(err))
			return
		}
	}
}
