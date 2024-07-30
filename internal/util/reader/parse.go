package reader

import (
	"aardappel/internal/types"
	"aardappel/internal/util/xlog"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go.uber.org/zap"
)

type TopicTxData struct {
	Update   map[string]json.RawMessage `json:"update"`
	NewImage map[string]json.RawMessage `json:"newImage"`
	Erase    map[string]interface{}     `json:"erase"`
	Key      []json.RawMessage          `json:"key"`
	TS       []uint64                   `json:"ts"`
}

type TopicResolvedData struct {
	Resolved []uint64 `json:"resolved"`
}

func ParseTxData(ctx context.Context, jsonData []byte, readerId uint32) (types.TxData, error) {
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
		if len(txData.NewImage) > 0 {
			data.ColumnValues = txData.NewImage
		} else {
			data.ColumnValues = txData.Update
		}
		data.OperationType = types.TxOperationUpdate
	}
	if txData.Erase != nil {
		data.ColumnValues = map[string]json.RawMessage{}
		data.OperationType = types.TxOperationErase
	}
	data.KeyValues = txData.Key
	data.TableId = readerId

	if len(txData.TS) != 2 {
		xlog.Error(ctx, "Unable to get step and tx_id from tx data",
			zap.Int("ts_len", len(txData.TS)))
		return types.TxData{}, errors.New("error parse tx data: len of ts in tx data is not 2")
	}
	data.Step = txData.TS[0]
	data.TxId = txData.TS[1]

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

	return data, nil
}
