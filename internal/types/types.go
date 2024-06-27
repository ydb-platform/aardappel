package types

import "encoding/json"

// Tx data

type TxOperationType uint8

const (
	TxOperationUpdate  TxOperationType = 0
	TxOperationErase   TxOperationType = 1
	TxOperationUnknown TxOperationType = 2
)

func (o TxOperationType) String() string {
	return [...]string{"update", "erase", "unknown"}[o]
}

type TxData struct {
	ColumnValues  map[string]json.RawMessage
	KeyValues     []json.RawMessage
	Step          uint64
	TxId          uint64
	OperationType TxOperationType
}

func (data TxData) IsUpdateOperation() bool {
	return data.OperationType == TxOperationUpdate
}

func (data TxData) IsEraseOperation() bool {
	return data.OperationType == TxOperationErase
}

// ReaderId + PartitionId for uniq partition id in hb tracker
type StreamId struct {
	ReaderId    uint8
	PartitionId int64
}

// Hb data
type HbData struct {
	StreamId StreamId
	Step     uint64
}
