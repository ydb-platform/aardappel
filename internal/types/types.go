package types

import "encoding/json"

// Tx data

type TxOperationType uint8

const (
	// topic message type
	TxOperationUpdate  TxOperationType = 0
	TxOperationErase   TxOperationType = 1
	TxOperationUnknown TxOperationType = 2

	// topic problem strategy
	ProblemStrategyStop     = "STOP"
	ProblemStrategyContinue = "CONTINUE"
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
	TableId       uint32 //local id of table in current replication
	CommitTopic   func() error
}

func (data TxData) IsUpdateOperation() bool {
	return data.OperationType == TxOperationUpdate
}

func (data TxData) IsEraseOperation() bool {
	return data.OperationType == TxOperationErase
}

// ReaderId + PartitionId for uniq partition id in hb tracker
type StreamId struct {
	ReaderId    uint32
	PartitionId int64
}

// Hb data
type HbData struct {
	StreamId    StreamId
	Step        uint64
	TxId        uint64
	CommitTopic func() error
}

type Position struct {
	Step uint64
	TxId uint64
}

func NewPosition(hb HbData) *Position {
	var pos Position
	pos.Step = hb.Step
	pos.TxId = hb.TxId
	return &pos
}

func (p Position) LessThan(other Position) bool {
	return p.Step < other.Step || (p.Step == other.Step && p.TxId < other.TxId)
}
