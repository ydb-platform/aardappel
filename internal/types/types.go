package types

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
	ColumnValues  map[string]interface{}
	KeyValues     []interface{}
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

type StreamId struct {
	TopicId     int64
	PartitionId int64
}

// Hb data
type HbData struct {
	PartitionId StreamId
	Step        uint64
}
