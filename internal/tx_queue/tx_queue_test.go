package tx_queue

import (
	"aardappel/internal/types"
	"github.com/stretchr/testify/assert"
	"testing"
)

func CreateTxData(step uint64, txId uint64) types.TxData {
	var result types.TxData = types.TxData{}
	result.Step = step
	result.TxId = txId
	return result
}

func TestPriorityQueueLess(t *testing.T) {
	var item1 = QueueItem{CreateTxData(1, 1), 0, 0}
	var item2 = QueueItem{CreateTxData(1, 2), 1, 1}
	var item3 = QueueItem{CreateTxData(2, 1), 2, 3}
	var item4 = QueueItem{CreateTxData(2, 1), 3, 2}

	assert.True(t, item2.Less(item1))
	assert.True(t, item3.Less(item2))
	assert.True(t, item3.Less(item1))
	assert.True(t, item3.Less(item4))
	assert.True(t, item4.Less(item2))
	assert.True(t, item4.Less(item1))
}

func TestTxQueue(t *testing.T) {
	var txQueue = NewTxQueue()
	item82 := CreateTxData(8, 2)
	item81 := CreateTxData(8, 1)
	item31Erase := CreateTxData(3, 1)
	item31Erase.OperationType = types.TxOperationErase
	item31Update := CreateTxData(3, 1)
	item31Update.OperationType = types.TxOperationUpdate
	//item21 := CreateTxData(2, 1)
	item12 := CreateTxData(1, 2)
	//item11 := CreateTxData(1, 1)
	//pushOrder := []types.TxData{item82, item81, item12, item31Erase, item31Update, item21, item12, item11}
	//expectedQueue := []types.TxData{item11, item12, item12, item31Erase, item31Update, item81, item21, item82}
	//expectedQueue2 := []types.TxData{item11, item12, item12, item21, item31Erase, item31Update, item81, item82}
	//for i := 0; i < len(pushOrder); i++ {
	//	txQueue.PushTx(pushOrder[i])
	//}
	txQueue.PushTx(item82)
	assert.Equal(t, []types.TxData{item82}, txQueue.GetTxs(9))
	txQueue.PushTx(item81)
	assert.Equal(t, []types.TxData{item81, item82}, txQueue.GetTxs(9))
	txQueue.PushTx(item12)
	assert.Equal(t, []types.TxData{item12, item82, item81}, txQueue.GetTxs(9))

	//assert.Equal(t, make([]types.TxData, 0), txQueue.GetTxs(0))
	//assert.Equal(t, make([]types.TxData, 0), txQueue.GetTxs(1))
	//assert.Equal(t, expectedQueue[0:3], txQueue.GetTxs(2))
	//assert.Equal(t, expectedQueue[0:3], txQueue.GetTxs(3))
	//assert.Equal(t, expectedQueue[0:5], txQueue.GetTxs(4))
	//assert.Equal(t, expectedQueue[0:5], txQueue.GetTxs(8))
	//assert.Equal(t, expectedQueue[0:8], txQueue.GetTxs(9))
}
