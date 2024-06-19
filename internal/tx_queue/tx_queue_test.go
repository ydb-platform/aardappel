package tx_queue

import (
	"aardappel/internal/types"
	"container/heap"
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

	assert.True(t, item1.Less(item2))
	assert.True(t, item2.Less(item3))
	assert.True(t, item1.Less(item3))
	assert.True(t, item4.Less(item3))
	assert.True(t, item2.Less(item4))
	assert.True(t, item1.Less(item4))
}

func TestPriorityQueue(t *testing.T) {
	var pq PriorityQueue
	pq.items = make([]QueueItem, 0)
	heap.Init(&pq)
	item1 := &QueueItem{
		item: CreateTxData(8, 2),
	}
	heap.Push(&pq, item1)
	item2 := &QueueItem{
		item: CreateTxData(8, 1),
	}
	heap.Push(&pq, item2)
	item3 := &QueueItem{
		item: CreateTxData(1, 2),
	}
	heap.Push(&pq, item3)
	item4 := &QueueItem{
		item: CreateTxData(2, 1),
	}
	heap.Push(&pq, item4)
	item5 := &QueueItem{
		item: CreateTxData(2, 1),
	}
	heap.Push(&pq, item5)
	actual := make([]types.TxData, 0)
	for pq.Len() > 0 {
		item := heap.Pop(&pq).(QueueItem)
		actual = append(actual, item.item)
	}
	assert.Equal(t, []types.TxData{
		CreateTxData(1, 2),
		CreateTxData(2, 1),
		CreateTxData(2, 1),
		CreateTxData(8, 1),
		CreateTxData(8, 2)}, actual)
}

func CheckPopTxs(t *testing.T, step uint64, upperIndex int) {
	var txQueue = NewTxQueue()
	item82 := CreateTxData(8, 2)
	item81 := CreateTxData(8, 1)
	item31Erase := CreateTxData(3, 1)
	item31Erase.OperationType = types.TxOperationErase
	item31Update := CreateTxData(3, 1)
	item31Update.OperationType = types.TxOperationUpdate
	item21 := CreateTxData(2, 1)
	item12 := CreateTxData(1, 2)
	item11 := CreateTxData(1, 1)
	pushOrder := []types.TxData{item82, item81, item21, item31Erase, item31Update, item21, item12, item11}
	expectedOrder := []types.TxData{item11, item12, item21, item21, item31Erase, item31Update, item81, item82}
	for i := 0; i < len(pushOrder); i++ {
		txQueue.PushTx(pushOrder[i])
	}
	if upperIndex == -1 {
		assert.Empty(t, txQueue.PopTxs(step))
		return
	}
	assert.Equal(t, expectedOrder[0:upperIndex], txQueue.PopTxs(step))
}

func TestTxQueueGet(t *testing.T) {
	CheckPopTxs(t, 0, -1)
	CheckPopTxs(t, 1, -1)
	CheckPopTxs(t, 2, 2)
	CheckPopTxs(t, 3, 4)
	CheckPopTxs(t, 4, 6)
	CheckPopTxs(t, 8, 6)
	CheckPopTxs(t, 9, 8)
}
