package tx_queue

import (
	"aardappel/internal/types"
	"container/heap"
)

type TxQueue struct {
	pq *PriorityQueue
}

func NewTxQueue() *TxQueue {
	var pq PriorityQueue
	pq.items = make([]QueueItem, 0)
	heap.Init(&pq)
	return &TxQueue{&pq}
}

func (txQueue *TxQueue) PushTx(data types.TxData) {
	item := &QueueItem{
		item: data,
	}
	heap.Push(txQueue.pq, item)
}

func (txQueue *TxQueue) PopTxsByPosition(pos types.Position) []types.TxData {
	result := make([]types.TxData, 0)
	for txQueue.pq.Get() != nil && (types.Position{txQueue.pq.Get().Step, txQueue.pq.Get().TxId}.LessThan(pos)) {
		item := heap.Pop(txQueue.pq).(QueueItem)
		result = append(result, item.item)
	}
	return result
}

func (txQueue *TxQueue) PopTxsByCountAndPosition(pos types.Position, maxCount int) []types.TxData {
	result := make([]types.TxData, 0)
	for txQueue.pq.Get() != nil && len(result) < maxCount && (types.Position{txQueue.pq.Get().Step, txQueue.pq.Get().TxId}.LessThan(pos)) {
		item := heap.Pop(txQueue.pq).(QueueItem)
		result = append(result, item.item)
	}
	return result
}

func (txQueue *TxQueue) PopTxsByCount(maxCount int) []types.TxData {
	result := make([]types.TxData, 0)
	for txQueue.pq.Get() != nil && len(result) < maxCount {
		item := heap.Pop(txQueue.pq).(QueueItem)
		result = append(result, item.item)
	}
	return result
}
