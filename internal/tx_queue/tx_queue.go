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

func (txQueue *TxQueue) PopTxsByStep(step uint64) []types.TxData {
	result := make([]types.TxData, 0)
	for txQueue.pq.Get() != nil && txQueue.pq.Get().Step < step {
		item := heap.Pop(txQueue.pq).(QueueItem)
		result = append(result, item.item)
	}
	return result
}

func (txQueue *TxQueue) PopTxsByCountAndStep(step uint64, maxCount int) []types.TxData {
	result := make([]types.TxData, 0)
	for txQueue.pq.Get() != nil && len(result) < maxCount && txQueue.pq.Get().Step < step {
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
