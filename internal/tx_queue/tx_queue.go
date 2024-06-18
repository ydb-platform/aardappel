package tx_queue

import (
	"aardappel/internal/types"
	"container/heap"
)

type TxQueue struct {
	pq *PriorityQueue
}

func NewTxQueue() *TxQueue {
	pq := NewPriorityQueue()
	heap.Init(pq)
	return &TxQueue{pq}
}

func (txQueue *TxQueue) PushTx(data types.TxData) {
	heap.Push(txQueue.pq, data)
}

func (txQueue *TxQueue) GetTxs(step uint64) []types.TxData {
	result := make([]types.TxData, 0)
	for _, item := range txQueue.pq.items {
		if item.item.Step >= step {
			return result
		}
		result = append(result, item.item)
	}
	return result
}

func (txQueue *TxQueue) CleanTxs(step uint64) {
	for txQueue.pq.Get() != nil && txQueue.pq.Get().Step < step {
		var _ = heap.Pop(txQueue.pq)
	}
}
