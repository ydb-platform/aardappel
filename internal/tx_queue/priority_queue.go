package tx_queue

import "aardappel/internal/types"

type QueueItem struct {
	item  types.TxData
	index int
	order uint32
}

func (lhs QueueItem) Less(rhs QueueItem) bool {
	return lhs.item.Step < rhs.item.Step ||
		lhs.item.Step == rhs.item.Step && lhs.item.TxId < rhs.item.TxId ||
		lhs.item.Step == rhs.item.Step && lhs.item.TxId == rhs.item.TxId && lhs.order < rhs.order
}

type PriorityQueue struct {
	items []QueueItem
	order uint32
}

func (pq PriorityQueue) Len() int { return len(pq.items) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq.items[i].Less(pq.items[j])
}

func (pq PriorityQueue) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
	pq.items[i].index = i
	pq.items[j].index = j
}

func (pq *PriorityQueue) Push(value interface{}) {
	currentLen := len(pq.items)
	item := value.(*QueueItem)
	item.index = currentLen
	item.order = pq.order
	pq.items = append(pq.items, *item)
	pq.order++
}

func (pq *PriorityQueue) Get() *types.TxData {
	if len(pq.items) == 0 {
		return nil
	}
	return &pq.items[0].item
}

func (pq *PriorityQueue) Pop() interface{} {
	currentItems := pq.items
	currentLen := len(currentItems)
	item := currentItems[currentLen-1]
	item.index = -1
	pq.items = currentItems[0 : currentLen-1]
	return item
}
