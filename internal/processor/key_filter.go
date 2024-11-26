package processor

import "context"

type KeyFilter interface {
	// Returns true if key should be skipped
	Filter(ctx context.Context, key []byte) bool
	AddKeysToBlock(ctx context.Context, keys [][]byte)
	GetBlockedKeysCount() uint64
}
