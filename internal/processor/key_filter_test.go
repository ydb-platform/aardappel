package processor

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"runtime"
	"strconv"
	"testing"
)

func TestAddBlockKeys(t *testing.T) {
	var m1, m2 runtime.MemStats
	runtime.GC()

	runtime.ReadMemStats(&m1)
	ctx := context.Background()
	filter, err := NewYdbMemoryKeyFilter(ctx, nil, "")
	assert.EqualValues(t, err, nil)

	var batch [][]byte
	for i := 100; i < 1000000; i++ {
		batch = append(batch, []byte(strconv.Itoa(i)))
	}

	filter.AddKeysToBlock(ctx, batch)

	for i := 0; i < 1000000; i++ {
		r := filter.Filter(ctx, []byte(strconv.Itoa(i)))
		if i < 100 {
			assert.False(t, r)
		} else {
			assert.True(t, r)
		}
	}

	runtime.ReadMemStats(&m2)
	fmt.Println("total:", m2.TotalAlloc-m1.TotalAlloc)
	fmt.Println("Alloc:", m2.Alloc-m1.Alloc)
}
