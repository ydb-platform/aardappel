package key_serializer

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSerializeAsRaw(t *testing.T) {
	b := make([]json.RawMessage, 2)
	b[0] = []byte("k1")
	b[1] = []byte("k2")
	res := Serialize(b, FmtRaw)
	assert.Equal(t, 5, len(res))
	assert.Equal(t, FmtRaw, res[0])
	assert.Equal(t, uint8(107), res[1]) // k
	assert.Equal(t, uint8(49), res[2])  // 1
	assert.Equal(t, uint8(107), res[3]) // k
	assert.Equal(t, uint8(50), res[4])  // 2
}

func TestSerializeAsRawEmpty(t *testing.T) {
	var b []json.RawMessage
	res := Serialize(b, FmtRaw)
	assert.Equal(t, 1, len(res))
	assert.Equal(t, FmtRaw, res[0])
}
