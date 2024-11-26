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
	res := Serialize(b, "t1", FmtRaw)
	assert.Equal(t, 7, len(res))
	assert.Equal(t, FmtRaw, res[0])
	assert.Equal(t, uint8(116), res[1]) // t
	assert.Equal(t, uint8(49), res[2])  // 1
	assert.Equal(t, uint8(107), res[3]) // k
	assert.Equal(t, uint8(49), res[4])  // 1
	assert.Equal(t, uint8(107), res[5]) // k
	assert.Equal(t, uint8(50), res[6])  // 2
}

func TestSerializeAsRawEmpty(t *testing.T) {
	var b []json.RawMessage
	res := Serialize(b, "t1", FmtRaw)
	assert.Equal(t, 3, len(res))
	assert.Equal(t, FmtRaw, res[0])
	assert.Equal(t, uint8(116), res[1]) // t
	assert.Equal(t, uint8(49), res[2])  // 1
}
