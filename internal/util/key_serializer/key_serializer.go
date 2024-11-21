package key_serializer

import "encoding/json"

const (
	FmtRaw = uint8(1)
)

func serializeAsRaw(key []json.RawMessage) []byte {
	var totalLen int
	totalLen = 1
	for i := 0; i < len(key); i++ {
		totalLen += len(key[i])
	}
	bs := make([]byte, totalLen)
	bs[0] = FmtRaw
	var pos int
	pos = 1
	for i := 0; i < len(key); i++ {
		pos += copy(bs[pos:], key[i][0:])
	}
	if pos != totalLen {
		panic("bug in raw key serialization!")
	}
	return bs
}

func Serialize(key []json.RawMessage, format uint8) []byte {
	switch format {
	case FmtRaw:
		return serializeAsRaw(key)
	}
	panic("unexpected format")
}
