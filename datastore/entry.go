package datastore

import (
	"encoding/binary"
)

type entry struct {
	key, value string
}

func (e *entry) Encode() []byte {
	kl := len(e.key)
	vl := len(e.value)
	size := kl + vl + 8
	res := make([]byte, size)
	binary.LittleEndian.PutUint32(res, uint32(kl))
	binary.LittleEndian.PutUint32(res[4:], uint32(vl))
	copy(res[8:], e.key)
	copy(res[8+kl:], e.value)
	return res
}

func (e *entry) Decode(data []byte) {
	kl := binary.LittleEndian.Uint32(data)
	vl := binary.LittleEndian.Uint32(data[4:])
	e.key = string(data[8 : 8+kl])
	e.value = string(data[8+kl : 8+kl+vl])
}
