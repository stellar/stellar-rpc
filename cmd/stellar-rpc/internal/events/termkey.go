package events

import (
	"encoding/binary"

	"github.com/zeebo/xxh3"
)

// TermKey is a 16-byte hash identifying a unique (field, value) pair
// in the bitmap index.
type TermKey [16]byte

// Field identifies which indexed field a term belongs to.
type Field byte

const (
	FieldContractID Field = 0
	FieldTopic0     Field = 1
	FieldTopic1     Field = 2
	FieldTopic2     Field = 3
	FieldTopic3     Field = 4
)

// ComputeTermKey computes a 16-byte term key by hashing the field byte
// followed by the value bytes: xxh3_128(field || value).
// Including the field byte ensures that the same value in different fields
// produces different keys.
func ComputeTermKey(value []byte, field Field) TermKey {
	// Prepend field byte to value for hashing.
	// Stack-allocated buffer avoids heap allocation for typical ScVal sizes.
	var scratch [128]byte
	n := len(value) + 1
	var buf []byte
	if n <= len(scratch) {
		buf = scratch[:n]
	} else {
		buf = make([]byte, n)
	}
	buf[0] = byte(field)
	copy(buf[1:], value)

	// TODO: replace with streamhash.PreHashInPlace once #654 (MPHF integration) lands.
	h := xxh3.Hash128(buf)

	var key TermKey
	binary.LittleEndian.PutUint64(key[:8], h.Lo)
	binary.LittleEndian.PutUint64(key[8:], h.Hi)
	return key
}
