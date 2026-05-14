package rocksdb

import (
	"encoding/binary"
	"fmt"
)

// byteOrder — every uint32/uint64 stored as raw bytes in this codebase
// uses big-endian. RocksDB iterator scans walk in byte-lex order, so
// numeric keys must encode lex == numeric — only big-endian gives that
// for unsigned ints. Little-endian on a uint32 key would produce
// scrambled ranges from a single-pass range scan.
//
//nolint:gochecknoglobals // single source of truth for storage byte order
var byteOrder binary.ByteOrder = binary.BigEndian

// EncodeUint32 returns a freshly-allocated 4-byte big-endian encoding.
func EncodeUint32(n uint32) []byte {
	b := make([]byte, 4)
	byteOrder.PutUint32(b, n)
	return b
}

// DecodeUint32 returns the uint32 from a 4-byte big-endian buffer.
// Panics if len(b) != 4 — both writer and reader are under our
// control, so a wrong length is a code bug, not a runtime condition.
func DecodeUint32(b []byte) uint32 {
	if len(b) != 4 {
		panic(fmt.Sprintf("rocksdb: DecodeUint32: expected 4 bytes, got %d", len(b)))
	}
	return byteOrder.Uint32(b)
}

// EncodeUint64 returns a freshly-allocated 8-byte big-endian encoding.
func EncodeUint64(n uint64) []byte {
	b := make([]byte, 8)
	byteOrder.PutUint64(b, n)
	return b
}

// DecodeUint64 returns the uint64 from an 8-byte big-endian buffer.
// Panics if len(b) != 8 — same rationale as DecodeUint32.
func DecodeUint64(b []byte) uint64 {
	if len(b) != 8 {
		panic(fmt.Sprintf("rocksdb: DecodeUint64: expected 8 bytes, got %d", len(b)))
	}
	return byteOrder.Uint64(b)
}
