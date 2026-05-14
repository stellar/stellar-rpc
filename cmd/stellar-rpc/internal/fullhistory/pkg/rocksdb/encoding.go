package rocksdb

import "encoding/binary"

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

// DecodeUint32 returns the uint32 from a 4-byte big-endian buffer,
// or 0 if len(b) != 4.
func DecodeUint32(b []byte) uint32 {
	if len(b) != 4 {
		return 0
	}
	return byteOrder.Uint32(b)
}

// EncodeUint64 returns a freshly-allocated 8-byte big-endian encoding.
func EncodeUint64(n uint64) []byte {
	b := make([]byte, 8)
	byteOrder.PutUint64(b, n)
	return b
}

// DecodeUint64 returns the uint64 from an 8-byte big-endian buffer,
// or 0 if len(b) != 8.
func DecodeUint64(b []byte) uint64 {
	if len(b) != 8 {
		return 0
	}
	return byteOrder.Uint64(b)
}
