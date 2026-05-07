package rocksdb

import "encoding/binary"

// byteOrder is the endianness used by every RocksDB key or value in
// this codebase that stores an integer as raw bytes.
// Big-endian: a uint32-keyed store iterates lexicographically in the
// SAME order as it iterates numerically, so range scans like "give me
// ledgers 100 through 200" work as a single contiguous iterator pass
// instead of a scattered sequence of point lookups.
//
// Different on-disk formats in this project pick different
// endiannesses, and that is intentional — the choice is dictated by
// the access pattern, not a project-wide convention.
// The packfile format (pkg/lfs) uses little-endian because its reads
// are positional: the reader is handed a byte offset, seeks to it,
// and decodes from there. Either endianness reads back correctly
// because no iteration over keys is involved.
// RocksDB does not have that luxury. Iterator scans walk keys in
// byte-lex order, so the byte encoding has to match the numeric
// order we want to scan in, which is what big-endian gives us.
// Little-endian would silently return scrambled and polluted ranges
// from a single-pass range scan with no error flagged.
// See experiment_endianness_test.go in this package for the
// empirical demonstration: same fixture, same iterator, only the
// encoding differs, and LE returns 8 values for GetLedgerRange(100,
// 200) where BE returns the correct 5.
//
// Unexported on purpose.
// No code outside pkg/rocksdb reads this directly.
// Layer-2 facades that need to encode a uint32 / uint64 as bytes call
// EncodeUint32 / DecodeUint32 / EncodeUint64 / DecodeUint64 below.
// That keeps the endianness pinned in one place — flip this var and
// every encoded value flips with it.
//
//nolint:gochecknoglobals // single source of truth for storage byte order
var byteOrder binary.ByteOrder = binary.BigEndian

// EncodeUint32 encodes n as 4 bytes in the project's storage byte
// order.
// Use this for any uint32 you're about to write into a RocksDB key or
// value (ledger sequence, chunk ID, tx-index ID, ...).
// Returns a freshly-allocated 4-byte slice the caller owns.
func EncodeUint32(n uint32) []byte {
	b := make([]byte, 4)
	byteOrder.PutUint32(b, n)
	return b
}

// DecodeUint32 reads 4 bytes encoded by EncodeUint32 and returns the
// uint32.
// Returns 0 if b is not exactly 4 bytes — callers that need to tell
// "decode failed" from "the encoded value really was zero" must
// validate len(b) == 4 themselves first.
func DecodeUint32(b []byte) uint32 {
	if len(b) != 4 {
		return 0
	}
	return byteOrder.Uint32(b)
}

// EncodeUint64 encodes n as 8 bytes in the project's storage byte
// order.
// Use this for any uint64 you're about to write into a RocksDB key or
// value.
// Returns a freshly-allocated 8-byte slice the caller owns.
func EncodeUint64(n uint64) []byte {
	b := make([]byte, 8)
	byteOrder.PutUint64(b, n)
	return b
}

// DecodeUint64 reads 8 bytes encoded by EncodeUint64 and returns the
// uint64.
// Returns 0 if b is not exactly 8 bytes — same caveat as
// DecodeUint32.
func DecodeUint64(b []byte) uint64 {
	if len(b) != 8 {
		return 0
	}
	return byteOrder.Uint64(b)
}
