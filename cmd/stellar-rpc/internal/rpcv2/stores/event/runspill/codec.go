package runspill

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// AppendTermPostings appends ONE term's record (term ‖ uvarint count ‖
// delta-varint ascending ids) to dst — the streaming primitive under
// AppendPackedRow, used directly by producers that emit terms one at a time
// in already-sorted order (the cold build's spill slab and run merge).
func AppendTermPostings(dst []byte, term [16]byte, ids []uint32) []byte {
	dst = append(dst, term[:]...)
	return AppendPostings(dst, ids)
}

// AppendPostings appends one term's ID list — uvarint count, then the IDs
// (first absolute, then strictly-positive deltas) — WITHOUT the 16-byte term
// prefix, for consumers that already know which term they are reading.
func AppendPostings(dst []byte, ids []uint32) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(ids)))
	var prev uint32
	for j, id := range ids {
		if j == 0 {
			dst = binary.AppendUvarint(dst, uint64(id))
		} else {
			dst = binary.AppendUvarint(dst, uint64(id-prev))
		}
		prev = id
	}
	return dst
}

// DecodeAscendingIDs decodes count event IDs (first absolute, then
// strictly-positive deltas) into ids, drawing raw uvarints from next. It is
// THE validation core for the packed-row ID stream — the slice-based
// DecodePackedRow above and runspill's streaming RunReader both delegate
// here, so the security-relevant invariants (raw-varint reject BEFORE
// accumulation, zero-delta reject, uint32 overflow) cannot drift between the
// two decoders again (the near-2^64 wrap fix once had to be applied to
// both). Errors are unwrapped; callers add their own context.
func DecodeAscendingIDs(next func() (uint64, error), count uint64, ids []uint32) ([]uint32, error) {
	var prev uint64
	for i := range count {
		v, err := next()
		if err != nil {
			return nil, err
		}
		// Reject the raw varint before accumulating: a crafted delta near
		// 2^64 would wrap prev+v back under MaxUint32 and smuggle a
		// non-ascending ID past the overflow check (postings are untrusted).
		if v > math.MaxUint32 {
			return nil, fmt.Errorf("delta %d overflows uint32", v)
		}
		abs := v
		if i > 0 {
			if v == 0 {
				return nil, errors.New("zero delta")
			}
			abs = prev + v
		}
		if abs > math.MaxUint32 {
			return nil, fmt.Errorf("event id %d overflows uint32", abs)
		}
		ids = append(ids, uint32(abs))
		prev = abs
	}
	return ids, nil
}

// TermPostingsLen returns the encoded byte length AppendTermPostings would
// produce for ids (16-byte term + uvarint count + delta uvarints) without
// encoding — the length arithmetic lives beside the encoder so it cannot
// drift from it.
func TermPostingsLen(ids []uint32) int {
	n := 16 + uvarintLen(uint64(len(ids)))
	var prev uint32
	for i, id := range ids {
		if i == 0 {
			n += uvarintLen(uint64(id))
		} else {
			n += uvarintLen(uint64(id - prev))
		}
		prev = id
	}
	return n
}

// uvarintLen is binary.PutUvarint's length without the buffer.
func uvarintLen(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}
