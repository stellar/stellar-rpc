package events

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
)

// This file is the SHARED packed-row codec: the byte encoding for a
// term-sorted batch of (term → ascending event IDs) postings. It is used by
//
//   - the hot events store's per-ledger index row (one row per ledger in the
//     hot chunk DB — the durable truth the hot sorted-run tier replays), and
//   - the cold build's spill runs (the sorted runs the spill-and-merge
//     external index build writes and k-way merges),
//
// which is what lets the freeze path merge hot rows and cold runs with one
// codec. Layout, per term (terms byte-sorted, so rows are mergeable):
//
//	16-byte term ‖ uvarint id-count ‖ ids as uvarints
//	(first absolute, then strictly-positive deltas)
//
// Event IDs are strictly ascending per term, so deltas are always ≥ 1 —
// DecodePackedRow rejects a zero delta as corruption.

// AppendPackedRow appends one packed row for perKeyIDs to dst and returns it.
// Each term's ID list must already be ascending (the single-writer ingest
// order); terms are byte-sorted here so the encoding is deterministic and
// merge-friendly. Delta-varint keeps a term with k sequential IDs to ~1 byte
// per ID.
func AppendPackedRow(dst []byte, perKeyIDs map[TermKey][]uint32) []byte {
	terms := make([]TermKey, 0, len(perKeyIDs))
	for k := range perKeyIDs {
		terms = append(terms, k)
	}
	slices.SortFunc(terms, func(a, b TermKey) int { return bytes.Compare(a[:], b[:]) })
	for _, t := range terms {
		dst = AppendTermPostings(dst, t, perKeyIDs[t])
	}
	return dst
}

// AppendTermPostings appends ONE term's record (term ‖ uvarint count ‖
// delta-varint ascending ids) to dst — the streaming primitive under
// AppendPackedRow, used directly by producers that emit terms one at a time
// in already-sorted order (the cold build's spill slab and run merge).
func AppendTermPostings(dst []byte, term TermKey, ids []uint32) []byte {
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

// DecodePackedRow parses a packed row, yielding each term's absolute
// event-ID list into add. IDs within a term must be strictly increasing
// (zero deltas rejected); every structural violation is an error — rows are
// untrusted input to warmup and to the cold merge. ids is reused across
// terms; add must consume it before returning (callers copy or fold).
func DecodePackedRow(val []byte, add func(term TermKey, ids []uint32)) error {
	var ids []uint32
	for len(val) > 0 {
		if len(val) < 16 {
			return fmt.Errorf("events: packed row: %d trailing bytes, want 16-byte term", len(val))
		}
		var term TermKey
		copy(term[:], val[:16])
		var err error
		if ids, val, err = decodePostings(val[16:], ids); err != nil {
			return fmt.Errorf("events: packed row: %w", err)
		}
		add(term, ids)
	}
	return nil
}

// decodePostings decodes the posting list at the head of b — uvarint count,
// then the IDs — into ids, and returns the unconsumed remainder. Errors are
// unwrapped; callers add their own context, matching DecodeAscendingIDs.
func decodePostings(b []byte, ids []uint32) ([]uint32, []byte, error) {
	count, n := binary.Uvarint(b)
	if n <= 0 {
		return nil, nil, errors.New("bad id-count uvarint")
	}
	b = b[n:]
	// Each ID takes ≥1 byte, so a count beyond the remaining bytes is
	// structurally impossible — reject before allocating for it.
	if count == 0 || count > uint64(len(b)) {
		return nil, nil, fmt.Errorf("id count %d exceeds %d remaining bytes", count, len(b))
	}
	next := func() (uint64, error) {
		v, k := binary.Uvarint(b)
		if k <= 0 {
			return 0, errors.New("bad event-id uvarint")
		}
		b = b[k:]
		return v, nil
	}
	// Grow once rather than letting append double its way there. count is
	// bounded by the remaining bytes above, so it cannot force a large
	// allocation from a small record.
	if uint64(cap(ids)) < count {
		ids = make([]uint32, 0, count)
	}
	out, err := DecodeAscendingIDs(next, count, ids[:0])
	if err != nil {
		return nil, nil, err
	}
	return out, b, nil
}

// DecodeAscendingIDs decodes count event IDs (first absolute, then
// strictly-positive deltas) into ids, drawing raw uvarints from next. It is
// THE validation core for the packed-row ID stream — the slice-based
// decodePostings above and runspill's streaming RunReader both delegate here,
// so the security-relevant invariants (raw-varint reject BEFORE accumulation,
// zero-delta reject, uint32 overflow) cannot drift between decoders again (the
// near-2^64 wrap fix once had to be applied to both). Errors are unwrapped; callers add their own context.
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

// DecodePostings decodes one term's ID list as written by AppendPostings. b
// must hold exactly one posting list and nothing else: trailing bytes are
// rejected, because the cold index sizes its records from the writer and any
// slack means the record was not written by this codec.
//
// The result is freshly allocated, so it can outlive the buffer it was decoded
// from and cannot alias a sibling decode.
func DecodePostings(b []byte) ([]uint32, error) {
	out, rest, err := decodePostings(b, nil)
	if err != nil {
		return nil, fmt.Errorf("events: postings: %w", err)
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("events: postings: %d trailing bytes", len(rest))
	}
	return out, nil
}

// PackedRecordLen returns the byte length of the packed record at the head
// of b (16-byte term + uvarint count + count delta uvarints). b must hold a
// whole, previously validated record — callers use this to walk or slice
// rows whose contents were validated at decode time.
func PackedRecordLen(b []byte) int {
	n := 16
	count, m := binary.Uvarint(b[n:])
	n += m
	for range count {
		_, m := binary.Uvarint(b[n:])
		n += m
	}
	return n
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

// uvarintLen is the byte width binary.AppendUvarint would use for v.
func uvarintLen(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}
