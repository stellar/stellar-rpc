// Package events holds the postings vocabulary the event stores share: the
// delta-varint codec one term's ascending event IDs are stored in, the
// Postings value that carries a term in whichever form the store already had
// it, and the set operations that work on either form.
//
// It sits below stores/event so the cold index writer, the cold reader and
// the hot in-RAM index can all speak about a term's postings without any of
// them depending on the others.
package events

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// This file is the postings codec: the byte encoding for ONE term's ascending
// event IDs, without any term prefix.
//
//	uvarint id-count ‖ ids as uvarints
//	(first absolute, then strictly-positive deltas)
//
// Event IDs are strictly ascending per term, so deltas are always ≥ 1 — the
// decoder rejects a zero delta as corruption. index.pack's delta codec
// (itemCodecDelta, see stores/event/cold_format.go) is exactly this encoding
// behind a codec byte.

// AppendPostings appends one term's ID list — uvarint count, then the IDs
// (first absolute, then strictly-positive deltas) — WITHOUT any term prefix,
// for consumers that already know which term they are reading. ids must be
// strictly ascending.
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
// THE validation core for the ID stream — every decoder delegates here, so the
// security-relevant invariants (raw-varint reject BEFORE accumulation,
// zero-delta reject, uint32 overflow) cannot drift between decoders. Errors
// are unwrapped; callers add their own context.
//
// It takes a next function rather than a slice because a streaming decoder
// over an io.Reader has to share these checks with the slice-based
// decodePostings above.
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
