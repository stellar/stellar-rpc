package event

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event/runspill"
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

// appendRecordIDs decodes ONE packed record (16-byte term ‖ uvarint count
// ‖ delta uvarints) at the head of rec, appending its ids to dst, and
// returns the record's byte length. It is decodePostings' loop with the
// closure fused away: the byte cursor is a local index rather than a
// captured slice header (the closure's escaped one costs a funcval call and
// a heap slice-header write per id), and the ids land in the caller's
// accumulator rather than in a per-record buffer that is then copied out.
// One walk, one write — the sealed-run probe (sealedRun.lookup) neither
// pre-sizes the record with PackedRecordLen nor re-copies its ids.
//
// The validation is character-for-character DecodeAscendingIDs': raw-varint
// reject BEFORE accumulation, zero delta, uint32 overflow, plus the
// pre-allocation guard on count (a THIRD copy of that guard, beside
// decodePostings above and runspill's RunReader). A second decoder beside
// the shared codec is exactly what codec.go's one-definition-site rule warns
// about — the near-2^64 wrap fix once had to be applied twice — so
// FuzzAppendRecordIDs pins the two against each other over arbitrary bytes:
// identical ids on accept, identical accept/reject classification. That is
// the pin; keep it green rather than letting the two drift.
//
// Errors are unwrapped (callers add their own context) and leave dst holding
// whatever the partial record contributed — the caller discards the
// accumulator with the error.
func appendRecordIDs(dst []uint32, rec []byte) ([]uint32, int, error) {
	// A run's fences land on record boundaries, so a well-formed window
	// cannot end mid-key; the guard keeps a truncated one an error instead
	// of an out-of-range panic in the 16-byte key slice below.
	if len(rec) < 16 {
		return dst, 0, fmt.Errorf("%d bytes, want 16-byte term", len(rec))
	}
	count, m := binary.Uvarint(rec[16:])
	if m <= 0 {
		return dst, 0, errors.New("bad id-count uvarint")
	}
	i := 16 + m
	// Each ID takes ≥1 byte, so a count beyond the remaining bytes is
	// structurally impossible — reject before growing for it.
	//nolint:gosec // i ≤ len(rec): binary.Uvarint never reports more bytes than it was given
	if count == 0 || count > uint64(len(rec)-i) {
		return dst, 0, fmt.Errorf("id count %d exceeds %d remaining bytes", count, len(rec)-i)
	}
	// Grow once, off the exact decoded count, so the shared accumulator does
	// not re-grow per record. count is bounded by the remaining bytes above,
	// so it cannot force a large allocation from a small record.
	dst = slices.Grow(dst, int(count)) //nolint:gosec // count ≤ len(rec)-i, an int
	var prev uint64
	for j := range count {
		v, k := binary.Uvarint(rec[i:])
		if k <= 0 {
			return dst, 0, errors.New("bad event-id uvarint")
		}
		i += k
		// Reject the raw varint before accumulating: a crafted delta near
		// 2^64 would wrap prev+v back under MaxUint32 and smuggle a
		// non-ascending ID past the overflow check (postings are untrusted).
		if v > math.MaxUint32 {
			return dst, 0, fmt.Errorf("delta %d overflows uint32", v)
		}
		abs := v
		if j > 0 {
			if v == 0 {
				return dst, 0, errors.New("zero delta")
			}
			abs = prev + v
		}
		if abs > math.MaxUint32 {
			return dst, 0, fmt.Errorf("event id %d overflows uint32", abs)
		}
		dst = append(dst, uint32(abs))
		prev = abs
	}
	return dst, i, nil
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

// The postings codec (AppendTermPostings / TermPostingsLen / DecodeAscendingIDs)
// lives in runspill, the innermost package that needs it — this package and
// runspill share one body, imported downward, so the run files and the packed
// index rows cannot drift apart. These delegates keep this package's API.

// AppendTermPostings appends ONE term's record (term ‖ uvarint count ‖
// delta-varint ascending ids) to dst.
func AppendTermPostings(dst []byte, term TermKey, ids []uint32) []byte {
	return runspill.AppendTermPostings(dst, term, ids)
}

// TermPostingsLen returns the encoded byte length AppendTermPostings would
// produce for ids without encoding.
func TermPostingsLen(ids []uint32) int { return runspill.TermPostingsLen(ids) }

// AppendPostings appends one posting list (uvarint count, then delta-varint
// ascending ids) to dst — the term-less half of AppendTermPostings, for
// consumers that already know which term they are reading.
func AppendPostings(dst []byte, ids []uint32) []byte { return runspill.AppendPostings(dst, ids) }

// DecodeAscendingIDs decodes count delta-varint ids via next, validating
// ascending order and uint32 range (postings are untrusted input).
func DecodeAscendingIDs(next func() (uint64, error), count uint64, ids []uint32) ([]uint32, error) {
	return runspill.DecodeAscendingIDs(next, count, ids)
}
