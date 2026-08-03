package event

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event/runspill"
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

// DecodePackedRow parses a packed row, yielding each term's absolute
// event-ID list into add. IDs within a term must be strictly increasing
// (zero deltas rejected); every structural violation is an error — rows are
// untrusted input to warmup and to the cold merge. ids is reused across
// terms; add must consume it before returning (callers copy or fold).
func DecodePackedRow(val []byte, add func(term TermKey, ids []uint32)) error {
	var ids []uint32
	next := func() (uint64, error) {
		v, n := binary.Uvarint(val)
		if n <= 0 {
			return 0, errors.New("bad event-id uvarint")
		}
		val = val[n:]
		return v, nil
	}
	for len(val) > 0 {
		if len(val) < 16 {
			return fmt.Errorf("events: packed row: %d trailing bytes, want 16-byte term", len(val))
		}
		var term TermKey
		copy(term[:], val[:16])
		val = val[16:]
		count, n := binary.Uvarint(val)
		if n <= 0 {
			return errors.New("events: packed row: bad id-count uvarint")
		}
		val = val[n:]
		// Each ID takes ≥1 byte, so a count beyond the remaining bytes is
		// structurally impossible — reject before allocating for it.
		if count == 0 || count > uint64(len(val)) {
			return fmt.Errorf("events: packed row: id count %d exceeds %d remaining bytes", count, len(val))
		}
		var err error
		if ids, err = DecodeAscendingIDs(next, count, ids[:0]); err != nil {
			return fmt.Errorf("events: packed row: %w", err)
		}
		add(term, ids)
	}
	return nil
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

// uvarintLen is the byte width binary.AppendUvarint would use for v.
func uvarintLen(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
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

// DecodeAscendingIDs decodes count delta-varint ids via next, validating
// ascending order and uint32 range (postings are untrusted input).
var DecodeAscendingIDs = runspill.DecodeAscendingIDs
