package txhash

// hot_rows.go — the packed per-ledger row codec for the hot tier (design:
// ~/bench-artifacts/txhash-onerow-design.md): ONE txhash-CF row per ledger,
// key rocksdb.EncodeUint32(seq), value = the ledger's full 32-byte tx hashes
// (fee-bump outer AND inner), sorted ascending bytewise, concatenated. A row
// exists for EVERY ledger — empty value for tx-less ones — so the CF itself
// is a dense chain: a missing sequence is corruption, never ambiguity.

import (
	"bytes"
	"fmt"
	"slices"
)

const (
	// rowHashLen is the row stride: full 32-byte hashes, never truncated.
	// Hot indexes are exact; truncation happens exactly once, at the cold
	// .bin emit (cold_freeze.go).
	rowHashLen = 32

	// maxRowHashes caps one row's hash count at a u16's range. Real ledgers
	// carry a few thousand transactions at the extreme; a row claiming more
	// is corrupt input, rejected before it can reach a batch (write) or the
	// window (warmup).
	maxRowHashes = 65535
)

// EncodeRow packs one ledger's tx hashes into a row value, sorting hashes in
// place. An intra-ledger duplicate is a loud reject: the write path feeds
// this pre-batch, so corrupt input fails the ledger before it can touch the
// commit batch. The same hash in DIFFERENT ledgers is legal and stored
// verbatim — the cross-ledger fee-bump shape. Empty input encodes the legal
// empty row.
func EncodeRow(hashes [][32]byte) ([]byte, error) {
	if len(hashes) > maxRowHashes {
		return nil, fmt.Errorf("txhash: ledger has %d hashes (max %d)", len(hashes), maxRowHashes)
	}
	slices.SortFunc(hashes, func(a, b [32]byte) int { return bytes.Compare(a[:], b[:]) })
	row := make([]byte, 0, len(hashes)*rowHashLen)
	var prev [32]byte
	for i, h := range hashes {
		if i > 0 && h == prev {
			return nil, fmt.Errorf("txhash: duplicate hash %x within one ledger", h)
		}
		prev = h
		row = append(row, h[:]...)
	}
	return row, nil
}

// validateRow re-checks a row value read back from the CF — 32-byte
// alignment, strictly ascending hashes, the encode-side count bound — and
// returns its hash count. Warmup's tail replay is the caller: ApplyRow
// trusts its input because both feeding paths (EncodeRow at write time,
// validateRow at warmup) have already enforced this shape.
func validateRow(row []byte) (int, error) {
	if len(row)%rowHashLen != 0 {
		return 0, fmt.Errorf("txhash: row length %d is not a multiple of %d", len(row), rowHashLen)
	}
	n := len(row) / rowHashLen
	if n > maxRowHashes {
		return 0, fmt.Errorf("txhash: row has %d hashes (max %d)", n, maxRowHashes)
	}
	for i := 1; i < n; i++ {
		prev, cur := row[(i-1)*rowHashLen:i*rowHashLen], row[i*rowHashLen:(i+1)*rowHashLen]
		if bytes.Compare(prev, cur) >= 0 {
			return 0, fmt.Errorf("txhash: row hashes not strictly ascending at record %d", i)
		}
	}
	return n, nil
}
