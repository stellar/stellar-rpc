package verify

import (
	"fmt"
	"strconv"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
)

// Mismatch is one disagreement between an artifact and what the chunk's
// ledgers say it should hold. Ledger is 0 and TxHash empty for a chunk-wide
// finding.
type Mismatch struct {
	Ledger   uint32
	TxHash   string
	Artifact string
	Field    string
	Expected string
	Actual   string
}

// ChunkResult is one chunk's outcome. Err is an infrastructure failure that
// stopped the chunk (an unreadable file, a failed archive fetch), distinct
// from a Mismatch, which is a verdict about the data.
type ChunkResult struct {
	Chunk    chunk.ID
	Kinds    []geometry.Kind
	Skipped  string
	Ledgers  uint32
	Txs      uint64
	TxHashes uint64
	Events   uint64
	// IndexChecked is set when every expected tx hash was resolved through
	// the frozen tx-hash index covering the chunk.
	IndexChecked bool
	Mismatches   []Mismatch
	Dropped      int
	Err          error
}

const (
	statusOK       = "ok"
	statusMismatch = "mismatch"
	statusError    = "error"
	statusSkipped  = "skipped"
)

func (r ChunkResult) status() string {
	switch {
	case r.Skipped != "":
		return statusSkipped
	case r.Err != nil:
		return statusError
	case len(r.Mismatches) > 0:
		return statusMismatch
	}
	return statusOK
}

// IndexResult is one frozen tx-hash index coverage's key-count check.
type IndexResult struct {
	Coverage geometry.TxHashIndexCoverage
	Expected uint64
	Actual   uint64
	Skipped  string
}

func (r IndexResult) failed() bool { return r.Skipped == "" && r.Expected != r.Actual }

// Report is one run's outcome.
type Report struct {
	Chunks  []ChunkResult
	Indexes []IndexResult
}

// Failed reports whether any chunk or index check did not come out clean.
func (r *Report) Failed() bool {
	for _, c := range r.Chunks {
		switch c.status() {
		case statusMismatch, statusError:
			return true
		}
	}
	for _, ix := range r.Indexes {
		if ix.failed() {
			return true
		}
	}
	return false
}

// Summary is the one-line outcome for the log.
func (r *Report) Summary() string {
	var ok, mismatched, errored, skipped int
	var ledgers, txs, events uint64
	for _, c := range r.Chunks {
		switch c.status() {
		case statusOK:
			ok++
		case statusMismatch:
			mismatched++
		case statusError:
			errored++
		default:
			skipped++
		}
		ledgers += uint64(c.Ledgers)
		txs += c.Txs
		events += c.Events
	}
	var idxBad int
	for _, ix := range r.Indexes {
		if ix.failed() {
			idxBad++
		}
	}
	return fmt.Sprintf(
		"chunks: %d ok, %d with mismatches, %d errored, %d skipped; "+
			"%d ledgers, %d transactions, %d events checked; %d tx-hash index checks failed",
		ok, mismatched, errored, skipped, ledgers, txs, events, idxBad)
}

// recorder collects one chunk's mismatches up to a cap; the overflow is
// counted so the report says how much was left out.
type recorder struct {
	limit   int
	out     []Mismatch
	dropped int
}

func (r *recorder) add(m Mismatch) {
	if len(r.out) >= r.limit {
		r.dropped++
		return
	}
	r.out = append(r.out, m)
}

func (r *recorder) full() bool { return len(r.out) >= r.limit }

func u32(v uint32) string { return strconv.FormatUint(uint64(v), 10) }

func u64(v uint64) string { return strconv.FormatUint(v, 10) }

func hexHash(h xdr.Hash) string { return h.HexString() }
