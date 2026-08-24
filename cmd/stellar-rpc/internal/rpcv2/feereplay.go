package rpcv2

import (
	"fmt"

	sdkingest "github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/feewindow"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
)

// replayFeeWindows rebuilds the in-memory getFeeStats windows on startup
// (issue #888): the windows are memory-only, so each run() refills them by
// replaying the last max(classic, soroban) committed ledgers (≤1000) before
// the hot loop — which resumes at lastCommitted+1 — starts feeding the same
// windows. run() calls this BEFORE launching the loop; running them
// concurrently would let the loop append a ledger the replay also covers.
//
// The replay is the fees-only consumer of the shared walk: raw bytes from the
// query router's read view → ExtractLedgerTxParts → FeesFromTxParts, events
// never computed. It deliberately does NOT go through store.LedgerReader /
// StreamLedgerViewFn — that seam exists for the JSON-RPC handlers and runs
// against a request's read view; the replay walks the registry's read view
// directly and feeds the raw bytes straight to the fee extractor.
func replayFeeWindows(registry *query.Registry, windows *feewindow.FeeWindows, lastCommitted uint32) error {
	if windows == nil {
		return nil // no fee consumer (tests) → nothing to refill
	}
	// The replay recomputes the full window from committed history, so start
	// from empty regardless of the windows' current state — appending on top
	// would double-count the overlap (and trip the windows' ledger-contiguity
	// check). This keeps the function correct on any input, not just the
	// freshly built windows run() hands it.
	windows.Reset()

	view, err := registry.NewReadView()
	if err != nil {
		return fmt.Errorf("acquire read view: %w", err)
	}
	defer view.Release()

	// Clamp to what exists: on a fresh start (or a history shorter than the
	// window) OldestLedger() can exceed lastCommitted by one — a no-op, not an
	// error. The int64 math sidesteps uint32 underflow when the window exceeds
	// lastCommitted.
	window := int64(windows.MaxRetentionWindow())
	latest := int64(lastCommitted)
	start := max(latest-window+1, int64(view.OldestLedger()))
	if start > latest {
		return nil
	}

	//nolint:gosec // start ∈ [OldestLedger, latest], both uint32
	iter, err := view.ScanLedgers(uint32(start), lastCommitted)
	if err != nil {
		return fmt.Errorf("scan ledgers [%d, %d]: %w", start, lastCommitted, err)
	}
	for entry, serr := range iter {
		if serr != nil {
			return fmt.Errorf("replay ledger stream: %w", serr)
		}
		// entry.Bytes is BORROWED — it aliases the reader's scratch buffer and is
		// overwritten on the next iteration — so the fees are folded right here in
		// the loop body; only the plain uint64 fee slices are retained.
		lcmView := xdr.LedgerCloseMetaView(entry.Bytes)
		txParts, perr := sdkingest.ExtractLedgerTxParts(lcmView)
		if perr != nil {
			return fmt.Errorf("extract ledger tx parts seq %d: %w", entry.Seq, perr)
		}
		fees, ferr := sdkingest.FeesFromTxParts(txParts)
		if ferr != nil {
			return fmt.Errorf("classify fees seq %d: %w", entry.Seq, ferr)
		}
		if aerr := windows.AppendLedgerFees(entry.Seq, fees); aerr != nil {
			return fmt.Errorf("append fees seq %d: %w", entry.Seq, aerr)
		}
	}
	return nil
}
