// Package adapters implements the internal/store serving interfaces over the
// v2 query router, so the shared JSON-RPC handlers run unchanged against the
// hot + cold stores.
package adapters

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"iter"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/ledger"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

// walkSpanCap bounds the range primed for LedgerReaderTx.GetLedger's walk. At
// 10,000 ledgers (one chunk) the span covers at most two chunks, so ScanLedgers
// resolves every reader at call time and an unroutable chunk fails before any
// row is returned instead of mid-walk. getTransactions' handler-level span cap
// (#889) keeps requests inside this window.
const walkSpanCap = 10_000

// LedgerReader satisfies store.LedgerReader over the query router. Each method
// takes its own read view; NewTx returns a handle that holds one view across
// calls and releases it on Done.
type LedgerReader struct {
	registry *query.Registry
}

// Compile-time interface check: no handler consumes this type until #889 wires
// the v2 method table, so nothing else would catch a signature drift.
var _ store.LedgerReader = (*LedgerReader)(nil)

func NewLedgerReader(registry *query.Registry) *LedgerReader {
	return &LedgerReader{registry: registry}
}

func (r *LedgerReader) GetLatestLedgerSequence(_ context.Context) (uint32, error) {
	view, err := r.registry.NewReadView()
	if err != nil {
		return 0, err
	}
	defer view.Release()
	if view.OldestLedger() > view.LatestLedger() {
		return 0, store.ErrEmptyDB
	}
	return view.LatestLedger(), nil
}

func (r *LedgerReader) GetLedger(_ context.Context, sequence uint32) (xdr.LedgerCloseMeta, bool, error) {
	view, err := r.registry.NewReadView()
	if err != nil {
		return xdr.LedgerCloseMeta{}, false, err
	}
	defer view.Release()
	return getLedger(view, sequence)
}

func (r *LedgerReader) GetLedgerRange(_ context.Context) (store.LedgerRange, error) {
	view, err := r.registry.NewReadView()
	if err != nil {
		return store.LedgerRange{}, err
	}
	defer view.Release()
	return getLedgerRange(view)
}

func (r *LedgerReader) StreamLedgerRange(
	ctx context.Context, startLedger, endLedger uint32, f store.StreamLedgerFn,
) error {
	view, err := r.registry.NewReadView()
	if err != nil {
		return err
	}
	defer view.Release()

	scan, err := view.ScanLedgers(startLedger, endLedger)
	if err != nil {
		return err
	}
	for entry, err := range scan {
		if err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var lcm xdr.LedgerCloseMeta
		if err := lcm.UnmarshalBinary(entry.Bytes); err != nil {
			return fmt.Errorf("adapters: unmarshal ledger %d: %w", entry.Seq, err)
		}
		if err := f(lcm); err != nil {
			return err
		}
	}
	return nil
}

func (r *LedgerReader) NewTx(_ context.Context) (store.LedgerReaderTx, error) {
	view, err := r.registry.NewReadView()
	if err != nil {
		return nil, err
	}
	return &ledgerReaderTx{view: view}, nil
}

// ledgerReaderTx satisfies store.LedgerReaderTx over one read view. GetLedger
// serves getTransactions' ascending, contiguous per-ledger walk by pulling from
// a single ScanLedgers iterator primed on the first call; GetLedgerRange and
// BatchGetLedgers read through the same view but never touch that iterator.
type ledgerReaderTx struct {
	view *query.ReadView

	// next/stop are the pull ends of the walk iterator; nil until the first
	// GetLedger primes them.
	next func() (ledger.Entry, error, bool)
	stop func()
}

// Compile-time interface check, same reason as LedgerReader's above.
var _ store.LedgerReaderTx = (*ledgerReaderTx)(nil)

func (tx *ledgerReaderTx) GetLedger(_ context.Context, sequence uint32) (xdr.LedgerCloseMeta, bool, error) {
	// chunk.IDFromLedger panics below ledger 2, and sequence comes from a
	// client-supplied cursor, so the guard is load-bearing, not defensive.
	if sequence < chunk.FirstLedgerSeq {
		return xdr.LedgerCloseMeta{}, false, nil
	}
	// ClampRange is the only place the servable window is enforced and no
	// point-read path calls it, so gate here: without this a view acquired
	// between ingestion's commit and its SetLatestLedger could return a ledger
	// above the view's frozen latest.
	if sequence < tx.view.OldestLedger() || sequence > tx.view.LatestLedger() {
		return xdr.LedgerCloseMeta{}, false, nil
	}

	if tx.next == nil {
		scan, err := tx.view.ScanLedgers(sequence, min(tx.view.LatestLedger(), sequence+walkSpanCap))
		if err != nil {
			return xdr.LedgerCloseMeta{}, false, err
		}
		tx.next, tx.stop = iter.Pull2(scan)
	}

	entry, err, ok := tx.next()
	if err != nil {
		return xdr.LedgerCloseMeta{}, false, err
	}
	if !ok || entry.Seq != sequence {
		// The walk contract (ascending, contiguous from the priming sequence)
		// was broken. Fail loudly rather than serve the wrong ledger's data.
		return xdr.LedgerCloseMeta{}, false, fmt.Errorf(
			"adapters: non-sequential GetLedger: asked for ledger %d", sequence)
	}
	var lcm xdr.LedgerCloseMeta
	if err := lcm.UnmarshalBinary(entry.Bytes); err != nil {
		return xdr.LedgerCloseMeta{}, false, fmt.Errorf("adapters: unmarshal ledger %d: %w", sequence, err)
	}
	return lcm, true, nil
}

func (tx *ledgerReaderTx) GetLedgerRange(_ context.Context) (store.LedgerRange, error) {
	return getLedgerRange(tx.view)
}

func (tx *ledgerReaderTx) BatchGetLedgers(
	_ context.Context, start, end uint32,
) ([]store.LedgerMetadataChunk, error) {
	scan, err := tx.view.ScanLedgers(start, end)
	if err != nil {
		return nil, err
	}
	var out []store.LedgerMetadataChunk
	for entry, err := range scan {
		if err != nil {
			return nil, err
		}
		var lcm xdr.LedgerCloseMeta
		if err := lcm.UnmarshalBinary(entry.Bytes); err != nil {
			return nil, fmt.Errorf("adapters: unmarshal ledger %d: %w", entry.Seq, err)
		}
		out = append(out, store.LedgerMetadataChunk{
			Header: lcm.LedgerHeaderHistoryEntry(),
			// Entry.Bytes aliases the reader's scratch buffer and is
			// overwritten on the next iteration step; clone what we keep.
			Lcm: bytes.Clone(entry.Bytes),
		})
	}
	return out, nil
}

func (tx *ledgerReaderTx) Done() error {
	if tx.stop != nil {
		tx.stop()
	}
	tx.view.Release()
	return nil
}

// getLedger is the one-shot point read: window-gated, then a single
// GetLedgerRaw against the sequence's chunk. A hot-store miss inside the window
// maps to (false, nil), matching v1's absent-ledger shape.
func getLedger(view *query.ReadView, sequence uint32) (xdr.LedgerCloseMeta, bool, error) {
	if sequence < chunk.FirstLedgerSeq {
		return xdr.LedgerCloseMeta{}, false, nil
	}
	if sequence < view.OldestLedger() || sequence > view.LatestLedger() {
		return xdr.LedgerCloseMeta{}, false, nil
	}
	reader, err := view.Ledgers(chunk.IDFromLedger(sequence))
	if err != nil {
		return xdr.LedgerCloseMeta{}, false, err
	}
	raw, err := reader.GetLedgerRaw(sequence)
	if errors.Is(err, stores.ErrNotFound) {
		return xdr.LedgerCloseMeta{}, false, nil
	}
	if err != nil {
		return xdr.LedgerCloseMeta{}, false, err
	}
	var lcm xdr.LedgerCloseMeta
	if err := lcm.UnmarshalBinary(raw); err != nil {
		return xdr.LedgerCloseMeta{}, false, fmt.Errorf("adapters: unmarshal ledger %d: %w", sequence, err)
	}
	return lcm, true, nil
}

// getLedgerRange reads the window's edge sequences from the view and their
// close times with two point reads — nothing caches close times.
func getLedgerRange(view *query.ReadView) (store.LedgerRange, error) {
	oldest, latest := view.OldestLedger(), view.LatestLedger()
	// Reachable on a genuine first start: with earliest_ledger pinned at a
	// chunk boundary, the last committed ledger is earliest-1, so oldest is
	// latest+1. There is no store-level Empty() helper.
	if oldest > latest {
		return store.LedgerRange{}, store.ErrEmptyDB
	}
	first, ok, err := getLedger(view, oldest)
	if err != nil {
		return store.LedgerRange{}, err
	}
	if !ok {
		return store.LedgerRange{}, fmt.Errorf("adapters: oldest ledger %d missing from its store", oldest)
	}
	last, ok, err := getLedger(view, latest)
	if err != nil {
		return store.LedgerRange{}, err
	}
	if !ok {
		return store.LedgerRange{}, fmt.Errorf("adapters: latest ledger %d missing from its store", latest)
	}
	return store.LedgerRange{
		FirstLedger: store.LedgerInfo{Sequence: oldest, CloseTime: first.LedgerCloseTime()},
		LastLedger:  store.LedgerInfo{Sequence: latest, CloseTime: last.LedgerCloseTime()},
	}, nil
}
