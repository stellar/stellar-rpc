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
// (methods.LedgerScanLimit, the same 10,000) keeps requests inside this window.
const walkSpanCap = 10_000

// LedgerReader satisfies store.LedgerReader over the query router. Every
// method reads through the request's shared view (see WithSharedView), or its
// own when the context carries none; NewTx returns a handle that keeps one
// view across calls until Done.
type LedgerReader struct {
	registry *query.Registry
}

func NewLedgerReader(registry *query.Registry) *LedgerReader {
	return &LedgerReader{registry: registry}
}

func (r *LedgerReader) GetLatestLedgerSequence(ctx context.Context) (uint32, error) {
	view, release, err := acquireView(ctx, r.registry)
	if err != nil {
		return 0, markErr(ctx, err)
	}
	defer release()
	if view.OldestLedger() > view.LatestLedger() {
		return 0, markErr(ctx, store.ErrEmptyDB)
	}
	return view.LatestLedger(), nil
}

func (r *LedgerReader) GetLedger(ctx context.Context, sequence uint32) (xdr.LedgerCloseMeta, bool, error) {
	view, release, err := acquireView(ctx, r.registry)
	if err != nil {
		return xdr.LedgerCloseMeta{}, false, markErr(ctx, err)
	}
	defer release()
	lcm, found, err := getLedger(view, sequence)
	return lcm, found, markErr(ctx, err)
}

func (r *LedgerReader) GetLedgerRange(ctx context.Context) (store.LedgerRange, error) {
	view, release, err := acquireView(ctx, r.registry)
	if err != nil {
		return store.LedgerRange{}, markErr(ctx, err)
	}
	defer release()
	lr, err := getLedgerRange(view, r.registry)
	return lr, markErr(ctx, err)
}

func (r *LedgerReader) StreamLedgerRange(
	ctx context.Context, startLedger, endLedger uint32, f store.StreamLedgerFn,
) error {
	view, release, err := acquireView(ctx, r.registry)
	if err != nil {
		return markErr(ctx, err)
	}
	defer release()

	scan, err := view.ScanLedgers(startLedger, endLedger)
	if err != nil {
		return markErr(ctx, err)
	}
	for entry, err := range scan {
		if err != nil {
			return markErr(ctx, err)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var lcm xdr.LedgerCloseMeta
		if err := lcm.UnmarshalBinary(entry.Bytes); err != nil {
			return markErr(ctx, fmt.Errorf("adapters: unmarshal ledger %d: %w", entry.Seq, err))
		}
		if err := f(lcm); err != nil {
			return err
		}
	}
	return nil
}

func (r *LedgerReader) NewTx(ctx context.Context) (store.LedgerReaderTx, error) {
	view, release, err := acquireView(ctx, r.registry)
	if err != nil {
		return nil, markErr(ctx, err)
	}
	return &ledgerReaderTx{view: view, release: release, registry: r.registry}, nil
}

// ledgerReaderTx satisfies store.LedgerReaderTx over one read view. GetLedger
// serves getTransactions' ascending, contiguous per-ledger walk by pulling from
// a single ScanLedgers iterator primed on the first call; GetLedgerRange and
// BatchGetLedgers read through the same view but never touch that iterator.
type ledgerReaderTx struct {
	view    *query.ReadView
	release func()
	// registry outlives the view; GetLedgerRange writes the oldest-close-time
	// cache through it.
	registry *query.Registry

	// next/stop are the pull ends of the walk iterator; nil until the first
	// GetLedger primes them.
	next func() (ledger.Entry, error, bool)
	stop func()
}

func (tx *ledgerReaderTx) GetLedger(ctx context.Context, sequence uint32) (xdr.LedgerCloseMeta, bool, error) {
	// The request duration limiter answers the client at the deadline but only
	// abandons the handler goroutine — it cannot stop it. Without this check
	// the abandoned getTransactions walk would keep decoding the rest of its
	// primed span (up to a whole chunk) while holding its read view, and the
	// deletion grace margin is sized assuming walks stop within one iteration
	// of their deadline.
	if err := ctx.Err(); err != nil {
		return xdr.LedgerCloseMeta{}, false, markErr(ctx, err)
	}
	// chunk.IDFromLedger panics below ledger 2, and sequence comes from a
	// client-supplied cursor, so the guard is load-bearing, not defensive.
	if sequence < chunk.FirstLedgerSeq {
		return xdr.LedgerCloseMeta{}, false, nil
	}
	// ClampRange is the only place the servable window is enforced and no
	// point-read path calls it, so gate here: without this a view acquired
	// between ingestion's commit and its SetLatestLedger could return a ledger
	// above the view's frozen latest.
	if !inWindow(tx.view, sequence) {
		return xdr.LedgerCloseMeta{}, false, nil
	}

	if tx.next == nil {
		scan, err := tx.view.ScanLedgers(sequence, min(tx.view.LatestLedger(), sequence+walkSpanCap))
		if err != nil {
			return xdr.LedgerCloseMeta{}, false, markErr(ctx, err)
		}
		tx.next, tx.stop = iter.Pull2(scan)
	}

	entry, err, ok := tx.next()
	if err != nil {
		return xdr.LedgerCloseMeta{}, false, markErr(ctx, err)
	}
	if !ok {
		// The iterator ran dry: the caller walked sequentially but past the span
		// primed above. getTransactions' span cap (methods.LedgerScanLimit) keeps
		// its walks inside the span, so reaching this means a new caller without
		// that cap. Fail loudly rather than serve a wrong-position read.
		return xdr.LedgerCloseMeta{}, false, markErr(ctx, fmt.Errorf(
			"adapters: ledger walk exhausted its primed %d-ledger span at ledger %d"+
				" — the calling handler must cap the request's ledger range",
			walkSpanCap, sequence))
	}
	if entry.Seq != sequence {
		// The walk contract (ascending, contiguous from the priming sequence)
		// was broken. Fail loudly rather than serve the wrong ledger's data.
		return xdr.LedgerCloseMeta{}, false, markErr(ctx, fmt.Errorf(
			"adapters: non-sequential GetLedger: asked for ledger %d, the walk is at ledger %d",
			sequence, entry.Seq))
	}
	var lcm xdr.LedgerCloseMeta
	if err := lcm.UnmarshalBinary(entry.Bytes); err != nil {
		return xdr.LedgerCloseMeta{}, false, markErr(ctx, fmt.Errorf("adapters: unmarshal ledger %d: %w", sequence, err))
	}
	return lcm, true, nil
}

func (tx *ledgerReaderTx) GetLedgerRange(ctx context.Context) (store.LedgerRange, error) {
	lr, err := getLedgerRange(tx.view, tx.registry)
	return lr, markErr(ctx, err)
}

func (tx *ledgerReaderTx) BatchGetLedgers(
	ctx context.Context, start, end uint32,
) ([]store.LedgerMetadataChunk, error) {
	scan, err := tx.view.ScanLedgers(start, end)
	if err != nil {
		return nil, markErr(ctx, err)
	}
	out := make([]store.LedgerMetadataChunk, 0, end-start+1)
	for entry, err := range scan {
		if err != nil {
			return nil, markErr(ctx, err)
		}
		var lcm xdr.LedgerCloseMeta
		if err := lcm.UnmarshalBinary(entry.Bytes); err != nil {
			return nil, markErr(ctx, fmt.Errorf("adapters: unmarshal ledger %d: %w", entry.Seq, err))
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
	tx.release()
	return nil
}

// inWindow reports whether seq falls inside the view's servable window
// [OldestLedger, LatestLedger] — the one gate every point read must apply.
func inWindow(view *query.ReadView, seq uint32) bool {
	return seq >= view.OldestLedger() && seq <= view.LatestLedger()
}

// getLedger is the one-shot point read: window-gated, then a single
// GetLedgerRaw against the sequence's chunk. A hot-store miss inside the window
// maps to (false, nil), matching v1's absent-ledger shape.
func getLedger(view *query.ReadView, sequence uint32) (xdr.LedgerCloseMeta, bool, error) {
	if sequence < chunk.FirstLedgerSeq {
		return xdr.LedgerCloseMeta{}, false, nil
	}
	if !inWindow(view, sequence) {
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

// getLedgerRange reads the window's edge sequences from the view. Close times
// come from the registry's in-memory stamps in the common case — the ingest
// loop stamps the tip on every commit, and the oldest edge is cached after one
// read per floor move (close times are immutable, and the floor moves once per
// chunk). Only a stamp miss pays a point read, of just the close time off the
// raw bytes.
func getLedgerRange(view *query.ReadView, registry *query.Registry) (store.LedgerRange, error) {
	oldest, latest := view.OldestLedger(), view.LatestLedger()
	// Reachable on a genuine first start: with earliest_ledger pinned at a
	// chunk boundary, the last committed ledger is earliest-1, so oldest is
	// latest+1. There is no store-level Empty() helper.
	if oldest > latest {
		return store.LedgerRange{}, store.ErrEmptyDB
	}
	firstCT, ok := view.OldestCloseTime()
	if !ok {
		var err error
		if firstCT, err = readCloseTime(view, oldest, "oldest"); err != nil {
			return store.LedgerRange{}, err
		}
		registry.RecordOldestCloseTime(oldest, firstCT)
	}
	lastCT, ok := view.LatestCloseTime()
	if !ok {
		// Only reachable right after boot (the seed stamp carries no close
		// time). No cache write — the next commit stamps the tip.
		var err error
		if lastCT, err = readCloseTime(view, latest, "latest"); err != nil {
			return store.LedgerRange{}, err
		}
	}
	return store.LedgerRange{
		FirstLedger: store.LedgerInfo{Sequence: oldest, CloseTime: firstCT},
		LastLedger:  store.LedgerInfo{Sequence: latest, CloseTime: lastCT},
	}, nil
}

// readCloseTime point-reads one ledger and decodes only its close time off the
// raw bytes — no full LedgerCloseMeta unmarshal. which names the window edge
// ("oldest"/"latest") in the missing-ledger error.
func readCloseTime(view *query.ReadView, seq uint32, which string) (int64, error) {
	// chunk.IDFromLedger panics below ledger 2; the window edges are always ≥ 2,
	// so this is purely defensive.
	if seq < chunk.FirstLedgerSeq {
		return 0, fmt.Errorf("adapters: %s ledger %d is below genesis", which, seq)
	}
	reader, err := view.Ledgers(chunk.IDFromLedger(seq))
	if err != nil {
		return 0, err
	}
	raw, err := reader.GetLedgerRaw(seq)
	if errors.Is(err, stores.ErrNotFound) {
		return 0, fmt.Errorf("adapters: %s ledger %d missing from its store", which, seq)
	}
	if err != nil {
		return 0, err
	}
	closeTime, err := xdr.LedgerCloseMetaView(raw).LedgerCloseTime()
	if err != nil {
		return 0, fmt.Errorf("adapters: decode close time of ledger %d: %w", seq, err)
	}
	return closeTime, nil
}
