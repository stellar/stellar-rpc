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

// walkSpanCap bounds the ledger span ledgerReaderTx.GetLedger primes its walk
// iterator with: one chunk's worth of ledgers, touching at most two chunks
// when the span straddles a boundary, so view.ScanLedgers can resolve every
// reader up front. Handler scan limits (methods.LedgerScanLimit) must stay
// ≤ this cap; the pairing test enforces it.
const walkSpanCap = chunk.LedgersPerChunk

// LedgerReader satisfies store.LedgerReader over the query router. Every
// method reads through the request's read view (see WithView); NewTx returns
// a handle that runs its walk against that same view until Done.
type LedgerReader struct{}

func NewLedgerReader() *LedgerReader {
	return &LedgerReader{}
}

func (r *LedgerReader) GetLatestLedgerSequence(ctx context.Context) (uint32, error) {
	view, err := query.ViewFrom(ctx)
	if err != nil {
		return 0, err
	}
	if view.OldestLedger() > view.LatestLedger() {
		return 0, store.ErrEmptyDB
	}
	return view.LatestLedger(), nil
}

func (r *LedgerReader) GetLedger(ctx context.Context, sequence uint32) (xdr.LedgerCloseMeta, bool, error) {
	view, err := query.ViewFrom(ctx)
	if err != nil {
		return xdr.LedgerCloseMeta{}, false, err
	}
	lcm, found, err := getLedger(view, sequence)
	return lcm, found, err
}

// WithLedgerRaw lends the ledger's raw bytes with no copy: the routed point
// read lends the tier's buffer, whose validity ends with fn — exactly the
// loan's terms.
func (r *LedgerReader) WithLedgerRaw(ctx context.Context, sequence uint32, fn store.WithLedgerRawFn) (bool, error) {
	view, err := query.ViewFrom(ctx)
	if err != nil {
		return false, err
	}
	if !inWindow(view, sequence) {
		return false, nil
	}
	found := false
	err = view.WithLedger(sequence, func(raw []byte) error {
		found = true
		return fn(raw)
	})
	if !found && errors.Is(err, stores.ErrNotFound) {
		return false, nil
	}
	return found, err
}

func (r *LedgerReader) GetLedgerRange(ctx context.Context) (store.LedgerRange, error) {
	view, err := query.ViewFrom(ctx)
	if err != nil {
		return store.LedgerRange{}, err
	}
	lr, err := getLedgerRange(view)
	return lr, err
}

func (r *LedgerReader) StreamLedgerRange(
	ctx context.Context, startLedger, endLedger uint32, f store.StreamLedgerFn,
) error {
	view, err := query.ViewFrom(ctx)
	if err != nil {
		return err
	}

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

func (r *LedgerReader) NewTx(ctx context.Context) (store.LedgerReaderTx, error) {
	view, err := query.ViewFrom(ctx)
	if err != nil {
		return nil, err
	}
	return &ledgerReaderTx{view: view}, nil
}

// ledgerReaderTx satisfies store.LedgerReaderTx over the request's read view
// (the serving wrapper owns and releases it — Done does not). GetLedger serves
// getTransactions' ascending, contiguous per-ledger walk by pulling from a
// single ScanLedgers iterator primed on the first call; GetLedgerRange and
// BatchGetLedgers read through the same view but never touch that iterator.
type ledgerReaderTx struct {
	view *query.ReadView

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
		return xdr.LedgerCloseMeta{}, false, err
	}
	// ClampRange is the only place the servable window is enforced and no
	// point-read path calls it, so gate here: without this a view acquired
	// between ingestion's commit and its SetLatestLedger could return a ledger
	// above the view's frozen latest.
	if !inWindow(tx.view, sequence) {
		return xdr.LedgerCloseMeta{}, false, nil
	}

	if tx.next == nil {
		// ScanLedgers' end is inclusive; the -1 keeps the span at exactly
		// walkSpanCap ledgers, so a chunk-aligned start opens one chunk
		// reader, not two.
		scan, err := tx.view.ScanLedgers(sequence, min(tx.view.LatestLedger(), sequence+walkSpanCap-1))
		if err != nil {
			return xdr.LedgerCloseMeta{}, false, err
		}
		tx.next, tx.stop = iter.Pull2(scan)
	}

	entry, err, ok := tx.next()
	if err != nil {
		return xdr.LedgerCloseMeta{}, false, err
	}
	if !ok {
		// The iterator ran dry: the caller walked sequentially but past the span
		// primed above. getTransactions' span cap (methods.LedgerScanLimit) keeps
		// its walks inside the span, so reaching this means a new caller without
		// that cap. Fail loudly rather than serve a wrong-position read.
		return xdr.LedgerCloseMeta{}, false, fmt.Errorf(
			"adapters: ledger walk exhausted its primed %d-ledger span at ledger %d"+
				" — the calling handler must cap the request's ledger range",
			walkSpanCap, sequence)
	}
	if entry.Seq != sequence {
		// The walk contract (ascending, contiguous from the priming sequence)
		// was broken. Fail loudly rather than serve the wrong ledger's data.
		return xdr.LedgerCloseMeta{}, false, fmt.Errorf(
			"adapters: non-sequential GetLedger: asked for ledger %d, the walk is at ledger %d",
			sequence, entry.Seq)
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
	ctx context.Context, start, end uint32,
) ([]store.LedgerMetadataChunk, error) {
	scan, err := tx.view.ScanLedgers(start, end)
	if err != nil {
		return nil, err
	}
	out := make([]store.LedgerMetadataChunk, 0, end-start+1)
	for entry, err := range scan {
		if err != nil {
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		headerRaw, err := rawHeaderFromLCMBytes(entry.Bytes)
		if err != nil {
			return nil, fmt.Errorf("adapters: slice ledger %d header: %w", entry.Seq, err)
		}
		// Entry.Bytes aliases the reader's scratch buffer and is overwritten
		// on the next iteration step; clone what we keep.
		out = append(out, store.LedgerMetadataChunk{
			HeaderRaw: bytes.Clone(headerRaw),
			Lcm:       bytes.Clone(entry.Bytes),
		})
	}
	return out, nil
}

// rawHeaderFromLCMBytes slices the marshaled LedgerHeaderHistoryEntry out of
// raw LCM bytes. No decode: the consumer (getLedgers) sends the header and
// the metadata as raw bytes.
func rawHeaderFromLCMBytes(raw []byte) ([]byte, error) {
	view, err := xdr.LedgerCloseMetaView(raw).LedgerHeader()
	if err != nil {
		return nil, err
	}
	return view.Raw()
}

func (tx *ledgerReaderTx) Done() error {
	if tx.stop != nil {
		tx.stop()
	}
	return nil
}

// inWindow reports whether seq falls inside the view's servable window
// [OldestLedger, LatestLedger] — the one gate every point read must apply.
// OldestLedger is always ≥ 2 (the floor sits on a chunk, and chunk 0 starts at
// ledger 2), so this also rejects the sequences chunk.IDFromLedger panics on.
func inWindow(view *query.ReadView, seq uint32) bool {
	return seq >= view.OldestLedger() && seq <= view.LatestLedger()
}

// getLedger is the one-shot point read: window-gated, then one ledger read. A
// hot-store miss inside the window maps to (false, nil), matching v1's
// absent-ledger shape.
func getLedger(view *query.ReadView, sequence uint32) (xdr.LedgerCloseMeta, bool, error) {
	if !inWindow(view, sequence) {
		return xdr.LedgerCloseMeta{}, false, nil
	}
	var lcm xdr.LedgerCloseMeta
	err := view.WithLedger(sequence, func(raw []byte) error {
		if uerr := lcm.UnmarshalBinary(raw); uerr != nil {
			return fmt.Errorf("adapters: unmarshal ledger %d: %w", sequence, uerr)
		}
		return nil
	})
	if errors.Is(err, stores.ErrNotFound) {
		return xdr.LedgerCloseMeta{}, false, nil
	}
	if err != nil {
		return xdr.LedgerCloseMeta{}, false, err
	}
	return lcm, true, nil
}

// getLedgerRange reads the window's edge sequences from the view. Close times
// come from the registry's in-memory stamps in the common case (see the
// Registry's latest and oldest fields); only a stamp miss pays a point read,
// of just the close time off the raw bytes.
func getLedgerRange(view *query.ReadView) (store.LedgerRange, error) {
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
		view.RecordOldestCloseTime(firstCT)
	}
	lastCT, ok := view.LatestCloseTime()
	if !ok {
		// Backstop — SeedCloseTimes stamps the tip before serving begins. No
		// cache write here: the next commit stamps the tip.
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

// readCloseTime is the fallback, not the normal path: reaching a close time
// costs decompressing its ledger, and the registry's stamps answer both window
// edges for every served request. This runs in the boot window before seeding,
// or on the read after the retention floor moves.
//
// which names the window edge ("oldest"/"latest") in the missing-ledger error.
func readCloseTime(view *query.ReadView, seq uint32, which string) (int64, error) {
	var closeTime int64
	err := view.WithLedger(seq, func(raw []byte) error {
		ct, cerr := xdr.LedgerCloseMetaView(raw).LedgerCloseTime()
		if cerr != nil {
			return fmt.Errorf("adapters: decode close time of ledger %d: %w", seq, cerr)
		}
		closeTime = ct
		return nil
	})
	if errors.Is(err, stores.ErrNotFound) {
		return 0, fmt.Errorf("adapters: %s ledger %d missing from its store", which, seq)
	}
	if err != nil {
		return 0, err
	}
	return closeTime, nil
}
