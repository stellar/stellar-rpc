// Package adapters implements the internal/store serving interfaces over the
// v2 query router, so the shared JSON-RPC handlers run unchanged against the
// hot + cold stores.
package adapters

import (
	"context"
	"errors"
	"fmt"
	"iter"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

// LedgerReader satisfies store.LedgerReader over the query router. Every
// method reads through the request's read view (see WithView); NewTx returns
// a handle whose scans read that same view until Done.
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

// ledgerReaderTx satisfies store.LedgerReaderTx over the request's read view.
// The serving wrapper owns and releases the view, so Done has nothing to do.
type ledgerReaderTx struct {
	view *query.ReadView
}

// ScanLedgers yields [start, end] straight off the view's scan with no copy:
// RawLedger.Raw aliases the chunk reader's scratch buffer until the next step.
func (tx *ledgerReaderTx) ScanLedgers(
	ctx context.Context, start, end uint32,
) iter.Seq2[store.RawLedger, error] {
	return func(yield func(store.RawLedger, error) bool) {
		// ClampRange answers a start below the floor with a *RangeError and an
		// inverted range with an error; raising start keeps the not-yielded
		// shape, which the handler turns into v1's InvalidParams naming the
		// caller's ledger. A start past latest already scans empty.
		start = max(start, tx.view.OldestLedger())
		if start > end {
			return
		}
		scan, err := tx.view.ScanLedgers(start, end)
		if err != nil {
			yield(store.RawLedger{}, err)
			return
		}
		for entry, err := range scan {
			if err != nil {
				yield(store.RawLedger{}, err)
				return
			}
			// The request duration limiter answers the client at the deadline
			// but only abandons the handler goroutine; without this check an
			// abandoned scan would keep decoding while holding its read view.
			if err := ctx.Err(); err != nil {
				yield(store.RawLedger{}, err)
				return
			}
			if !yield(store.RawLedger{Sequence: entry.Seq, Raw: entry.Bytes}, nil) {
				return
			}
		}
	}
}

func (tx *ledgerReaderTx) GetLedgerRange(_ context.Context) (store.LedgerRange, error) {
	return getLedgerRange(tx.view)
}

func (tx *ledgerReaderTx) Done() error { return nil }

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
