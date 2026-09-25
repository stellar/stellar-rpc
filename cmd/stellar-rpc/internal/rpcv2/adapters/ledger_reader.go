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

// ScanLedgers reads the request's view; unlike a Tx it takes no snapshot of its own.
func (r *LedgerReader) ScanLedgers(ctx context.Context, start, end uint32) iter.Seq2[store.RawLedger, error] {
	view, err := query.ViewFrom(ctx)
	if err != nil {
		return func(yield func(store.RawLedger, error) bool) { yield(store.RawLedger{}, err) }
	}
	return scanView(ctx, view, start, end)
}

func (r *LedgerReader) GetLedgerRange(ctx context.Context) (store.LedgerRange, error) {
	view, err := query.ViewFrom(ctx)
	if err != nil {
		return store.LedgerRange{}, err
	}
	lr, err := getLedgerRange(view)
	return lr, err
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

func (tx *ledgerReaderTx) ScanLedgers(ctx context.Context, start, end uint32) iter.Seq2[store.RawLedger, error] {
	return scanView(ctx, tx.view, start, end)
}

func (tx *ledgerReaderTx) GetLedgerRange(_ context.Context) (store.LedgerRange, error) {
	return getLedgerRange(tx.view)
}

func (tx *ledgerReaderTx) Done() error { return nil }

// scanView yields [start, end] off the view with no copy: RawLedger.Raw
// aliases the tier's buffer until the next step. A scan of one, after clamping,
// is the routed point read, so getLatestLedger's I/O stays a pinned lookup, not a chunk walk;
// a hot-store miss inside the window yields nothing, matching v1's absent shape.
func scanView(ctx context.Context, view *query.ReadView, start, end uint32) iter.Seq2[store.RawLedger, error] {
	return func(yield func(store.RawLedger, error) bool) {
		// The request duration limiter answers the client at the deadline but
		// only abandons the handler goroutine; without these checks an
		// abandoned scan would keep decoding while holding its read view.
		if err := ctx.Err(); err != nil {
			yield(store.RawLedger{}, err)
			return
		}
		// Clamp to the window here rather than let ClampRange answer a start below
		// the floor with a *RangeError: not yielding is the shape the handler
		// turns into v1's InvalidParams naming the caller's ledger.
		start = max(start, view.OldestLedger())
		end = min(end, view.LatestLedger())
		if start > end {
			return
		}
		if start == end {
			yielded := false
			err := view.WithLedger(start, func(raw []byte) error {
				yielded = true
				yield(store.RawLedger{Sequence: start, Raw: raw}, nil)
				return nil
			})
			if err != nil && !yielded && !errors.Is(err, stores.ErrNotFound) {
				yield(store.RawLedger{}, err)
			}
			return
		}
		scan, err := view.ScanLedgers(start, end)
		if err != nil {
			yield(store.RawLedger{}, err)
			return
		}
		for entry, err := range scan {
			if err != nil {
				yield(store.RawLedger{}, err)
				return
			}
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
