package bench

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"

	sdkingest "github.com/stellar/go-stellar-sdk/ingest"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/adapters"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

// newQueryRequest builds one query type's request. For txhash and events it
// first builds the type's pool under ctx, outside every timer, and records the
// pool in p.Settings. Each request takes its own read view inside the timer and
// returns how many items came back.
func newQueryRequest(
	ctx context.Context, logger *supportlog.Entry, ds *queryDataset, p queryPlan, qtype string,
) (queryRequest, error) {
	switch qtype {
	case queryTypeLedgers:
		return ledgersRequest(ds, p), nil
	case queryTypeTxPage:
		return txPageRequest(ds, p), nil
	case queryTypeTxHash:
		pool, err := buildTxHashPool(ctx, logger, ds, p.NotFoundFraction, p.Seed, p.TxHashPoolSize)
		if err != nil {
			return nil, err
		}
		p.Settings["txhashPoolHashes"] = strconv.Itoa(len(pool.hashes))
		p.Settings["txhashPoolLedgers"] = strconv.Itoa(pool.ledgerCount)
		return txHashRequest(ds, pool), nil
	case queryTypeEvents:
		pool, err := buildEventFilterPool(ctx, logger, ds)
		if err != nil {
			return nil, err
		}
		p.Settings["eventsPool"] = pool.kind()
		return eventsRequest(ds, p, pool), nil
	default:
		// Unreachable: parseQueryTypes rejects anything else.
		return nil, fmt.Errorf("unknown query type %q", qtype)
	}
}

// ledgersRequest measures getLedgers' read of --ledgers-span ledgers from a
// random start in the dataset's range. A read that returns fewer ledgers than
// the range holds fails the request.
func ledgersRequest(ds *queryDataset, p queryPlan) queryRequest {
	return func(ctx context.Context, rng *rand.Rand) (requestTiming, error) {
		lo, hi := ds.pickRange(rng, p.LedgersSpan)
		return timed(func() (int, error) {
			read := 0
			err := ds.readLedgers(ctx, lo, hi, func(seq uint32, raw []byte) (bool, error) {
				// The read must have materialized the ledger. Only its length is
				// read; the measured work is the read, not a decode of the bytes.
				if len(raw) == 0 {
					return false, fmt.Errorf("ledger %d has zero bytes", seq)
				}
				read++
				return true, nil
			})
			if err != nil {
				return 0, err
			}
			// A gap in the dataset must not report a fast success.
			if want := int(hi-lo) + 1; read != want {
				return 0, fmt.Errorf("read %d of the %d ledgers in [%d, %d]", read, want, lo, hi)
			}
			return read, nil
		})
	}
}

// txPageRequest measures getTransactions' read: read --txpage-span ledgers and
// materialize each one's transactions, envelopes included, up to
// --txpage-limit. The read ends at the ledger that fills the page.
func txPageRequest(ds *queryDataset, p queryPlan) queryRequest {
	return func(ctx context.Context, rng *rand.Rand) (requestTiming, error) {
		lo, hi := ds.pickRange(rng, p.TxPageSpan)
		return timed(func() (int, error) {
			txs := 0
			err := ds.readLedgers(ctx, lo, hi, func(seq uint32, raw []byte) (bool, error) {
				// Every byte field of a view aliases the borrowed ledger bytes, so
				// no view outlives this call.
				views, err := sdkingest.LedgerTransactionViewRange(
					xdr.LedgerCloseMetaView(raw), 0, p.TxPageLimit-txs, ds.Passphrase)
				if err != nil {
					return false, fmt.Errorf("materialize transactions of ledger %d: %w", seq, err)
				}
				txs += len(views)
				return txs < p.TxPageLimit, nil
			})
			if err != nil {
				return 0, err
			}
			return txs, nil
		})
	}
}

// txHashRequest measures getTransaction's read through
// adapters.TransactionReader: hot tx-hash indexes, then the cold window
// indexes, each candidate verified against its ledger. A lookup whose result
// differs from the pool's expectation fails the request. The reader is
// stateless; one serves every request.
func txHashRequest(ds *queryDataset, pool *txHashPool) queryRequest {
	reader := adapters.NewTransactionReader(ds.Passphrase, nil)
	return func(ctx context.Context, rng *rand.Rand) (requestTiming, error) {
		hash, wantFound := pool.pick(rng)
		t, err := timed(func() (int, error) {
			view, err := ds.view()
			if err != nil {
				return 0, fmt.Errorf("acquire read view: %w", err)
			}
			defer view.Release()

			_, err = reader.GetTransaction(query.WithView(ctx, view), xdr.Hash(hash))
			found := err == nil
			if errors.Is(err, store.ErrNoTransaction) {
				err = nil
			}
			if err != nil {
				return 0, fmt.Errorf("look up transaction %x: %w", hash, err)
			}
			if found != wantFound {
				return 0, fmt.Errorf("transaction %x: found=%t, expected %t", hash, found, wantFound)
			}
			if found {
				return 1, nil
			}
			return 0, nil
		})
		if err != nil {
			return requestTiming{}, err
		}
		t.outcome = outcomeFound
		if !wantFound {
			t.outcome = outcomeNotFound
		}
		return t, nil
	}
}

// eventsRequest measures getEvents' read: one page of at most --events-limit
// events from a random start ledger to the end of the dataset's range, under a
// filter set from the pool. An empty page is not an error.
func eventsRequest(ds *queryDataset, p queryPlan, pool *eventFilterPool) queryRequest {
	return func(ctx context.Context, rng *rand.Rand) (requestTiming, error) {
		filters := pool.pick(rng)
		lo := ds.pickStart(rng, 1)
		hi := ds.LastLedger
		cursor := query.EventCursor{Scope: query.EventScope{
			MinLedger: lo,
			MaxLedger: &hi,
			Dir:       query.Ascending,
			Filters:   filters,
		}}
		return timed(func() (int, error) {
			view, err := ds.view()
			if err != nil {
				return 0, fmt.Errorf("acquire read view: %w", err)
			}
			defer view.Release()

			page, err := view.QueryEvents(ctx, cursor, p.EventsLimit)
			if err != nil {
				return 0, fmt.Errorf("query events over [%d, %d]: %w", lo, hi, err)
			}
			return len(page.Events), nil
		})
	}
}

// pickStart returns a random first ledger for a span-long read inside the
// dataset's range. A span wider than the range returns the range's start.
func (ds *queryDataset) pickStart(rng *rand.Rand, span uint32) uint32 {
	room := ds.LastLedger - ds.FirstLedger + 1
	if span >= room {
		return ds.FirstLedger
	}
	return ds.FirstLedger + uint32(rng.IntN(int(room-span+1))) //nolint:gosec // room fits a chunk range
}

// pickRange returns a random span-long ledger range inside the dataset's
// range. The end is clamped to the dataset's last ledger, which a hot dataset
// sets below the registry's latest when --sample-ledgers narrows the range.
func (ds *queryDataset) pickRange(rng *rand.Rand, span uint32) (uint32, uint32) {
	lo := ds.pickStart(rng, span)
	return lo, min(lo+span-1, ds.LastLedger)
}

// readLedgers acquires a read view and calls fn with each ledger of [lo, hi],
// ascending, until fn returns false. One ledger goes through
// ReadView.WithLedger, the daemon's point read; a wider range goes through
// ReadView.ScanLedgers. The bytes are borrowed: fn must not keep them after it
// returns. A point read that returns no ledger is an error, and the read stops
// with ctx.Err() once ctx is done.
func (ds *queryDataset) readLedgers(
	ctx context.Context, lo, hi uint32, fn func(seq uint32, raw []byte) (bool, error),
) error {
	view, err := ds.view()
	if err != nil {
		return fmt.Errorf("acquire read view: %w", err)
	}
	defer view.Release()

	visit := func(seq uint32, raw []byte) (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		return fn(seq, raw)
	}
	if lo == hi {
		visited := false
		var visitErr error
		err := view.WithLedger(lo, func(raw []byte) error {
			visited = true
			_, visitErr = visit(lo, raw)
			return visitErr
		})
		switch {
		case visitErr != nil:
			return visitErr
		case err != nil:
			return fmt.Errorf("read ledger %d: %w", lo, err)
		case !visited:
			return fmt.Errorf("read ledger %d: no ledger returned", lo)
		}
		return nil
	}
	scan, err := view.ScanLedgers(lo, hi)
	if err != nil {
		return fmt.Errorf("scan ledgers [%d, %d]: %w", lo, hi, err)
	}
	for entry, serr := range scan {
		if serr != nil {
			return fmt.Errorf("scan ledgers [%d, %d]: %w", lo, hi, serr)
		}
		more, err := visit(entry.Seq, entry.Bytes)
		if err != nil || !more {
			return err
		}
	}
	return nil
}
