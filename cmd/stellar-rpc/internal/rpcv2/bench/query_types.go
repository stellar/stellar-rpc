package bench

import (
	"context"
	"fmt"
	"math/rand/v2"

	sdkingest "github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// newQueryRequest builds one query type's request. Each request takes its own
// read view inside the timer and returns how many items came back.
func newQueryRequest(ds *queryDataset, p queryPlan, qtype string) (queryRequest, error) {
	switch qtype {
	case queryTypeLedgers:
		return ledgersRequest(ds, p), nil
	case queryTypeTxPage:
		return txPageRequest(ds, p), nil
	default:
		// Unreachable: parseQueryTypes rejects anything else.
		return nil, fmt.Errorf("unknown query type %q", qtype)
	}
}

// ledgersRequest measures getLedgers' read: one ReadView.ScanLedgers over
// --ledgers-span ledgers from a random start in the dataset's range. The scan
// stops with ctx.Err() once ctx is done.
func ledgersRequest(ds *queryDataset, p queryPlan) queryRequest {
	return func(ctx context.Context, rng *rand.Rand) (requestTiming, error) {
		lo, hi := ds.pickRange(rng, p.LedgersSpan)
		return timed(func() (int, error) {
			view, err := ds.view()
			if err != nil {
				return 0, fmt.Errorf("acquire read view: %w", err)
			}
			defer view.Release()

			scan, err := view.ScanLedgers(lo, hi)
			if err != nil {
				return 0, fmt.Errorf("scan ledgers [%d, %d]: %w", lo, hi, err)
			}
			read := 0
			for entry, serr := range scan {
				if serr != nil {
					return 0, fmt.Errorf("scan ledgers [%d, %d]: %w", lo, hi, serr)
				}
				if err := ctx.Err(); err != nil {
					return 0, err
				}
				// The scan must have materialized the ledger. Only its length is
				// read; the measured work is the scan, not a read of the bytes.
				if len(entry.Bytes) == 0 {
					return 0, fmt.Errorf("ledger %d decoded to zero bytes", entry.Seq)
				}
				read++
			}
			return read, nil
		})
	}
}

// txPageRequest measures getTransactions' read: scan --txpage-span ledgers and
// materialize each one's transactions, envelopes included, up to
// --txpage-limit. The scan ends at the ledger that fills the page. ScanLedgers
// lends its ledger bytes until the iterator steps; every byte field of a view
// aliases them, so no view outlives its loop step. The scan stops with
// ctx.Err() once ctx is done.
func txPageRequest(ds *queryDataset, p queryPlan) queryRequest {
	return func(ctx context.Context, rng *rand.Rand) (requestTiming, error) {
		lo, hi := ds.pickRange(rng, p.TxPageSpan)
		return timed(func() (int, error) {
			view, err := ds.view()
			if err != nil {
				return 0, fmt.Errorf("acquire read view: %w", err)
			}
			defer view.Release()

			scan, err := view.ScanLedgers(lo, hi)
			if err != nil {
				return 0, fmt.Errorf("scan ledgers [%d, %d]: %w", lo, hi, err)
			}
			txs := 0
			for entry, serr := range scan {
				if serr != nil {
					return 0, fmt.Errorf("scan ledgers [%d, %d]: %w", lo, hi, serr)
				}
				if err := ctx.Err(); err != nil {
					return 0, err
				}
				views, verr := sdkingest.LedgerTransactionViewRange(
					xdr.LedgerCloseMetaView(entry.Bytes), 0, p.TxPageLimit-txs, ds.Passphrase)
				if verr != nil {
					return 0, fmt.Errorf("materialize transactions of ledger %d: %w", entry.Seq, verr)
				}
				txs += len(views)
				if txs >= p.TxPageLimit {
					break
				}
			}
			return txs, nil
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
