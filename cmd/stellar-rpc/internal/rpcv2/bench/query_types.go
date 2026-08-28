package bench

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"

	sdkingest "github.com/stellar/go-stellar-sdk/ingest"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/adapters"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

// This file holds the four measured request bodies, one per query type. Each
// one models a served endpoint: it takes its own read view, issues the read
// through query.ReadView, and returns how many items came back. None of them
// touches a store reader that ReadView did not hand over, so the p99-campaign
// refactor of the readers moves these numbers without moving this code.

// Sub-stage labels for the tx-hash distribution. The blended total_r<rate> row
// is what the results converter reads; these two additionally split it, since a
// hit and a miss are different amounts of work and a blended p99 hides which
// one moved (see recordLeg).
const (
	txHashStageFound = "found"
	txHashStageMiss  = "miss"
)

// newQueryRequest builds one query type's measured request, sampling whatever
// corpus it needs first. The corpus build reads the dataset and is deliberately
// outside every timer; the returned closure is what a leg measures.
func newQueryRequest(
	ctx context.Context, logger *supportlog.Entry, f *queryFixture, p queryPlan, qtype string,
) (queryRequest, error) {
	switch qtype {
	case queryTypeLedgers:
		return ledgersRequest(f, p), nil
	case queryTypeTxPage:
		return txPageRequest(f, p), nil
	case queryTypeTxHash:
		corpus, err := buildTxHashCorpus(ctx, logger, f, p.MissFraction, p.Seed)
		if err != nil {
			return nil, err
		}
		return txHashRequest(ctx, f, corpus), nil
	case queryTypeEvents:
		corpus, err := buildEventFilterCorpus(ctx, logger, f)
		if err != nil {
			return nil, err
		}
		return eventsRequest(ctx, f, p, corpus), nil
	default:
		// Unreachable: parseQueryTypes rejects anything else.
		return nil, fmt.Errorf("unknown query type %q", qtype)
	}
}

// ledgersRequest measures getLedgers' read: one range scan of --ledgers-span
// ledgers from a random point in the fixture's range, through
// ReadView.ScanLedgers.
//
// The span is a flag because a point read is simply a span of one, and a leg
// measures one shape at a time, so its percentiles describe a single
// distribution. The default is the endpoint's page cap, which is what a client
// actually asks for.
func ledgersRequest(f *queryFixture, p queryPlan) queryRequest {
	return func(rng *rand.Rand) (cellSample, error) {
		lo := f.pickStart(rng, p.LedgersSpan)
		hi := lo + p.LedgersSpan - 1
		return timed("", func() (int, error) {
			view, err := f.view()
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
				// Touch the borrowed bytes so the read is not optimized into a
				// seek: a served request decodes them.
				if len(entry.Bytes) == 0 {
					return 0, fmt.Errorf("ledger %d decoded to zero bytes", entry.Seq)
				}
				read++
			}
			return read, nil
		})
	}
}

// txPageRequest measures getTransactions' read: walk --txpage-span ledgers and
// materialize each one's transactions in apply order, stopping at
// --txpage-limit transactions the way a page does.
//
// It materializes the full transaction views the endpoint serves, envelopes
// included, which costs a TxSet re-hash per transaction. Each ledger's
// transactions are counted inside the loop body, because ScanLedgers lends its
// ledger bytes only until the iterator steps and every byte field of a
// transaction view aliases them.
func txPageRequest(f *queryFixture, p queryPlan) queryRequest {
	return func(rng *rand.Rand) (cellSample, error) {
		lo := f.pickStart(rng, p.TxPageSpan)
		hi := lo + p.TxPageSpan - 1
		return timed("", func() (int, error) {
			view, err := f.view()
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
				remaining := p.TxPageLimit - txs
				if remaining <= 0 {
					break
				}
				views, verr := sdkingest.LedgerTransactionViewRange(
					xdr.LedgerCloseMetaView(entry.Bytes), 0, remaining, f.Passphrase)
				if verr != nil {
					return 0, fmt.Errorf("materialize transactions of ledger %d: %w", entry.Seq, verr)
				}
				txs += len(views)
			}
			return txs, nil
		})
	}
}

// txHashRequest measures getTransaction's read: the served by-hash path,
// adapters.TransactionReader. It probes the view's hot tx-hash indexes, then
// its cold window indexes, verifies each candidate by reading the candidate's
// ledger, and copies the transaction's bytes out of that ledger buffer the way
// the endpoint does.
//
// The view owns both index tiers, and the query package tests them. It hands
// over each index already gated to the servable ledger range, so an index hit
// below the retention floor reads as not-found instead of as a failed ledger
// read, and it opens a cold index on that index's first probe, so a hot hit
// pays no file open.
//
// The reader is stateless, so one serves every request of a leg.
func txHashRequest(ctx context.Context, f *queryFixture, corpus *txHashCorpus) queryRequest {
	reader := adapters.NewTransactionReader(f.Passphrase, nil)
	return func(rng *rand.Rand) (cellSample, error) {
		hash, wantFound := corpus.pick(rng)
		stage := txHashStageFound
		if !wantFound {
			stage = txHashStageMiss
		}
		return timed(stage, func() (int, error) {
			view, err := f.view()
			if err != nil {
				return 0, fmt.Errorf("acquire read view: %w", err)
			}
			defer view.Release()

			_, err = reader.GetTransaction(adapters.WithView(ctx, view), xdr.Hash(hash))
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
	}
}

// eventsRequest measures getEvents' read: one page of at most --events-limit
// events over the fixture's ledger range, under a filter set drawn from the
// corpus.
//
// One page, not a full drain: a page is what the endpoint serves, and a drain
// would fold an unbounded number of them into one sample. A page can come back
// empty and still be real work — the engine bounds each page's scan window, so a
// filter matching nothing walks the window and returns nothing — which is why
// the sample's item count can be zero without being an error.
func eventsRequest(
	ctx context.Context, f *queryFixture, p queryPlan, corpus *eventFilterCorpus,
) queryRequest {
	return func(rng *rand.Rand) (cellSample, error) {
		filters := corpus.pick(rng)
		hi := f.LastLedger
		cursor := query.EventCursor{Scope: query.EventScope{
			MinLedger: f.FirstLedger,
			MaxLedger: &hi,
			Dir:       query.Ascending,
			Filters:   filters,
		}}
		return timed("", func() (int, error) {
			view, err := f.view()
			if err != nil {
				return 0, fmt.Errorf("acquire read view: %w", err)
			}
			defer view.Release()

			page, err := view.QueryEvents(ctx, cursor, p.EventsLimit)
			if err != nil {
				return 0, fmt.Errorf("query events over [%d, %d]: %w", f.FirstLedger, hi, err)
			}
			return len(page.Events), nil
		})
	}
}

// pickStart returns a random first ledger for a span-long read that stays
// inside the fixture's servable range. A span wider than the range is clamped
// to the range's start, so a small fixture still reads rather than erroring.
func (f *queryFixture) pickStart(rng *rand.Rand, span uint32) uint32 {
	room := f.LastLedger - f.FirstLedger + 1
	if span >= room {
		return f.FirstLedger
	}
	return f.FirstLedger + uint32(rng.IntN(int(room-span+1))) //nolint:gosec // room fits a chunk range
}
