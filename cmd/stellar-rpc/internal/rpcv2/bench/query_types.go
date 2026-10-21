package bench

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"strconv"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
)

// newQueryRequest builds one query type's request. Each request is one call to
// the daemon's method handler and returns how many items came back. For txhash
// and events it first builds the type's pool and records it in settings.
func newQueryRequest(
	ctx context.Context, logger *supportlog.Entry, ds *queryDataset, p queryPlan, qtype string,
	settings map[string]string,
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
		settings["txhashPoolHashes"] = strconv.Itoa(len(pool.hashes))
		return txHashRequest(ds, p, pool), nil
	case queryTypeEvents:
		pool, err := buildEventFilterPool(ctx, logger, ds, p.Seed)
		if err != nil {
			return nil, err
		}
		settings["eventsPool"] = pool.labels()
		return eventsRequest(ds, p, pool), nil
	default:
		return nil, fmt.Errorf("unknown query type %q", qtype)
	}
}

// ledgersRequest calls getLedgers for --ledgers-limit ledgers from a random
// start in the dataset's range. A response with fewer ledgers than the limit,
// cut at the dataset's last ledger, fails the request, because getLedgers skips
// a ledger its store does not hold.
func ledgersRequest(ds *queryDataset, p queryPlan) queryRequest {
	return func(ctx context.Context, rng *rand.Rand) (requestTiming, error) {
		start := ds.pickStart(rng, p.LedgersLimit)
		timing, err := ds.server.call(ctx, protocol.GetLedgersMethodName, protocol.GetLedgersRequest{
			StartLedger: start,
			Pagination:  &protocol.LedgerPaginationOptions{Limit: uint(p.LedgersLimit)},
			Format:      p.Format,
		})
		if err != nil {
			return requestTiming{}, err
		}
		if want := min(p.LedgersLimit, ds.LastLedger-start+1); timing.items != int(want) {
			return requestTiming{}, fmt.Errorf("getLedgers returned %d of the %d ledgers from %d",
				timing.items, want, start)
		}
		return timing, nil
	}
}

// txPageRequest calls getTransactions for a page of --txpage-limit
// transactions from a random start in the dataset's range.
func txPageRequest(ds *queryDataset, p queryPlan) queryRequest {
	return func(ctx context.Context, rng *rand.Rand) (requestTiming, error) {
		return ds.server.call(ctx, protocol.GetTransactionsMethodName, protocol.GetTransactionsRequest{
			StartLedger: ds.pickStart(rng, 1),
			Pagination:  &protocol.LedgerPaginationOptions{Limit: uint(p.TxPageLimit)},
			Format:      p.Format,
		})
	}
}

// txHashRequest calls getTransaction for a hash from the pool. A response
// whose status differs from the pool's expectation fails the request.
func txHashRequest(ds *queryDataset, p queryPlan, pool *txHashPool) queryRequest {
	return func(ctx context.Context, rng *rand.Rand) (requestTiming, error) {
		hash, wantFound := pool.pick(rng)
		timing, err := ds.server.call(ctx, protocol.GetTransactionMethodName, protocol.GetTransactionRequest{
			Hash:   hex.EncodeToString(hash[:]),
			Format: p.Format,
		})
		if err != nil {
			return requestTiming{}, err
		}
		found := timing.items == 1
		if found != wantFound {
			return requestTiming{}, fmt.Errorf("transaction %x: found=%t, expected %t", hash, found, wantFound)
		}
		timing.outcome = outcomeNotFound
		if found {
			timing.outcome = outcomeFound
		}
		return timing, nil
	}
}

// eventsRequest calls queryEvents for one page of at most --events-limit events
// from a random start ledger to the end of the dataset's range, under a filter
// set from the pool whose label is the request's outcome. A no_match request
// that returns events fails.
func eventsRequest(ds *queryDataset, p queryPlan, pool *eventFilterPool) queryRequest {
	limit := uint(p.EventsLimit)
	return func(ctx context.Context, rng *rand.Rand) (requestTiming, error) {
		start := ds.pickStart(rng, 1)
		set := pool.pick(rng)
		timing, err := ds.server.call(ctx, protocol.QueryEventsMethodName, protocol.QueryEventsRequest{
			MinLedger: start,
			MaxLedger: ds.LastLedger,
			Filters:   set.filters,
			Limit:     &limit,
			Format:    p.Format,
		})
		if err != nil {
			return requestTiming{}, err
		}
		if set.outcome == outcomeNoMatch && timing.items > 0 {
			return requestTiming{}, fmt.Errorf("%s filter %+v returned %d events from ledger %d",
				outcomeNoMatch.label(), set.filters[0], timing.items, start)
		}
		timing.outcome = set.outcome
		return timing, nil
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
