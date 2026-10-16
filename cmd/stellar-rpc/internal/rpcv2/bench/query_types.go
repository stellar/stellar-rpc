package bench

import (
	"context"
	"fmt"
	"math/rand/v2"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
)

// newQueryRequest builds one query type's request. Each request is one call to
// the daemon's method handler and returns how many items came back.
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

// ledgersRequest calls getLedgers for --ledgers-limit ledgers from a random
// start in the dataset's range. A response with fewer ledgers than the limit,
// cut at the dataset's last ledger, fails the request, because getLedgers skips
// a ledger its store does not hold.
func ledgersRequest(ds *queryDataset, p queryPlan) queryRequest {
	return func(ctx context.Context, rng *rand.Rand) (requestTiming, error) {
		start := ds.pickStart(rng, p.LedgersLimit)
		var resp protocol.GetLedgersResponse
		timing, err := ds.server.call(ctx, protocol.GetLedgersMethodName, protocol.GetLedgersRequest{
			StartLedger: start,
			Pagination:  &protocol.LedgerPaginationOptions{Limit: uint(p.LedgersLimit)},
			Format:      p.Format,
		}, &resp)
		if err != nil {
			return requestTiming{}, err
		}
		if want := min(p.LedgersLimit, ds.LastLedger-start+1); len(resp.Ledgers) != int(want) {
			return requestTiming{}, fmt.Errorf("getLedgers returned %d of the %d ledgers from %d",
				len(resp.Ledgers), want, start)
		}
		timing.items = len(resp.Ledgers)
		return timing, nil
	}
}

// txPageRequest calls getTransactions for a page of --txpage-limit
// transactions from a random start in the dataset's range.
func txPageRequest(ds *queryDataset, p queryPlan) queryRequest {
	return func(ctx context.Context, rng *rand.Rand) (requestTiming, error) {
		var resp protocol.GetTransactionsResponse
		timing, err := ds.server.call(ctx, protocol.GetTransactionsMethodName, protocol.GetTransactionsRequest{
			StartLedger: ds.pickStart(rng, 1),
			Pagination:  &protocol.LedgerPaginationOptions{Limit: uint(p.TxPageLimit)},
			Format:      p.Format,
		}, &resp)
		if err != nil {
			return requestTiming{}, err
		}
		timing.items = len(resp.Transactions)
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
