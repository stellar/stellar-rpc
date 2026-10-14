package query

import (
	"slices"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/eventquery"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
)

// OldestLedger is the oldest ledger this request may serve: the first ledger of
// the view's retention-floor chunk.
func (a *ReadView) OldestLedger() uint32 { return a.floor.FirstLedger() }

// ClampRange validates a request's leading edge (where results begin:
// lo ascending, hi descending) and clamps its trailing edge into the
// view's range [OldestLedger, LatestLedger]. The returned bounds are
// meaningful only for RangeServe: an ascending scan stops at latest, a
// descending scan ends at the floor. The two empty outcomes are
// documented on RangeOutcome. An ascending leading edge below the
// oldest servable ledger is rejected with *RangeError, and an inverted
// input (lo > hi) with ErrInvertedRange, so a malformed range is never
// confused with an empty one.
func (a *ReadView) ClampRange(dir Direction, lo, hi uint32) (uint32, uint32, RangeOutcome, error) {
	return eventquery.ClampRangeWithin(dir, lo, hi, a.OldestLedger(), a.latest.seq)
}

// chunksBetween returns the inclusive chunk ids from first..last (first <= last)
// in scan order.
func chunksBetween(first, last chunk.ID, dir Direction) []chunk.ID {
	out := make([]chunk.ID, 0, int(last-first)+1)
	for c := first; c <= last; c++ {
		out = append(out, c)
	}
	if dir == Descending {
		slices.Reverse(out)
	}
	return out
}
