package query

import (
	"errors"
	"fmt"
	"slices"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
)

// ErrInvertedRange rejects a request whose low edge exceeds its high edge before
// clamping. It is distinct from the valid empty case (lo > hi after clamping,
// meaning the request lies beyond latest): an inverted input is a malformed
// request, not an empty-but-valid one.
var ErrInvertedRange = errors.New("query: inverted range (lo > hi)")

// Direction is a range request's scan direction.
type Direction int

const (
	Ascending  Direction = iota // results begin at the low edge and rise
	Descending                  // results begin at the high edge and fall
)

// RangeError reports a request whose leading edge falls below the view's
// retention floor. It carries the available range so the handler can report it,
// matching v1's out-of-range behavior. Silently clamping is wrong here: it would
// drop the first results the caller asked for.
type RangeError struct {
	Requested uint32 // the leading-edge ledger that fell below the floor
	Oldest    uint32 // oldest servable ledger in the view's range
	Latest    uint32 // newest servable ledger in the view's range
}

func (e *RangeError) Error() string {
	return fmt.Sprintf(
		"query: ledger %d is below the retention floor; available range is [%d, %d]",
		e.Requested, e.Oldest, e.Latest)
}

// OldestLedger is the oldest ledger this request may serve: the first ledger of
// the view's retention-floor chunk.
func (a *ReadView) OldestLedger() uint32 { return a.floor.FirstLedger() }

// RangeOutcome classifies a clamped range: servable now, or empty for
// one of the two edge reasons a caller reports differently.
type RangeOutcome int

const (
	// RangeServe: the returned [lo, hi] is servable now.
	RangeServe RangeOutcome = iota
	// RangeBeyondLatest: nothing is servable yet. The range lies past
	// the view's latest ledger, or a descending top is above it. That
	// top is never truncated: a descending scan cannot revisit a
	// ledger, so serving below a top this view lacks would skip
	// [latest+1, hi] forever. Callers wait.
	RangeBeyondLatest
	// RangeBelowFloor: a descending range's remaining top is below the
	// view's oldest servable ledger. Per the proposal, a descending
	// scan never gets an out-of-range error: it reports OldestReached.
	// Ascending keeps *RangeError for the same shape, per the same
	// table.
	RangeBelowFloor
)

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
	if lo > hi {
		return 0, 0, RangeServe, fmt.Errorf("%w: [%d, %d]", ErrInvertedRange, lo, hi)
	}
	oldest, latest := a.OldestLedger(), a.latest.seq

	leading := lo
	if dir == Descending {
		leading = hi
	}
	if leading < oldest {
		if dir == Descending {
			return 0, 0, RangeBelowFloor, nil
		}
		return 0, 0, RangeServe, &RangeError{Requested: leading, Oldest: oldest, Latest: latest}
	}

	if hi > latest {
		if dir == Descending {
			return 0, 0, RangeBeyondLatest, nil
		}
		hi = latest // truncate beyond the tip
	}
	if lo > hi {
		return 0, 0, RangeBeyondLatest, nil // ascending start past latest
	}
	if lo < oldest {
		lo = oldest // terminate at the floor
	}
	return lo, hi, RangeServe, nil
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
