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

// ClampRange validates a request's leading edge (where results begin:
// lo ascending, hi descending) and clamps its trailing edge into the
// view's range [OldestLedger, LatestLedger]. A leading edge below the
// oldest servable ledger is rejected with *RangeError, not clamped.
// The trailing edge is truncated: an ascending scan stops at latest, a
// descending scan ends at the floor.
//
// lo > hi in the result means nothing is servable yet: the range lies
// beyond latest, or a descending top is above latest. That top is
// never truncated: a descending scan cannot revisit a ledger, so
// serving below a top this view lacks would skip [latest+1, hi]
// forever. Callers wait instead. An inverted input (lo > hi before
// clamping) is rejected with ErrInvertedRange, so a malformed range is
// never confused with an empty one.
func (a *ReadView) ClampRange(dir Direction, lo, hi uint32) (uint32, uint32, error) {
	if lo > hi {
		return 0, 0, fmt.Errorf("%w: [%d, %d]", ErrInvertedRange, lo, hi)
	}
	oldest, latest := a.OldestLedger(), a.latestLedger

	leading := lo
	if dir == Descending {
		leading = hi
	}
	if leading < oldest {
		return 0, 0, &RangeError{Requested: leading, Oldest: oldest, Latest: latest}
	}

	if hi > latest {
		if dir == Descending {
			return latest + 1, latest, nil // top above latest: wait
		}
		hi = latest // truncate beyond the tip
	}
	if lo < oldest {
		lo = oldest // terminate at the floor
	}
	return lo, hi, nil
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
