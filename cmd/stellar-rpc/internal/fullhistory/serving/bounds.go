package serving

import (
	"fmt"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
)

// Direction is a range request's scan direction.
type Direction int

const (
	Ascending  Direction = iota // results begin at the low edge and rise
	Descending                  // results begin at the high edge and fall
)

// RangeError reports a request whose leading edge falls below the admitted
// retention floor. It carries the available range so the handler can report it,
// matching v1's out-of-range behavior. Silently clamping is wrong here: it would
// drop the first results the caller asked for.
type RangeError struct {
	Requested uint32 // the leading-edge ledger that fell below the floor
	Oldest    uint32 // oldest servable ledger in the admitted range
	Latest    uint32 // newest servable ledger in the admitted range
}

func (e *RangeError) Error() string {
	return fmt.Sprintf(
		"serving: ledger %d is below the retention floor; available range is [%d, %d]",
		e.Requested, e.Oldest, e.Latest)
}

// OldestLedger is the oldest ledger this request may serve: the first ledger of
// the admitted retention-floor chunk.
func (a *Admission) OldestLedger() uint32 { return a.floor.FirstLedger() }

// ClampRange validates a request's leading edge against the admitted floor and
// clamps its trailing edge into the admitted range [OldestLedger, Latest]. The
// leading edge — where results begin, lo for ascending and hi for descending —
// below the oldest servable ledger is rejected with *RangeError (not clamped).
// The trailing edge is truncated: an ascending scan stops at latest, a descending
// scan terminates at the floor. It returns the clamped [lo, hi]; lo > hi means
// the request lies entirely beyond latest, so there is nothing to serve yet.
func (a *Admission) ClampRange(dir Direction, lo, hi uint32) (uint32, uint32, error) {
	oldest, latest := a.OldestLedger(), a.latest

	leading := lo
	if dir == Descending {
		leading = hi
	}
	if leading < oldest {
		return 0, 0, &RangeError{Requested: leading, Oldest: oldest, Latest: latest}
	}

	if hi > latest {
		hi = latest // truncate beyond the tip
	}
	if lo < oldest {
		lo = oldest // terminate at the floor
	}
	return lo, hi, nil
}

// ChunksForRange clamps the requested ledger range (ClampRange) and returns the
// chunks overlapping the clamped range in scan order — the sequence a range query
// walks, resolving each chunk to its serving store. A leading edge below the floor
// is rejected with *RangeError; an empty result means the request lies beyond
// latest (nothing to serve yet). Each chunk belongs to exactly one serving store
// per path, so multi-chunk results are concatenated, not merged.
func (a *Admission) ChunksForRange(dir Direction, lo, hi uint32) ([]chunk.ID, error) {
	lo, hi, err := a.ClampRange(dir, lo, hi)
	if err != nil {
		return nil, err
	}
	if lo > hi {
		return nil, nil // entirely beyond latest
	}
	return chunkSeq(chunk.IDFromLedger(lo), chunk.IDFromLedger(hi), dir), nil
}

// chunkSeq returns the inclusive chunk ids from first..last in scan order
// (first <= last). Descending counts down without underflowing at chunk 0.
func chunkSeq(first, last chunk.ID, dir Direction) []chunk.ID {
	out := make([]chunk.ID, 0, int(last-first)+1)
	if dir == Descending {
		for c := last; ; c-- {
			out = append(out, c)
			if c == first {
				break
			}
		}
		return out
	}
	for c := first; c <= last; c++ {
		out = append(out, c)
	}
	return out
}
