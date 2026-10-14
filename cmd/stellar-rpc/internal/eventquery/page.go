package eventquery

import (
	"context"
	"errors"
	"fmt"
	"math"
)

// FirstLedgerSeq is genesis, the lowest ledger any scope may name. It
// equals the archival store's chunk.FirstLedgerSeq (asserted in rpcv2/query).
const FirstLedgerSeq = 2

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

// RangeError reports a request whose leading edge falls outside the view's
// range [Oldest, Latest]. Clamping instead would drop the first results the
// caller asked for.
type RangeError struct {
	Requested uint32 // the leading-edge ledger that fell outside the range
	Oldest    uint32 // oldest servable ledger in the view's range
	Latest    uint32 // newest servable ledger in the view's range
}

// Error names the side the ledger fell on. The two sides call for opposite
// client actions: pruned history is gone, a future ledger arrives with time.
func (e *RangeError) Error() string {
	side := "below the retention floor"
	if e.Requested > e.Latest {
		side = "above the latest ledger"
	}
	return fmt.Sprintf("query: ledger %d is %s; available range is [%d, %d]",
		e.Requested, side, e.Oldest, e.Latest)
}

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

// ErrPositionMismatch: the cursor's Position does not identify a
// stored event on this node; the client must restart the query.
var ErrPositionMismatch = errors.New("query: cursor position does not match stored events")

// ErrInvalidLimit rejects a non-positive page limit. Its own sentinel,
// not ErrCursorMalformed: the limit is a per-request parameter, not
// part of the cursor, and the handler maps the two differently.
var ErrInvalidLimit = errors.New("query: page limit must be positive")

// MaxScanLedgers bounds the ledgers one page may scan, so a filter that
// matches nothing cannot walk the node's whole retention in one call:
// the page stops at the window's edge and returns ScanHasMore with the
// watermark advanced through it. The value equals the archival store's
// chunk.LedgersPerChunk, so a page there touches at most two chunks;
// rpcv2/query asserts the two stay equal. Registry.maxScanLedgers shrinks it in tests.
//
// Exported because a handler whose own window must fit inside one page
// has to pin itself against this number, not against the constant it
// happens to derive from.
const MaxScanLedgers uint32 = 10_000

// ScanStatus is where a page's walk stopped; the handler maps it to
// the wire scanStatus.
type ScanStatus int

const (
	// ScanHasMore: more range remains. The page filled, or the page's
	// scan window ended with scope beyond it.
	ScanHasMore ScanStatus = iota
	// ScanComplete: the walk finished the cursor's bounds within
	// served history.
	ScanComplete
	// ScanWaitingForLedgers: the scope needs ledgers past the view's
	// last committed one. An ascending walk reached it, the whole
	// scope is still above it, or a descending scope's top edge is
	// above it: descending never revisits a ledger, so the page waits
	// (empty, cursor unchanged) rather than clamp the top away.
	ScanWaitingForLedgers
	// ScanOldestReached: a descending walk reached the oldest served
	// ledger, and the scope extends below it.
	ScanOldestReached
)

// EventPage is one page of results: the events in walk order and the
// advanced cursor. Next carries the same Scope with moved bookmarks:
// ScannedLedger advances only when ledgers were covered, and Position
// is present only while it leads the watermark (assemblePage drops a
// passed one).
type EventPage struct {
	Events []Payload
	Next   EventCursor
	Status ScanStatus
}

// Querier is the engine behind queryEvents. The handler is engine-neutral:
// the archival daemon serves from the chunk stores through a query.ReadView,
// the rpcv1 daemon from its SQLite event table. Begin opens one request's
// consistent read: bounds and pages come from one snapshot.
type Querier interface {
	Begin(ctx context.Context) (Session, error)
}

// Session is one request's read of the events store. QueryEvents
// returns this package's errors (ErrCursorMalformed, ErrInvertedRange,
// ErrInvalidLimit, *RangeError, ErrPositionMismatch) so the handler can
// classify them; anything else is a store failure.
type Session interface {
	OldestLedger() uint32
	LatestLedger() uint32
	QueryEvents(ctx context.Context, cursor EventCursor, limit int) (*EventPage, error)
	Close() error
}

// ClampRangeWithin validates a request's leading edge (where results begin:
// lo ascending, hi descending) and clamps its trailing edge into the
// serving range [oldest, latest]. The returned bounds are meaningful only
// for RangeServe: an ascending scan stops at latest, a descending scan ends
// at the floor. An ascending leading edge below oldest is rejected with
// *RangeError, and an inverted input (lo > hi) with ErrInvertedRange, so a
// malformed range is never confused with an empty one.
func ClampRangeWithin(dir Direction, lo, hi, oldest, latest uint32) (uint32, uint32, RangeOutcome, error) {
	if lo > hi {
		return 0, 0, RangeServe, fmt.Errorf("%w: [%d, %d]", ErrInvertedRange, lo, hi)
	}

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

// ValidateCursor rejects a malformed cursor (ErrCursorMalformed,
// ErrInvertedRange) or a non-positive limit (ErrInvalidLimit).
func ValidateCursor(cursor *EventCursor, limit int) error {
	if limit <= 0 {
		return fmt.Errorf("%w: got %d", ErrInvalidLimit, limit)
	}
	if err := validateScope(&cursor.Scope); err != nil {
		return err
	}
	return validateBookmarks(cursor)
}

// validateScope rejects scope shapes the server never mints.
func validateScope(scope *EventScope) error {
	switch scope.Dir {
	case Ascending:
	case Descending:
		if scope.MaxLedger == nil {
			return fmt.Errorf("%w: descending scope without a max ledger", ErrCursorMalformed)
		}
	default:
		return fmt.Errorf("%w: invalid direction %d", ErrCursorMalformed, scope.Dir)
	}
	if scope.MaxLedger != nil && scope.MinLedger > *scope.MaxLedger {
		return fmt.Errorf("%w: [%d, %d]", ErrInvertedRange, scope.MinLedger, *scope.MaxLedger)
	}
	// No ledger exists below genesis. A scope that starts below it can
	// never finish: a descending walk would report OldestReached
	// forever. The server never mints such a scope.
	if scope.MinLedger < FirstLedgerSeq {
		return fmt.Errorf("%w: min ledger %d is below genesis (%d)",
			ErrCursorMalformed, scope.MinLedger, FirstLedgerSeq)
	}
	// The codec caps the filter count; a scope above it would do the
	// page's full work and then fail to encode the advanced cursor, an
	// internal error where a client error belongs. Refuse it up front.
	if len(scope.Filters) > MaxCursorFilters {
		return fmt.Errorf("%w: %d filters exceeds the %d-filter cap",
			ErrCursorMalformed, len(scope.Filters), MaxCursorFilters)
	}
	// Checked here so a bad cursor fails on every path. Matches also
	// checks, but only when a chunk is scanned; the empty-range and
	// waiting paths scan none.
	if err := ValidateFilters(scope.Filters); err != nil {
		return fmt.Errorf("%w: %w", ErrCursorMalformed, err)
	}
	return nil
}

// validateBookmarks rejects bookmarks outside the scope's bounds. The
// server mints scope and bookmarks together, always consistent, so a
// bookmark outside them can only come from a forged or corrupted
// cursor. Refuse it rather than guess.
func validateBookmarks(cursor *EventCursor) error {
	if pos := cursor.Position; pos != nil {
		if pos.Ledger < cursor.Scope.MinLedger ||
			(cursor.Scope.MaxLedger != nil && pos.Ledger > *cursor.Scope.MaxLedger) {
			return fmt.Errorf("%w: position ledger %d outside scope bounds",
				ErrCursorMalformed, pos.Ledger)
		}
	}
	// MaxUint32 is rejected for its own reason: no ledger carries that
	// number, and an ascending resume would compute mark+1, which wraps.
	if mark := cursor.ScannedLedger; mark == math.MaxUint32 ||
		(mark != 0 && mark < cursor.Scope.MinLedger) ||
		(cursor.Scope.MaxLedger != nil && mark > *cursor.Scope.MaxLedger) {
		return fmt.Errorf("%w: scanned ledger %d is not a servable ledger",
			ErrCursorMalformed, mark)
	}
	return validateBookmarkPair(cursor)
}

// validateBookmarkPair rejects Position and ScannedLedger combinations
// no walk mints together. assemblePage drops a Position once the
// watermark passes it, so a present Position is exactly the one live
// mid-ledger re-entry point: one ledger past the mark in walk order,
// or the zero-watermark anchor, the first ledger the walk serves
// (MinLedger ascending; MaxLedger descending, which validateScope has
// already guaranteed, so the dereference is safe).
func validateBookmarkPair(cursor *EventCursor) error {
	pos, mark := cursor.Position, cursor.ScannedLedger
	if pos == nil {
		return nil
	}
	var want uint32
	switch {
	case cursor.Scope.Dir == Descending && mark == 0:
		want = *cursor.Scope.MaxLedger
	case cursor.Scope.Dir == Descending:
		want = mark - 1
	case mark == 0:
		want = cursor.Scope.MinLedger
	default:
		want = mark + 1
	}
	if pos.Ledger != want {
		return fmt.Errorf("%w: position ledger %d does not pair with scanned ledger %d",
			ErrCursorMalformed, pos.Ledger, mark)
	}
	return nil
}

// ResumeBounds narrows the scope's bounds by the bookmarks and
// returns the pre-clamp window [lo, hi]. A present Position is the one
// live mid-ledger re-entry point (validateBookmarkPair): the window
// starts at its ledger and Position is returned for the re-entry
// clip. Otherwise the walk resumes just past the watermark.
func ResumeBounds(cursor *EventCursor) (uint32, uint32, *EventPosition) {
	lo, hi := cursor.Scope.MinLedger, uint32(math.MaxUint32)
	if cursor.Scope.MaxLedger != nil {
		hi = *cursor.Scope.MaxLedger
	}
	pos, mark := cursor.Position, cursor.ScannedLedger
	if cursor.Scope.Dir == Descending {
		switch {
		case pos != nil:
			return lo, pos.Ledger, pos
		case mark > 0:
			hi = min(hi, mark-1)
		}
		return lo, hi, nil
	}
	switch {
	case pos != nil:
		return pos.Ledger, hi, pos
	case mark > 0:
		lo = max(lo, mark+1)
	}
	return lo, hi, nil
}

// WalkResult is what a scan reports to AssemblePage. Events are the
// delivered events in walk order and Last the position of the last one.
// NextUnserved is set when the page filled while the scan still had
// matches: the ledger of the first match the client has not seen.
// CoveredThrough is the window's far ledger in walk order, set when the
// scan ran out of candidates: every candidate was checked, even if nothing
// was delivered. Finished means the whole window was consumed.
type WalkResult struct {
	Events         []Payload
	Last           *EventPosition
	NextUnserved   *uint32
	CoveredThrough *uint32
	Finished       bool
}

// ClipScanWindow bounds a page's scan to window ledgers. The window keeps the
// leading edge (the resume point) and gives up the far end; a truncated page
// is never terminal, so the next page continues.
func ClipScanWindow(lo, hi, window uint32, desc bool) (uint32, uint32, bool) {
	truncated := hi-lo+1 > window
	if truncated {
		if desc {
			lo = hi - window + 1
		} else {
			hi = lo + window - 1
		}
	}
	return lo, hi, truncated
}

// AssemblePage derives a page's cursor outputs from a walk's facts for a
// node serving [oldest, latest]. See assemblePage.
func AssemblePage(
	cursor EventCursor, walk WalkResult, clo, chi uint32, truncated bool, oldest, latest uint32,
) *EventPage {
	desc := cursor.Scope.Dir == Descending
	next := cursor
	if walk.Last != nil {
		next.Position = walk.Last
	}
	next.ScannedLedger = watermark(&cursor, walk, clo, chi, desc)
	// Drop a Position the watermark has passed: resumeBounds re-enters
	// only from a leading Position, so a passed one would never be read
	// again. Every cursor carries either a bare watermark or the one
	// live mid-ledger re-entry point.
	if pos := next.Position; pos != nil {
		passed := pos.Ledger <= next.ScannedLedger
		if desc {
			passed = next.ScannedLedger != 0 && pos.Ledger >= next.ScannedLedger
		}
		if passed {
			next.Position = nil
		}
	}

	status := ScanHasMore
	if walk.Finished && !truncated {
		// A truncated window is never terminal: scope remains beyond it.
		status = terminalStatus(&cursor.Scope, desc, oldest, latest)
	}
	return &EventPage{Events: walk.Events, Next: next, Status: status}
}

// watermark derives the last fully covered ledger from the walk's
// stop facts. A stop with a known next match covers every ledger
// strictly before it in walk order. Consumed chunks cover through
// their far ledger. A walk that covered nothing echoes the cursor's
// incoming value.
func watermark(cursor *EventCursor, walk WalkResult, clo, chi uint32, desc bool) uint32 {
	switch {
	case walk.NextUnserved != nil && desc:
		if v := *walk.NextUnserved + 1; v <= chi {
			return v
		}
	case walk.NextUnserved != nil:
		// No underflow: a match's ledger is at least genesis (2).
		if v := *walk.NextUnserved - 1; v >= clo {
			return v
		}
	case walk.CoveredThrough != nil:
		return *walk.CoveredThrough
	}
	return cursor.ScannedLedger
}

// terminalStatus classifies a walk that finished its clamped window:
// either the scope's own bound was reached (complete), or the walk
// stopped at the edge of the node's serving range [oldest, latest].
func terminalStatus(scope *EventScope, desc bool, oldest, latest uint32) ScanStatus {
	if desc {
		if scope.MinLedger >= oldest {
			return ScanComplete
		}
		return ScanOldestReached
	}
	if scope.MaxLedger != nil && *scope.MaxLedger <= latest {
		return ScanComplete
	}
	return ScanWaitingForLedgers
}
