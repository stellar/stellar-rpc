package query

// The cross-chunk getEvents pager. QueryEvents advances a cursor by
// one page. The cursor carries the request's pinned Scope (bounds,
// direction, filters) and two bookmarks: Position, the last event
// delivered, and ScannedLedger, the last ledger fully covered. A page
// resumes from whichever bookmark is further along, walks the range
// chunk by chunk through event.Matches, and returns the events plus
// the advanced cursor, ready to encode. The server keeps nothing
// between pages.

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event"
)

// ErrPositionMismatch: the cursor's Position does not identify a
// stored event on this node; the client must restart the query.
var ErrPositionMismatch = errors.New("query: cursor position does not match stored events")

// maxScanLedgers bounds the ledgers one page may scan, so a filter
// that matches nothing cannot walk the node's whole retention in one
// call: the page stops at the window's edge and returns ScanHasMore
// with the watermark advanced through it. At 10,000 ledgers a page
// touches at most two chunks. A var, not a const, so tests can shrink
// it to force window seams; production never writes it. The value may
// become configuration.
//
//nolint:gochecknoglobals // test seam; production never writes it
var maxScanLedgers = uint32(10_000)

// ScanStatus is where a page's walk stopped; the handler maps it to
// the wire scanStatus.
type ScanStatus int

const (
	// ScanHasMore: more range remains. The page filled, the page's
	// scan window ended with scope beyond it, or the walk is waiting:
	// a descending resume point is above this view's latest ledger, so
	// the page is empty and the cursor unchanged.
	ScanHasMore ScanStatus = iota
	// ScanComplete: the walk finished the cursor's bounds within
	// served history.
	ScanComplete
	// ScanWaitingForLedgers: the scope extends past the view's last
	// committed ledger. Either an ascending walk reached it, or the
	// whole scope is still above it.
	ScanWaitingForLedgers
	// ScanOldestReached: a descending walk reached the oldest served
	// ledger, and the scope extends below it.
	ScanOldestReached
)

// EventPage is one page of results: the events in walk order and the
// advanced cursor. Next carries the same Scope with moved bookmarks:
// Position moves only when events were delivered, ScannedLedger only
// when ledgers were covered.
type EventPage struct {
	Events []events.Payload
	Next   EventCursor
	Status ScanStatus
}

// QueryEvents advances cursor by one page of at most limit events.
// Errors: ErrCursorMalformed for a malformed cursor, ErrInvertedRange
// for an inverted scope, a plain error for a non-positive limit,
// *RangeError for a resume point below the view's retention floor,
// and ErrPositionMismatch for a Position no stored event matches.
// Anything else is a store failure.
func (a *ReadView) QueryEvents(ctx context.Context, cursor EventCursor, limit int) (*EventPage, error) {
	if err := validateCursor(&cursor, limit); err != nil {
		return nil, err
	}
	desc := cursor.Scope.Dir == Descending

	lo, hi, reenter := resumeBounds(&cursor)
	if lo > hi {
		// Resume moved past the scope's far bound: nothing is left to serve.
		return &EventPage{Next: cursor, Status: ScanComplete}, nil
	}
	if desc && hi > a.LatestLedger() &&
		(cursor.Position != nil || cursor.ScannedLedger > 0) {
		// A bookmark above this view's latest ledger proves more
		// ledgers exist; another node already served them. A descending
		// walk never goes back up, so serving now would skip those
		// ledgers forever. Wait until this view has them.
		return &EventPage{Next: cursor, Status: ScanHasMore}, nil
	}
	lo, hi, err := a.ClampRange(cursor.Scope.Dir, lo, hi)
	if err != nil {
		return nil, err
	}
	if lo > hi {
		// Beyond latest: nothing to scan yet. Either an ascending
		// resume moved past it, or a fresh scope is entirely above it.
		return &EventPage{Next: cursor, Status: ScanWaitingForLedgers}, nil
	}
	// Bound the page's scan window to maxScanLedgers. The window keeps
	// the leading edge (the resume point) and gives up the far end; a
	// truncated page is never terminal, so the next page continues.
	truncated := hi-lo+1 > maxScanLedgers
	if truncated {
		if desc {
			lo = hi - maxScanLedgers + 1
		} else {
			hi = lo + maxScanLedgers - 1
		}
	}

	chunks, err := a.ChunksForRange(cursor.Scope.Dir, lo, hi)
	if err != nil {
		return nil, err
	}
	walk, err := a.walkChunks(ctx, chunks, lo, hi, cursor.Scope.Filters, reenter, desc, limit)
	if err != nil {
		return nil, err
	}
	return assemblePage(cursor, walk, lo, hi, a.OldestLedger(), a.LatestLedger(), desc, truncated), nil
}

func validateCursor(cursor *EventCursor, limit int) error {
	if limit <= 0 {
		return fmt.Errorf("query: page limit must be positive, got %d", limit)
	}
	if err := validateScope(&cursor.Scope); err != nil {
		return err
	}
	return validateBookmarks(cursor)
}

// validateScope rejects scope shapes the server never mints.
func validateScope(scope *EventCursorQuery) error {
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
	if scope.MinLedger < chunk.FirstLedgerSeq {
		return fmt.Errorf("%w: min ledger %d is below genesis (%d)",
			ErrCursorMalformed, scope.MinLedger, chunk.FirstLedgerSeq)
	}
	// Checked here so a bad cursor fails on every path. Matches also
	// checks, but only when a chunk is scanned; the empty-range and
	// waiting paths scan none.
	if err := event.ValidateFilters(scope.Filters); err != nil {
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
	if mark := cursor.ScannedLedger; mark == math.MaxUint32 ||
		(mark != 0 && mark < cursor.Scope.MinLedger) ||
		(cursor.Scope.MaxLedger != nil && mark > *cursor.Scope.MaxLedger) {
		return fmt.Errorf("%w: scanned ledger %d outside scope bounds",
			ErrCursorMalformed, mark)
	}
	return nil
}

// resumeBounds narrows the scope's bounds by the bookmarks and
// returns the pre-clamp window [lo, hi]. When Position is further
// along than ScannedLedger, its ledger was only partially served: the
// window starts at that ledger and Position is returned for the
// re-entry clip. validateCursor already refused bookmarks outside the
// scope's bounds.
func resumeBounds(cursor *EventCursor) (uint32, uint32, *EventPosition) {
	lo, hi := cursor.Scope.MinLedger, uint32(math.MaxUint32)
	if cursor.Scope.MaxLedger != nil {
		hi = *cursor.Scope.MaxLedger
	}
	pos, mark := cursor.Position, cursor.ScannedLedger
	if cursor.Scope.Dir == Descending {
		switch {
		case pos != nil && (mark == 0 || pos.Ledger < mark):
			return lo, pos.Ledger, pos
		case mark > 0:
			hi = min(hi, mark-1)
		}
		return lo, hi, nil
	}
	switch {
	case pos != nil && pos.Ledger > mark:
		return pos.Ledger, hi, pos
	case mark > 0:
		lo = max(lo, mark+1)
	}
	return lo, hi, nil
}

// walkResult accumulates the chunks' results; see chunkResult for the
// stop-fact fields. finished means every part was consumed.
type walkResult struct {
	events         []events.Payload
	last           *EventPosition
	nextUnserved   *uint32
	coveredThrough *uint32
	finished       bool
}

// walkChunks scans the clamped range chunk by chunk. Each chunk's
// reader is resolved only when the walk reaches it: a page usually
// fills within the first chunk, and the scope can span the node's
// whole retention.
func (a *ReadView) walkChunks(
	ctx context.Context, chunks []chunk.ID, lo, hi uint32, filters []event.Filter,
	reenter *EventPosition, desc bool, limit int,
) (walkResult, error) {
	var walk walkResult
	for i, c := range chunks {
		if i > 0 {
			reenter = nil // the resume point is always in the first chunk
		}
		r, err := a.Events(c)
		if err != nil {
			return walkResult{}, err
		}
		part := EventPart{
			Chunk: c, Reader: r,
			From: max(lo, c.FirstLedger()), To: min(hi, c.LastLedger()),
		}
		res, err := scanChunk(ctx, part, filters, reenter, desc, limit-len(walk.events))
		if err != nil {
			return walkResult{}, err
		}
		walk.events = append(walk.events, res.events...)
		if res.last != nil {
			walk.last = res.last
		}
		if res.nextUnserved != nil {
			walk.nextUnserved = res.nextUnserved
			return walk, nil
		}
		if res.coveredThrough != nil {
			walk.coveredThrough = res.coveredThrough
		}
		if len(walk.events) >= limit && i < len(chunks)-1 {
			return walk, nil // full exactly at a chunk boundary, range remaining
		}
	}
	walk.finished = true
	return walk, nil
}

// chunkResult is one chunk's contribution to a page.
type chunkResult struct {
	events []events.Payload
	last   *EventPosition
	// nextUnserved is set when the page filled while the stream still
	// had matches: the ledger of the first match the client has not
	// seen. The next page serves it.
	nextUnserved *uint32
	// coveredThrough is the chunk's far ledger in walk order, set when
	// the stream ended: every candidate in the chunk's window was
	// checked, even if nothing was delivered. Nil when the window was
	// empty; an empty window claims no coverage.
	coveredThrough *uint32
}

func scanChunk(
	ctx context.Context, part EventPart, filters []event.Filter,
	reenter *EventPosition, desc bool, room int,
) (chunkResult, error) {
	ofs, err := part.Reader.Offsets()
	if err != nil {
		return chunkResult{}, err
	}
	// Clip to the offsets snapshot's coverage. In production this is a
	// no-op: latest never exceeds a serving chunk's ingested range. It
	// keeps test fixtures with partially ingested chunks from turning
	// into IDRangeForLedgers errors.
	end := ofs.EndLedger()
	if end == ofs.StartLedger() {
		return chunkResult{}, nil // nothing ingested yet
	}
	pLo := max(part.From, ofs.StartLedger())
	pHi := min(part.To, end-1)
	if pLo > pHi {
		return chunkResult{}, nil
	}
	window, err := chunkWindow(ctx, part.Reader, ofs, pLo, pHi, reenter, desc)
	if err != nil {
		return chunkResult{}, err
	}

	var out chunkResult
	for m, merr := range event.Matches(ctx, part.Reader, filters, window, desc) {
		if merr != nil {
			return chunkResult{}, merr
		}
		if len(out.events) >= room {
			// The page is full and the next match is known: every
			// ledger strictly before its ledger, in walk order, is
			// fully covered.
			l := m.LedgerSequence
			out.nextUnserved = &l
			return out, nil
		}
		lStart, _, err := ofs.EventIDs(m.LedgerSequence)
		if err != nil {
			return chunkResult{}, fmt.Errorf("query: delivered event's ledger: %w", err)
		}
		out.events = append(out.events, m.Payload)
		out.last = &EventPosition{
			Ledger: m.LedgerSequence, Tx: m.TxIdx, Op: m.OpIdx, Event: m.EventIdx,
			LedgerOrdinal: m.Ordinal - lStart,
		}
	}
	// Stream ended: every candidate in [pLo, pHi] was checked, so the
	// chunk's far ledger is fully covered even when nothing was delivered.
	covered := pHi
	if desc {
		covered = pLo
	}
	out.coveredThrough = &covered
	return out, nil
}

// chunkWindow translates [pLo, pHi] to ordinals and clips past the
// re-entry position. The position is always inside [pLo, pHi]:
// QueryEvents refuses to serve when a bookmark is above the view's
// latest, and resumeBounds starts the window at the position's ledger.
func chunkWindow(
	ctx context.Context, r event.Reader, ofs *events.LedgerOffsets,
	pLo, pHi uint32, reenter *EventPosition, desc bool,
) (event.IDRange, error) {
	window, err := event.IDRangeForLedgers(ofs, pLo, pHi)
	if err != nil {
		return event.IDRange{}, err
	}
	if reenter == nil {
		return window, nil
	}
	ord, err := resumeOrdinal(ctx, r, ofs, reenter)
	if err != nil {
		return event.IDRange{}, err
	}
	if desc {
		window.End = min(window.End, ord)
	} else {
		window.Start = max(window.Start, ord+1)
	}
	return window, nil
}

// resumeOrdinal returns the re-entry position's chunk-relative
// ordinal after checking it: fetch the claimed slot, compare the
// identity fields. Any disagreement is ErrPositionMismatch.
// Within-ledger order is deterministic (close-meta stream order,
// fixed inclusion rule), so a mismatch is a wrong cursor, not
// something to recover from.
func resumeOrdinal(
	ctx context.Context, r event.Reader, ofs *events.LedgerOffsets, pos *EventPosition,
) (uint32, error) {
	lStart, lEnd, err := ofs.EventIDs(pos.Ledger)
	if err != nil {
		return 0, fmt.Errorf("query: resume ledger %d: %w", pos.Ledger, err)
	}
	if uint64(lStart)+uint64(pos.LedgerOrdinal) >= uint64(lEnd) {
		return 0, fmt.Errorf("%w: ledger %d has no stored event at index %d",
			ErrPositionMismatch, pos.Ledger, pos.LedgerOrdinal)
	}
	ord := lStart + pos.LedgerOrdinal
	got, err := r.FetchEvents(ctx, []uint32{ord})
	if err != nil {
		return 0, fmt.Errorf("query: resume position fetch: %w", err)
	}
	p := &got[0]
	if p.LedgerSequence != pos.Ledger || p.TxIdx != pos.Tx ||
		p.OpIdx != pos.Op || p.EventIdx != pos.Event {
		return 0, fmt.Errorf("%w: ledger %d index %d holds (tx %d, op %d, event %d)",
			ErrPositionMismatch, pos.Ledger, pos.LedgerOrdinal, p.TxIdx, p.OpIdx, p.EventIdx)
	}
	return ord, nil
}

// assemblePage is the one place cursor outputs are derived from the
// walk's facts: the advanced bookmarks never regress and the watermark
// never leaves the clamped window [clo, chi].
func assemblePage(
	cursor EventCursor, walk walkResult, clo, chi, oldest, latest uint32, desc, truncated bool,
) *EventPage {
	next := cursor
	if walk.last != nil {
		next.Position = walk.last
	}
	next.ScannedLedger = watermark(&cursor, walk, clo, chi, desc)

	status := ScanHasMore
	if walk.finished && !truncated {
		// A truncated window is never terminal: scope remains beyond it.
		status = terminalStatus(&cursor.Scope, desc, oldest, latest)
	}
	return &EventPage{Events: walk.events, Next: next, Status: status}
}

// watermark derives the last fully covered ledger from the walk's
// stop facts. A stop with a known next match covers every ledger
// strictly before it in walk order. Consumed chunks cover through
// their far ledger. A walk that covered nothing echoes the cursor's
// incoming value.
func watermark(cursor *EventCursor, walk walkResult, clo, chi uint32, desc bool) uint32 {
	switch {
	case walk.nextUnserved != nil && desc:
		if v := *walk.nextUnserved + 1; v <= chi {
			return v
		}
	case walk.nextUnserved != nil:
		// No underflow: a match's ledger is at least genesis (2).
		if v := *walk.nextUnserved - 1; v >= clo {
			return v
		}
	case walk.coveredThrough != nil:
		return *walk.coveredThrough
	}
	return cursor.ScannedLedger
}

// terminalStatus classifies a walk that finished its clamped window:
// either the scope's own bound was reached (complete), or the walk
// stopped at the edge of the node's serving range [oldest, latest].
func terminalStatus(scope *EventCursorQuery, desc bool, oldest, latest uint32) ScanStatus {
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
