package query

// The cross-chunk getEvents pager. QueryEvents advances a cursor by
// one page. The cursor carries the request's pinned Scope (bounds,
// direction, filters) and two bookmarks: Position, the last event
// delivered while its ledger is only partly served, and ScannedLedger,
// the last ledger fully covered. A page re-enters at Position when one
// is present and resumes past ScannedLedger otherwise, walks the range
// chunk by chunk through event.Matches, and returns the events plus
// the advanced cursor, ready to encode. The server keeps nothing
// between pages.

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/eventquery"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event"
)

// QueryEvents advances cursor by one page of at most limit events.
// Errors: ErrCursorMalformed for a malformed cursor, ErrInvertedRange
// for an inverted scope, ErrInvalidLimit for a non-positive limit,
// *RangeError for an ascending resume point below the view's
// retention floor (descending reports OldestReached instead), and
// ErrPositionMismatch for a Position no stored event matches.
// Anything else is a store failure.
func (a *ReadView) QueryEvents(ctx context.Context, cursor EventCursor, limit int) (*EventPage, error) {
	return a.queryEvents(ctx, cursor, nil, limit)
}

// EventID names an event by identity alone, without the stored ordinal an
// EventPosition carries: the four fields a getEvents v1 cursor spells out.
type EventID struct{ Ledger, Tx, Op, Event uint32 }

// atOrAfter reports whether (tx, op, ev) sorts at or after e's own triple,
// in the lexicographic order the event ids themselves sort by.
func (e *EventID) atOrAfter(tx, op, ev uint32) bool {
	if tx != e.Tx {
		return tx > e.Tx
	}
	if op != e.Op {
		return op > e.Op
	}
	return ev >= e.Event
}

// resumeAt is the walk's re-entry point in its first chunk: either a
// position carrying the stored ordinal, which a v2 cursor round-trips, or a
// bare event id, which is all a v1 cursor names. At most one is set.
type resumeAt struct {
	pos  *EventPosition
	from *EventID
}

// QueryEventsFrom serves one page of scope starting at the event id from,
// inclusive; a nil from starts at the scope's low edge. It serves an API
// whose cursor names an event by identity rather than by stored position,
// so the walk seeks the id's own ledger for the matching boundary instead
// of being handed an ordinal. Inclusive is what v1's "id >= cursor" scan
// promises. Ascending only, the one direction v1 has.
//
// The returned page carries no Next cursor: a caller that names events by
// id mints its own, and the bookmarks this walk advances are not a cursor
// the pager would take back.
func (a *ReadView) QueryEventsFrom(
	ctx context.Context, scope EventScope, from *EventID, limit int,
) (*EventPage, error) {
	if scope.Dir != Ascending {
		return nil, errors.New("query: resuming from an event id needs an ascending scope")
	}
	page, err := a.queryEvents(ctx, EventCursor{Scope: scope}, from, limit)
	if err != nil {
		return nil, err
	}
	page.Next = EventCursor{}
	return page, nil
}

func (a *ReadView) queryEvents(
	ctx context.Context, cursor EventCursor, from *EventID, limit int,
) (*EventPage, error) {
	if err := eventquery.ValidateCursor(&cursor, limit); err != nil {
		return nil, err
	}
	desc := cursor.Scope.Dir == Descending

	lo, hi, reenter := eventquery.ResumeBounds(&cursor)
	if from != nil {
		if from.Ledger < cursor.Scope.MinLedger ||
			(cursor.Scope.MaxLedger != nil && from.Ledger > *cursor.Scope.MaxLedger) {
			return nil, fmt.Errorf("%w: resume id ledger %d outside scope bounds",
				ErrCursorMalformed, from.Ledger)
		}
		lo = max(lo, from.Ledger)
	}
	if lo > hi {
		// Resume moved past the scope's far bound: nothing is left to serve.
		return &EventPage{Next: cursor, Status: ScanComplete}, nil
	}
	lo, hi, outcome, err := a.ClampRange(cursor.Scope.Dir, lo, hi)
	if err != nil {
		return nil, err
	}
	switch outcome {
	case RangeBeyondLatest:
		return &EventPage{Next: cursor, Status: ScanWaitingForLedgers}, nil
	case RangeBelowFloor:
		// Descending only: the remaining range is below the floor. Per
		// the proposal a descending scan never errors out of range; it
		// reports OldestReached, and a re-pull reports it again.
		return &EventPage{Next: cursor, Status: ScanOldestReached}, nil
	case RangeServe:
	}
	// Bound the page's scan window. The window keeps the leading edge
	// (the resume point) and gives up the far end; a truncated page is
	// never terminal, so the next page continues.
	window := a.maxScanLedgers
	if window == 0 {
		window = MaxScanLedgers
	}
	lo, hi, truncated := eventquery.ClipScanWindow(lo, hi, window, desc)

	chunks := chunksBetween(chunk.IDFromLedger(lo), chunk.IDFromLedger(hi), cursor.Scope.Dir)
	resume := resumeAt{pos: reenter, from: from}
	walk, err := a.walkChunks(ctx, chunks, lo, hi, cursor.Scope.Filters, resume, desc, limit)
	if err != nil {
		return nil, err
	}
	return a.assemblePage(cursor, walk, lo, hi, truncated), nil
}

// eventPart is one chunk's slice of a clamped range: the reader plus
// the intersected ledger bounds, the input shape scanChunk consumes.
type eventPart struct {
	Chunk    chunk.ID
	Reader   event.Reader
	From, To uint32
}

// walkChunks scans the clamped range chunk by chunk. Each chunk's
// reader is resolved only when the walk reaches it: a page usually
// fills within the first chunk, so at the default scan window (at
// most two chunks) the second often goes unopened.
func (a *ReadView) walkChunks(
	ctx context.Context, chunks []chunk.ID, lo, hi uint32, filters []event.Filter,
	resume resumeAt, desc bool, limit int,
) (WalkResult, error) {
	var walk WalkResult
	for i, c := range chunks {
		if i > 0 {
			resume = resumeAt{} // the resume point is always in the first chunk
		}
		r, err := a.Events(c)
		if err != nil {
			return WalkResult{}, err
		}
		part := eventPart{
			Chunk: c, Reader: r,
			From: max(lo, c.FirstLedger()), To: min(hi, c.LastLedger()),
		}
		res, err := scanChunk(ctx, part, filters, resume, desc, limit-len(walk.Events))
		if err != nil {
			return WalkResult{}, err
		}
		walk.Events = append(walk.Events, res.events...)
		if res.last != nil {
			walk.Last = res.last
		}
		if res.nextUnserved != nil {
			walk.NextUnserved = res.nextUnserved
			return walk, nil
		}
		if res.coveredThrough != nil {
			walk.CoveredThrough = res.coveredThrough
		}
		if len(walk.Events) >= limit && i < len(chunks)-1 {
			return walk, nil // full exactly at a chunk boundary, range remaining
		}
	}
	walk.Finished = true
	return walk, nil
}

// chunkResult is one chunk's contribution to a page.
type chunkResult struct {
	events []event.Payload
	last   *EventPosition
	// nextUnserved is set when the page filled while the stream still
	// had matches: the ledger of the first match the client has not
	// seen. The next page serves it.
	nextUnserved *uint32
	// coveredThrough is the chunk's far ledger in walk order, set when
	// the stream ended: every candidate in the chunk's window was
	// checked, even if nothing was delivered. Nil only when the page
	// filled first (nextUnserved is set instead).
	coveredThrough *uint32
}

func scanChunk(
	ctx context.Context, part eventPart, filters []event.Filter,
	resume resumeAt, desc bool, room int,
) (chunkResult, error) {
	ofs, err := part.Reader.Offsets()
	if err != nil {
		return chunkResult{}, err
	}
	// The offsets snapshot must cover the walk's whole window. Backfill
	// is chunk-aligned and latest never exceeds a serving chunk's
	// ingested range, so a shortfall on either side is a store bug;
	// clipping would let the walk continue past never-scanned ledgers
	// and claim them covered.
	end := ofs.EndLedger()
	if end == ofs.StartLedger() || part.From < ofs.StartLedger() || part.To > end-1 {
		return chunkResult{}, fmt.Errorf(
			"query: chunk %s offsets cover [%d, %d] but the walk needs [%d, %d]",
			part.Chunk, ofs.StartLedger(), end-1, part.From, part.To)
	}
	window, err := chunkWindow(ctx, part.Reader, ofs, part.From, part.To, resume, desc)
	if err != nil {
		return chunkResult{}, err
	}

	var out chunkResult
	// room+1: the walk reads one match past the room to learn
	// nextUnserved, so a page that fills is one right-sized fetch.
	for m, merr := range event.Matches(ctx, part.Reader, filters, window, desc, room+1) {
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
	// Stream ended: every candidate in [part.From, part.To] was checked,
	// so the chunk's far ledger is fully covered even when nothing was
	// delivered.
	covered := part.To
	if desc {
		covered = part.From
	}
	out.coveredThrough = &covered
	return out, nil
}

// chunkWindow translates [pLo, pHi] to ordinals and clips past the
// re-entry point. Either form of re-entry is inside [pLo, pHi]:
// QueryEvents refuses to serve when a bookmark is above the view's
// latest, and resumeBounds starts the window at its ledger.
func chunkWindow(
	ctx context.Context, r event.Reader, ofs *event.LedgerOffsets,
	pLo, pHi uint32, resume resumeAt, desc bool,
) (event.IDRange, error) {
	window, err := event.IDRangeForLedgers(ofs, pLo, pHi)
	if err != nil {
		return event.IDRange{}, err
	}
	if from := resume.from; from != nil {
		// An id names no ordinal, so seek its ledger for the first one at
		// or after it. Clipping here rather than dropping matches later
		// keeps the page's cost independent of how deep the id sits.
		ord, err := seekOrdinal(ctx, r, ofs, from)
		if err != nil {
			return event.IDRange{}, err
		}
		window.Start = max(window.Start, ord)
		return window, nil
	}
	if resume.pos == nil {
		return window, nil
	}
	ord, err := resumeOrdinal(ctx, r, ofs, resume.pos)
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

// seekOrdinal returns the first ordinal in from's ledger whose id is at or
// after from, and the ledger's end when none is, which empties that
// ledger's share of the window. Order inside a ledger is id order, so this
// is a binary search: each probe reads one event, and unlike a resume from
// a position there is nothing to verify afterwards, since an id is a
// boundary the client asked for rather than a slot it claims exists.
func seekOrdinal(
	ctx context.Context, r event.Reader, ofs *event.LedgerOffsets, from *EventID,
) (uint32, error) {
	lStart, lEnd, err := ofs.EventIDs(from.Ledger)
	if err != nil {
		return 0, fmt.Errorf("query: resume ledger %d: %w", from.Ledger, err)
	}
	// sort.Search wants a predicate that is false then true across the
	// range, which "at or after from" is. Its probe cannot report an error,
	// so a failed read is kept aside and answered true to end the search;
	// the index it returns is discarded on that path.
	var ferr error
	i := sort.Search(int(lEnd-lStart), func(i int) bool {
		if ferr != nil {
			return true
		}
		got, err := r.FetchEvents(ctx, []uint32{lStart + uint32(i)}) //nolint:gosec // i is within the ledger
		if err != nil {
			ferr = err
			return true
		}
		p := &got[0]
		return from.atOrAfter(p.TxIdx, p.OpIdx, p.EventIdx)
	})
	if ferr != nil {
		return 0, fmt.Errorf("query: resume seek: %w", ferr)
	}
	return lStart + uint32(i), nil //nolint:gosec // i is within the ledger
}

// resumeOrdinal returns the re-entry position's chunk-relative
// ordinal after checking it: fetch the claimed slot, compare the
// identity fields. Any disagreement is ErrPositionMismatch.
// Within-ledger order is deterministic (close-meta stream order,
// fixed inclusion rule), so a mismatch is a wrong cursor, not
// something to recover from.
func resumeOrdinal(
	ctx context.Context, r event.Reader, ofs *event.LedgerOffsets, pos *EventPosition,
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
func (a *ReadView) assemblePage(
	cursor EventCursor, walk WalkResult, clo, chi uint32, truncated bool,
) *EventPage {
	return eventquery.AssemblePage(cursor, walk, clo, chi, truncated, a.OldestLedger(), a.LatestLedger())
}
