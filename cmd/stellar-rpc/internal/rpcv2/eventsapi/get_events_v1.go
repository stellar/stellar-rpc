package eventsapi

// The v1 getEvents endpoint served natively by the v2 pager. The wire
// contract is the shared v1 handler's (internal/methods/get_events.go),
// reproduced choice for choice: the same scan window, the same error codes
// and messages in the same order, a cursor on every response. v1 cursors are
// bare event ids, so the v2 cursor codec never runs; a cursor request is
// translated into the pager's resume state instead.

import (
	"context"
	"fmt"
	"math"

	"github.com/creachadair/jrpc2"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/methods"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/adapters"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

// NewV1Handler builds the v1 getEvents handler. Limits are the getEvents
// method's own knobs, not getEventsV2's: the term budget defaults high
// enough that no legal v1 request is rejected (config.DefaultGetEventsV1TermBudget).
func NewV1Handler(limits Limits) jrpc2.Handler {
	return methods.NewHandler(
		func(ctx context.Context, req protocol.GetEventsRequest) (protocol.GetEventsResponse, error) {
			return getEventsV1(ctx, limits, &req)
		})
}

//nolint:cyclop // the v1 handler's validation order, kept in one place like the original
func getEventsV1(
	ctx context.Context, limits Limits, req *protocol.GetEventsRequest,
) (protocol.GetEventsResponse, error) {
	zero := protocol.GetEventsResponse{}
	if err := req.Valid(limits.MaxLimit); err != nil {
		return zero, &jrpc2.Error{Code: jrpc2.InvalidParams, Message: err.Error()}
	}
	view, err := query.ViewFrom(ctx)
	if err != nil {
		return zero, &jrpc2.Error{Code: jrpc2.InternalError, Message: err.Error()}
	}
	lr, err := adapters.NewLedgerReader().GetLedgerRange(ctx)
	if err != nil {
		return zero, &jrpc2.Error{Code: jrpc2.InternalError, Message: err.Error()}
	}

	start := protocol.Cursor{Ledger: req.StartLedger}
	fromCursor := false
	limit := limits.DefaultLimit
	if req.Pagination != nil {
		if req.Pagination.Cursor != nil {
			start = *req.Pagination.Cursor
			// The item right after the cursor, with the shared handler's
			// exact arithmetic: the increment wraps at MaxUint32, and the
			// scan start is inclusive, so a wrapped cursor re-serves its
			// whole (tx, op) group there and must here.
			start.Event++
			fromCursor = true
		}
		if req.Pagination.Limit > 0 {
			limit = req.Pagination.Limit
		}
	}
	// endLedger is exclusive, per the v1 arithmetic: one scan window past
	// the start, capped at the tip, then the request's own end.
	endLedger := min(start.Ledger+methods.LedgerScanLimit, lr.LastLedger.Sequence+1)
	if req.EndLedger != 0 {
		endLedger = min(req.EndLedger, endLedger)
	}
	if start.Ledger < lr.FirstLedger.Sequence || start.Ledger > lr.LastLedger.Sequence {
		return zero, &jrpc2.Error{
			Code: jrpc2.InvalidRequest,
			Message: fmt.Sprintf("startLedger must be within the ledger range: %d - %d",
				lr.FirstLedger.Sequence, lr.LastLedger.Sequence),
		}
	}
	filters, err := v1Filters(req.Filters)
	if err != nil {
		return zero, &jrpc2.Error{Code: jrpc2.InvalidParams, Message: err.Error()}
	}
	if err := checkTermBudget(filters, limits.TermBudget); err != nil {
		return zero, responseError(err, lr.FirstLedger.Sequence, lr.LastLedger.Sequence)
	}

	// An end at or below the start is legal v1 input and an empty window.
	// Served here: the pager's scopes are inclusive and never inverted.
	if endLedger <= start.Ledger {
		return v1Response(nil, endLedger, limit, lr), nil
	}
	maxLedger := endLedger - 1
	scope := query.EventScope{MinLedger: start.Ledger, MaxLedger: &maxLedger, Filters: filters}
	cursor := query.EventCursor{Scope: scope}
	if fromCursor {
		if cursor, err = v1Resume(ctx, view, start, scope); err != nil {
			return zero, &jrpc2.Error{Code: jrpc2.InvalidRequest, Message: err.Error()}
		}
	}
	pageLimit := int(min(limit, math.MaxInt32)) //nolint:gosec // min clamps it
	page, err := view.QueryEvents(ctx, cursor, pageLimit)
	if err != nil {
		// The v1 handler codes every failure past validation as an invalid
		// request, cancellation included.
		return zero, &jrpc2.Error{Code: jrpc2.InvalidRequest, Message: err.Error()}
	}
	// A short page must mean the scope is done: v1Response's window-end
	// cursor claims everything through endLedger-1 was scanned. That holds
	// because one pager call covers a full v1 window (the pairing test on
	// LedgerScanLimit); if that coupling ever breaks, fail loud here rather
	// than mint a cursor that skips the unscanned remainder.
	if uint(len(page.Events)) < limit &&
		(page.Status == query.ScanHasMore || page.Status == query.ScanWaitingForLedgers) {
		return zero, &jrpc2.Error{
			Code:    jrpc2.InternalError,
			Message: "getEvents: the scan stopped before the request's window was covered",
		}
	}
	events := make([]protocol.EventInfo, 0, len(page.Events))
	for i := range page.Events {
		info, err := eventInfoV1(&page.Events[i], req.Format)
		if err != nil {
			return zero, err
		}
		events = append(events, info)
	}
	return v1Response(events, endLedger, limit, lr), nil
}

// v1Response mints the cursor the v1 way: a page that fills its limit hands
// back its last event's id; a short page hands back the end of the scanned
// window, so the next pull continues past it.
func v1Response(
	events []protocol.EventInfo, endLedger uint32, limit uint, lr store.LedgerRange,
) protocol.GetEventsResponse {
	var cursor string
	if len(events) > 0 && uint(len(events)) == limit {
		cursor = events[len(events)-1].ID
	} else {
		windowEnd := protocol.MaxCursor
		windowEnd.Ledger = endLedger - 1
		cursor = windowEnd.String()
	}
	if events == nil {
		events = []protocol.EventInfo{}
	}
	return protocol.GetEventsResponse{
		Events:                events,
		Cursor:                cursor,
		LatestLedger:          lr.LastLedger.Sequence,
		OldestLedger:          lr.FirstLedger.Sequence,
		LatestLedgerCloseTime: lr.LastLedger.CloseTime,
		OldestLedgerCloseTime: lr.FirstLedger.CloseTime,
	}
}

// v1Resume turns a v1 cursor, already incremented the v1 way, into the
// pager's resume state: the scan starts at the first stored event at or
// after c, the inclusive start v1's id >= scan uses. Within a ledger,
// storage order is id order, so that boundary is found by binary search
// with point fetches, and the pager re-enters after the event before it.
func v1Resume(
	ctx context.Context, view *query.ReadView, c protocol.Cursor, scope query.EventScope,
) (query.EventCursor, error) {
	// A short page hands back MaxCursor at the window's end, whose tx and
	// op sentinels top every storable event, so the search below would
	// always walk to the ledger's end. Answer it without reading anything:
	// this is the cursor a caught-up poller sends every time.
	if c.Tx == protocol.MaxCursor.Tx && c.Op == protocol.MaxCursor.Op {
		return query.EventCursor{Scope: scope, ScannedLedger: c.Ledger}, nil
	}
	r, err := view.Events(chunk.IDFromLedger(c.Ledger))
	if err != nil {
		return query.EventCursor{}, err
	}
	ofs, err := r.Offsets()
	if err != nil {
		return query.EventCursor{}, err
	}
	lStart, lEnd, err := ofs.EventIDs(c.Ledger)
	if err != nil {
		return query.EventCursor{}, err
	}
	// Find the first stored ordinal in the cursor's ledger at or after the
	// cursor. prev tracks the payload just below the boundary: lo only
	// moves to mid+1, so the last advancing probe is exactly lo-1.
	lo, hi := lStart, lEnd
	var prev *event.Payload
	for lo < hi {
		mid := lo + (hi-lo)/2
		got, err := r.FetchEvents(ctx, []uint32{mid})
		if err != nil {
			return query.EventCursor{}, err
		}
		p := got[0]
		atOrAfter := protocol.Cursor{
			Ledger: c.Ledger, Tx: p.TxIdx, Op: p.OpIdx, Event: p.EventIdx,
		}.Cmp(c) >= 0
		if atOrAfter {
			hi = mid
		} else {
			lo = mid + 1
			prev = &p
		}
	}
	switch lo {
	case lEnd:
		// Everything in the cursor's ledger sorts below it (the window-end
		// cursor of a short page, whose incremented sentinel tuple still
		// tops every storable event, or the incremented last event, or an
		// empty ledger): the watermark covers the ledger and resume moves
		// past it.
		return query.EventCursor{Scope: scope, ScannedLedger: c.Ledger}, nil
	case lStart:
		// The whole ledger sorts at or after the cursor: scan it whole.
		return query.EventCursor{Scope: scope}, nil
	default:
		return query.EventCursor{Scope: scope, Position: &query.EventPosition{
			Ledger: prev.LedgerSequence, Tx: prev.TxIdx, Op: prev.OpIdx, Event: prev.EventIdx,
			LedgerOrdinal: lo - 1 - lStart,
		}}, nil
	}
}

// v1Filters expands a validated v1 filter list into the pager's filters. The
// OR dimensions within one v1 filter (contract ids, topic filters) multiply
// out, one store filter per combination: at most 5 filters x 5 contract ids
// x 5 topics = 125, under the pager's cap. A combination with no constraints
// matches every event, so the whole query collapses to the pager's
// match-all (nil).
func v1Filters(in []protocol.EventFilter) ([]event.Filter, error) {
	var out []event.Filter
	for i := range in {
		expanded, matchAll, err := expandV1Filter(&in[i])
		if err != nil {
			return nil, err
		}
		if matchAll {
			return nil, nil
		}
		out = append(out, expanded...)
	}
	return out, nil
}

func expandV1Filter(f *protocol.EventFilter) ([]event.Filter, bool, error) {
	// A validated type set holds only contract and system: naming both
	// constrains nothing, naming one is one term. Either way the type never
	// multiplies the expansion.
	var eventType *xdr.ContractEventType
	if len(f.EventType) == 1 {
		name := f.EventType.Keys()[0]
		typ, ok := protocol.GetEventTypeXDRFromEventType()[name]
		if !ok {
			// Valid admits only contract and system, so a name that is
			// neither is a handler bug, not client input.
			return nil, false, fmt.Errorf("unsupported event type %q", name)
		}
		eventType = &typ
	}
	contracts := [][]byte{nil}
	if len(f.ContractIDs) > 0 {
		contracts = make([][]byte, 0, len(f.ContractIDs))
		for _, id := range f.ContractIDs {
			raw, err := strkey.Decode(strkey.VersionByteContract, id)
			if err != nil {
				// Unreachable: req.Valid decoded it already. The message is
				// the v1 handler's backstop wording.
				return nil, false, fmt.Errorf("invalid contract ID: %v", id)
			}
			contracts = append(contracts, raw)
		}
	}
	shapes := []topicShape{{}}
	if len(f.Topics) > 0 {
		shapes = make([]topicShape, 0, len(f.Topics))
		for _, tf := range f.Topics {
			shape, err := topicShapeOf(tf)
			if err != nil {
				return nil, false, err
			}
			shapes = append(shapes, shape)
		}
	}

	out := make([]event.Filter, 0, len(contracts)*len(shapes))
	for _, cid := range contracts {
		for _, sh := range shapes {
			flt := event.Filter{
				ContractID: cid, EventType: eventType,
				Topics: sh.topics, TopicCount: sh.count,
			}
			if isMatchAll(&flt) {
				return nil, true, nil
			}
			out = append(out, flt)
		}
	}
	return out, false, nil
}

// topicShape is one v1 TopicFilter in the store's terms: the pinned
// positional values plus the arity constraint. N segments match exactly N
// topics; a trailing "**" relaxes that to at least the prefix, and "at least
// zero" is the zero value, no constraint.
type topicShape struct {
	topics [protocol.MaxTopicCount][]byte
	count  event.TopicCountFilter
}

func topicShapeOf(tf protocol.TopicFilter) (topicShape, error) {
	segs := tf
	shape := topicShape{count: event.TopicCountFilter{Count: len(tf), Exact: true}}
	if n := len(tf); n > 0 && tf[n-1].Wildcard != nil && *tf[n-1].Wildcard == protocol.WildCardZeroOrMore {
		// "At least zero" is the wildcard, which is the zero value.
		segs = tf[:n-1]
		shape.count = event.TopicCountFilter{Count: len(segs)}
	}
	for i, s := range segs {
		// "*" is any value, and the position still exists via the count.
		// A segment with neither value nor wildcard is skipped the way the
		// shared handler skips it, rather than dereferenced.
		if s.Wildcard != nil || s.ScVal == nil {
			continue
		}
		raw, err := s.ScVal.MarshalBinary()
		if err != nil {
			return topicShape{}, fmt.Errorf("failed to marshal segment: %w", err)
		}
		shape.topics[i] = raw
	}
	return shape, nil
}

func isMatchAll(f *event.Filter) bool {
	if f.EventType != nil || len(f.ContractID) > 0 || f.TopicCount != (event.TopicCountFilter{}) {
		return false
	}
	for i := range f.Topics {
		if len(f.Topics[i]) > 0 {
			return false
		}
	}
	return true
}

// eventInfoV1 mints the v1 response event: the same stored-payload decode as
// eventInfoV2, reshaped into the v1 wire type.
func eventInfoV1(p *event.Payload, format string) (protocol.EventInfo, error) {
	v2, err := eventInfoV2(p, format)
	if err != nil {
		return protocol.EventInfo{}, fmt.Errorf("could not parse event: %w", err)
	}
	return protocol.EventInfo{
		EventType:       v2.EventType,
		Ledger:          v2.Ledger,
		LedgerClosedAt:  v2.LedgerClosedAt,
		ContractID:      v2.ContractID,
		ID:              v2.ID,
		OpIndex:         v2.OpIndex,
		TxIndex:         v2.TxIndex,
		TransactionHash: v2.TransactionHash,
		TopicXDR:        v2.TopicXDR,
		TopicJSON:       v2.TopicJSON,
		ValueXDR:        v2.ValueXDR,
		ValueJSON:       v2.ValueJSON,
	}, nil
}
