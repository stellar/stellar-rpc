package eventsapi

// The v1 getEvents endpoint served natively by the v2 pager. The wire
// contract is the shared v1 handler's (internal/methods/get_events.go),
// reproduced choice for choice: the same scan window, the same error codes
// and messages in the same order, a cursor on every response. v1 cursors are
// bare event ids, so the v2 cursor codec never runs; a cursor request is
// translated into the pager's resume state instead.

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/creachadair/jrpc2"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/methods"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/adapters"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

// NewV1Handler builds the v1 getEvents handler. Limits are the getEvents
// method's own page caps, which are v1's rather than the v2 spec's.
func NewV1Handler(limits Limits, logger *supportlog.Entry) jrpc2.Handler {
	return methods.NewHandler(
		func(ctx context.Context, req protocol.GetEventsRequest) (protocol.GetEventsResponse, error) {
			return getEventsV1(ctx, limits, logger, &req)
		})
}

//nolint:cyclop // the v1 handler's validation order, kept in one place like the original
func getEventsV1(
	ctx context.Context, limits Limits, logger *supportlog.Entry, req *protocol.GetEventsRequest,
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
	minLedger, from := v1ResumePoint(start, fromCursor)
	// An end at or below the start is legal v1 input and an empty window.
	// Served here: the pager's scopes are inclusive and never inverted.
	if endLedger <= minLedger {
		return v1Response(nil, endLedger, limit, lr), nil
	}
	maxLedger := endLedger - 1
	scope := query.EventScope{MinLedger: minLedger, MaxLedger: &maxLedger, Filters: filters}
	pageLimit := int(min(limit, math.MaxInt32)) //nolint:gosec // min clamps it
	page, err := view.QueryEventsFrom(ctx, scope, from, pageLimit)
	if err != nil {
		// The v1 handler codes every failure past validation as an invalid
		// request, cancellation included. Only a server fault is logged:
		// the client's copy of the message is the only one unless this
		// does, but a caller hanging up or running out its deadline is
		// routine and would drown the real faults.
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			logger.WithError(err).Error("getEvents: serving the request failed")
		}
		return zero, &jrpc2.Error{Code: jrpc2.InvalidRequest, Message: err.Error()}
	}
	// A short page must mean the scope is done: v1Response's window-end
	// cursor claims everything through endLedger-1 was scanned. That holds
	// because one pager call covers a full v1 window (the pairing test on
	// LedgerScanLimit); if that coupling ever breaks, fail loud here rather
	// than mint a cursor that skips the unscanned remainder.
	if uint(len(page.Events)) < limit &&
		(page.Status == query.ScanHasMore || page.Status == query.ScanWaitingForLedgers) {
		logger.WithField("startLedger", minLedger).WithField("endLedger", endLedger).
			WithField("served", len(page.Events)).WithField("limit", limit).
			WithField("scanStatus", page.Status).
			Error("getEvents: the scan stopped before the request's window was covered " +
				"(unreachable while the v1 window fits one pager page)")
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

// v1ResumePoint maps the request's resolved start onto where the walk
// begins: the scope's low ledger, and the id to serve from within it.
//
// A window-end cursor gets neither. It carries MaxCursor's tx and op
// sentinels, which top every storable id, so its ledger is finished by
// definition and the scope starts past it; seeking that ledger would read
// all of it to reach the same answer, and this is the cursor a caught-up
// poller sends on every request. Any other cursor is an inclusive id,
// already carrying v1's increment, which is v1's own "id >= cursor" scan.
func v1ResumePoint(start protocol.Cursor, fromCursor bool) (uint32, *query.EventID) {
	switch {
	case !fromCursor:
		return start.Ledger, nil
	case start.Tx == protocol.MaxCursor.Tx && start.Op == protocol.MaxCursor.Op:
		return start.Ledger + 1, nil
	default:
		return start.Ledger, &query.EventID{
			Ledger: start.Ledger, Tx: start.Tx, Op: start.Op, Event: start.Event,
		}
	}
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

// v1Filters compiles a validated v1 filter list into the pager's filters;
// nil matches every event.
func v1Filters(in []protocol.EventFilter) ([]event.Filter, error) {
	return store.CompileV1EventFilters(in)
}

// eventInfoV1 mints the v1 response event: eventInfoV2's output in the v1 wire type.
func eventInfoV1(p *event.Payload, format string) (protocol.EventInfo, error) {
	v2, err := eventInfoV2(p, format)
	if err != nil {
		return protocol.EventInfo{}, fmt.Errorf("could not parse event: %w", err)
	}
	return protocol.EventInfo(v2), nil
}
