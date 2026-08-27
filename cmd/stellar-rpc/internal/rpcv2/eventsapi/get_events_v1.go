package eventsapi

// The v1 getEvents shim: the existing wire API served natively by the v2
// pager. PREP SCAFFOLD ONLY — the translation cores below are stubs; the
// implementation plan lives in getevents-v1-shim-brief.md at the repo root,
// and the parity harness in v1_parity_test.go is the acceptance oracle.
//
// TODO(fable): implement, in this order, per the brief:
//  1. v1Filters: type-set normalization (a validated set holds only
//     contract/system: empty or both => unconstrained, singleton => one
//     type) then cross-product expansion (filter x type x contractID x
//     topicFilter) into store filters with positional topics and TopicCount
//     arity. "diagnostic" never reaches here: request validation rejects it.
//  2. Range mapping: start = resolved cursor-or-startLedger, endLedger
//     exclusive per the v1 handler's arithmetic (start+LedgerScanLimit,
//     clamped to latest+1, then request.EndLedger); v1-style range check on
//     start BEFORE the pager (jrpc2.InvalidRequest, v1 message format).
//     endLedger <= start is legal v1 input: empty page + backwards MaxCursor,
//     never reaches the pager (which would reject the inverted scope).
//  3. Cursor resume: strictly-after (Event++), Position via ledger-local
//     search (view.Events / Offsets.EventIDs / ordered compare); no
//     successor => MinLedger = cursor.Ledger+1, nil Position.
//  4. Response: one QueryEvents call, v1 EventInfo minting (shared decode
//     core with eventInfoV2), cursor = last event id on a full page else
//     protocol.MaxCursor at the window end (page's scanned ledger; clamp
//     >= start.Ledger while review finding C1 is unfixed), close times via
//     the ledger-range assembly (placement decision in the brief).
//  5. Term budget on the expanded filters via checkTermBudget, knobs from
//     the getEvents (not getEventsV2) method config.

import (
	"context"
	"errors"

	"github.com/creachadair/jrpc2"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/methods"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event"
)

// errV1ShimPending marks the prep stub. The parity harness skips shim
// comparisons while responses carry this message; its removal is the switch
// that arms them.
var errV1ShimPending = errors.New("getEvents v1 shim not implemented yet (prep stub)")

// NewV1Handler builds the v1 getEvents handler. Limits are the getEvents
// method's own knobs (max/default items, term budget), not getEventsV2's.
func NewV1Handler(limits Limits) jrpc2.Handler {
	return methods.NewHandler(
		func(ctx context.Context, req protocol.GetEventsRequest) (protocol.GetEventsResponse, error) {
			return getEventsV1(ctx, limits, &req)
		})
}

func getEventsV1(
	_ context.Context, _ Limits, _ *protocol.GetEventsRequest,
) (protocol.GetEventsResponse, error) {
	// TODO(fable): steps 2-5 of the plan above.
	return protocol.GetEventsResponse{}, &jrpc2.Error{
		Code: jrpc2.MethodNotFound, Message: errV1ShimPending.Error(),
	}
}

// v1Filters translates a validated v1 filter list into the pager's filters.
// An empty result means the request had no filters (match all); no
// normalization path can drop every branch, since validation admits only
// contract/system type sets and well-formed contract ids and topic filters.
func v1Filters(_ []protocol.EventFilter) ([]event.Filter, error) {
	// TODO(fable): step 1 of the plan above. Expansion produces one store
	// filter per (filter, type, contractID, topicFilter) combination;
	// tests compare with ElementsMatch, so order is free.
	return nil, errV1ShimPending
}
