// Package eventsapi serves getEventsV2. It holds the handler and the
// conversions between the SDK's request and response types and the
// pager's own form (query.EventScope, stores/event.Filter).
package eventsapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/creachadair/jrpc2"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/methods"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/adapters"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/xdr2json"
)

// Limits are the caps an operator configures.
type Limits struct {
	TermBudget uint32
	// MaxLimit is protocol.MaxLimitV2 unless an operator lowers it.
	MaxLimit uint
	// DefaultLimit applies when the request sets no limit.
	DefaultLimit uint
}

// NewHandler builds the getEventsV2 handler.
func NewHandler(limits Limits) jrpc2.Handler {
	return methods.NewHandler(
		func(ctx context.Context, req protocol.GetEventsV2Request) (protocol.GetEventsV2Response, error) {
			return getEventsV2(ctx, limits, &req)
		})
}

// getEventsV2 classifies every failure in one place.
func getEventsV2(
	ctx context.Context, limits Limits, req *protocol.GetEventsV2Request,
) (protocol.GetEventsV2Response, error) {
	view, err := adapters.ViewFrom(ctx)
	if err != nil {
		return protocol.GetEventsV2Response{}, responseError(err, 0, 0)
	}
	oldest, latest := view.OldestLedger(), view.LatestLedger()
	resp, err := serve(ctx, view, limits, req, oldest, latest)
	if err != nil {
		return protocol.GetEventsV2Response{}, responseError(err, oldest, latest)
	}
	return resp, nil
}

func serve(
	ctx context.Context, view *query.ReadView, limits Limits,
	req *protocol.GetEventsV2Request, oldest, latest uint32,
) (protocol.GetEventsV2Response, error) {
	cursor, limit, err := requestCursor(limits, req, latest)
	if err != nil {
		return protocol.GetEventsV2Response{}, err
	}
	// Checked on the cursor path too: a cursor carries the filters and is
	// not signed, so a hand-built one could ask for any number of lookups.
	if err := checkTermBudget(cursor.Scope.Filters, limits.TermBudget); err != nil {
		return protocol.GetEventsV2Response{}, err
	}
	page, err := view.QueryEvents(ctx, cursor, limit)
	if err != nil {
		return protocol.GetEventsV2Response{}, err
	}
	return response(page, req.Format, oldest, latest)
}

// requestCursor turns either request shape into the cursor the pager
// advances, plus the limit to advance it by.
func requestCursor(
	limits Limits, req *protocol.GetEventsV2Request, latest uint32,
) (query.EventCursor, int, error) {
	// Check the operator's limit first. req.Valid checks
	// protocol.MaxLimitV2, and whichever check runs first is the number
	// the error names.
	limit := limits.DefaultLimit
	if req.Limit != nil {
		if *req.Limit > limits.MaxLimit {
			return query.EventCursor{}, 0, &protocol.InvalidParamsError{
				Message: fmt.Sprintf("limit must be between 1 and %d", limits.MaxLimit),
				Data:    protocol.InvalidParamsErrorData{Reason: protocol.ErrorReasonInvalidParams},
			}
		}
		limit = *req.Limit
	}
	if err := req.Valid(protocol.DefaultMaxFiltersV2); err != nil {
		return query.EventCursor{}, 0, err
	}
	if req.Cursor != "" {
		cursor, err := query.DecodeEventCursor(req.Cursor)
		if err != nil {
			return query.EventCursor{}, 0, err
		}
		return *cursor, int(limit), nil
	}
	scope, err := eventScope(req, latest)
	if err != nil {
		return query.EventCursor{}, 0, err
	}
	return query.EventCursor{Scope: scope}, int(limit), nil
}

// checkTermBudget reports both numbers on rejection. A client needs them
// to see how far over it is and split the query.
func checkTermBudget(filters []event.Filter, budget uint32) error {
	used := uint32(event.CountDistinctTerms(filters)) //nolint:gosec // a term count cannot be negative
	if used <= budget {
		return nil
	}
	return &protocol.InvalidParamsError{
		Message: fmt.Sprintf("query needs %d distinct index terms, but this node allows %d;"+
			" split it into narrower queries", used, budget),
		Data: protocol.InvalidParamsErrorData{
			Reason:     protocol.ErrorReasonInvalidParams,
			TermsUsed:  used,
			TermBudget: budget,
		},
	}
}

// response carries a cursor on every page but the last: an absent cursor is
// what tells a client the query is finished.
func response(
	page *query.EventPage, format string, oldest, latest uint32,
) (protocol.GetEventsV2Response, error) {
	resp := protocol.GetEventsV2Response{
		Events:        make([]protocol.EventInfoV2, 0, len(page.Events)),
		ScanStatus:    responseScanStatus(page.Status),
		ScannedLedger: page.Next.ScannedLedger,
		OldestLedger:  oldest,
		LatestLedger:  latest,
	}
	for i := range page.Events {
		info, err := eventInfoV2(&page.Events[i], format)
		if err != nil {
			return protocol.GetEventsV2Response{}, err
		}
		resp.Events = append(resp.Events, info)
	}
	if page.Status != query.ScanComplete {
		token, err := page.Next.Encode()
		if err != nil {
			return protocol.GetEventsV2Response{}, fmt.Errorf("rpcv2: mint cursor: %w", err)
		}
		resp.Cursor = token
	}
	return resp, nil
}

// responseError maps the pager's and the SDK's errors onto the three
// machine-readable reasons. Anything else becomes an internal error; jrpc2
// would otherwise code a plain error as SystemError.
func responseError(err error, oldest, latest uint32) error {
	var invalid *protocol.InvalidParamsError
	if errors.As(err, &invalid) {
		return jrpcError(invalid.Message, invalid.Data)
	}
	var outOfRange *query.RangeError
	if errors.As(err, &outOfRange) {
		return jrpcError(err.Error(), protocol.LedgerOutOfRangeErrorData{
			MissingLedger: outOfRange.Requested,
			OldestLedger:  outOfRange.Oldest,
			LatestLedger:  outOfRange.Latest,
		})
	}
	switch {
	case errors.Is(err, query.ErrCursorMalformed),
		errors.Is(err, query.ErrCursorUnknownVersion),
		errors.Is(err, query.ErrPositionMismatch):
		return jrpcError(err.Error(), protocol.CursorMalformedErrorData{
			OldestLedger: oldest,
			LatestLedger: latest,
		})
	case errors.Is(err, query.ErrInvertedRange),
		errors.Is(err, query.ErrInvalidLimit),
		errors.Is(err, errJSONInputFormatUnsupported):
		return jrpcError(err.Error(), protocol.InvalidParamsErrorData{
			Reason: protocol.ErrorReasonInvalidParams,
		})
	}
	// Cancellation and the deadline pass through. jrpc2 codes them
	// itself, and those codes say more than an internal error.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return &jrpc2.Error{Code: jrpc2.InternalError, Message: err.Error()}
}

// jrpcError: only InvalidParamsErrorData needs its reason set by hand. The
// other two payloads set theirs when they marshal.
func jrpcError(message string, data any) error {
	encoded, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("rpcv2: encode error data: %w", err)
	}
	return &jrpc2.Error{Code: jrpc2.InvalidParams, Message: message, Data: encoded}
}

// errJSONInputFormatUnsupported: xdrInputFormat "json" needs a JSON-to-XDR
// converter, because the term index matches on a topic's canonical bytes.
// Deferred to #940.
var errJSONInputFormatUnsupported = errors.New(
	"rpcv2: xdrInputFormat \"json\" is not supported yet")

// eventScope converts a validated range-form request into the pager's scope.
// An absent ascending maxLedger stays nil, the open bound. A descending one
// is pinned to latest, so every page of the session shares one top edge.
func eventScope(
	req *protocol.GetEventsV2Request, latest uint32,
) (query.EventScope, error) {
	// A below-genesis minLedger is raised, not rejected. The floor rules
	// then decide the outcome: ledger_out_of_range ascending,
	// OLDEST_REACHED descending. Only a forged cursor now trips the
	// pager's own below-genesis check.
	scope := query.EventScope{MinLedger: max(req.MinLedger, chunk.FirstLedgerSeq)}
	if req.Order == protocol.OrderDescending {
		scope.Dir = query.Descending
		maxLedger := req.MaxLedger
		if maxLedger == 0 {
			maxLedger = latest
		}
		scope.MaxLedger = &maxLedger
	} else if req.MaxLedger != 0 {
		maxLedger := req.MaxLedger
		scope.MaxLedger = &maxLedger
	}
	if len(req.Filters) > 0 {
		scope.Filters = make([]event.Filter, len(req.Filters))
		for i := range req.Filters {
			f, err := eventFilter(&req.Filters[i], req.XDRInputFormat)
			if err != nil {
				return query.EventScope{}, fmt.Errorf("filters[%d]: %w", i, err)
			}
			scope.Filters[i] = f
		}
	}
	return scope, nil
}

// eventFilter converts one request filter into the store's matching form.
// v2 filters carry no arity, so TopicCount stays the wildcard.
func eventFilter(f *protocol.EventFilterV2, xdrInputFormat string) (event.Filter, error) {
	var out event.Filter
	if f.ContractID != "" {
		raw, err := strkey.Decode(strkey.VersionByteContract, f.ContractID)
		if err != nil {
			return event.Filter{}, fmt.Errorf("contractId: %w", err)
		}
		out.ContractID = raw
	}
	switch f.EventType {
	case "":
	case protocol.EventTypeContract:
		out.EventType = new(xdr.ContractEventTypeContract)
	case protocol.EventTypeSystem:
		out.EventType = new(xdr.ContractEventTypeSystem)
	default:
		// Valid admits only the two names above. Anything else is a
		// handler bug, not client input.
		return event.Filter{}, fmt.Errorf("unsupported event type %q", f.EventType)
	}
	for i, topic := range f.Topics() {
		if topic == nil {
			continue
		}
		// The check is here, not above the loop, so a filter with no topics
		// is served whatever format it declares.
		if xdrInputFormat == protocol.FormatJSON {
			return event.Filter{}, errJSONInputFormatUnsupported
		}
		// A base64 topic is a JSON string of the ScVal's XDR bytes. Valid
		// checked them, and they are what the term index is keyed on.
		var view xdr.ScValView
		if err := json.Unmarshal(topic, &view); err != nil {
			return event.Filter{}, fmt.Errorf("topic%d: %w", i, err)
		}
		out.Topics[i] = []byte(view)
	}
	return out, nil
}

func responseScanStatus(s query.ScanStatus) string {
	switch s {
	case query.ScanComplete:
		return protocol.ScanStatusComplete
	case query.ScanWaitingForLedgers:
		return protocol.ScanStatusWaitingForLedgers
	case query.ScanOldestReached:
		return protocol.ScanStatusOldestReached
	default:
		return protocol.ScanStatusHasMore
	}
}

// eventInfoV2 builds one response event from a stored payload. The ID is
// the same TOID-and-index form v1 mints, and topics and value follow the
// request's xdrFormat.
func eventInfoV2(p *event.Payload, format string) (protocol.EventInfoV2, error) {
	var ev xdr.ContractEvent
	if err := ev.UnmarshalBinary(p.ContractEventBytes); err != nil {
		return protocol.EventInfoV2{}, fmt.Errorf("rpcv2: stored event bytes: %w", err)
	}
	if ev.Body.V != 0 || ev.Body.V0 == nil {
		// Unreachable: UnmarshalBinary above rejects any other discriminant.
		// Kept as a guard on the V0 dereferences that follow.
		return protocol.EventInfoV2{}, fmt.Errorf("rpcv2: stored event has body version %d", ev.Body.V)
	}

	eventType, err := responseEventType(ev.Type)
	if err != nil {
		return protocol.EventInfoV2{}, err
	}

	info := protocol.EventInfoV2{
		EventType:      eventType,
		Ledger:         int32(p.LedgerSequence), //nolint:gosec // ledger sequences fit int32 by protocol
		LedgerClosedAt: time.Unix(p.LedgerClosedAt, 0).UTC().Format(time.RFC3339),
		ID: protocol.Cursor{
			Ledger: p.LedgerSequence, Tx: p.TxIdx, Op: p.OpIdx, Event: p.EventIdx,
		}.String(),
		OpIndex:         p.OpIdx,
		TxIndex:         p.TxIdx,
		TransactionHash: p.TxHash.HexString(),
	}
	if ev.ContractId != nil {
		info.ContractID = strkey.MustEncode(strkey.VersionByteContract, ev.ContractId[:])
	}

	if format == protocol.FormatJSON {
		info.TopicJSON = make([]json.RawMessage, 0, len(ev.Body.V0.Topics))
		for i := range ev.Body.V0.Topics {
			converted, err := xdr2json.ConvertInterface(ev.Body.V0.Topics[i])
			if err != nil {
				return protocol.EventInfoV2{}, fmt.Errorf("rpcv2: topic %d to json: %w", i, err)
			}
			info.TopicJSON = append(info.TopicJSON, converted)
		}
		valueJSON, err := xdr2json.ConvertInterface(ev.Body.V0.Data)
		if err != nil {
			return protocol.EventInfoV2{}, fmt.Errorf("rpcv2: value to json: %w", err)
		}
		info.ValueJSON = valueJSON
		return info, nil
	}

	info.TopicXDR = make([]string, 0, len(ev.Body.V0.Topics))
	for i := range ev.Body.V0.Topics {
		encoded, err := xdr.MarshalBase64(ev.Body.V0.Topics[i])
		if err != nil {
			return protocol.EventInfoV2{}, fmt.Errorf("rpcv2: topic %d to xdr: %w", i, err)
		}
		info.TopicXDR = append(info.TopicXDR, encoded)
	}
	valueXDR, err := xdr.MarshalBase64(ev.Body.V0.Data)
	if err != nil {
		return protocol.EventInfoV2{}, fmt.Errorf("rpcv2: value to xdr: %w", err)
	}
	info.ValueXDR = valueXDR
	return info, nil
}

// responseEventType: ingest stores contract and system events only.
func responseEventType(t xdr.ContractEventType) (string, error) {
	switch t {
	case xdr.ContractEventTypeSystem:
		return protocol.EventTypeSystem, nil
	case xdr.ContractEventTypeContract:
		return protocol.EventTypeContract, nil
	default:
		return "", fmt.Errorf("rpcv2: stored event has type %d;"+
			" this endpoint serves contract and system events only", t)
	}
}
