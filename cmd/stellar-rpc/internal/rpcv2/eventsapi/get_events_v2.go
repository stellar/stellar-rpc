// Package eventsapi serves getEventsV2. It holds the handler and the
// conversions between the SDK's request and response types and the
// pager's own form (query.EventScope, stores/event.Filter).
package eventsapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/creachadair/jrpc2"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

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

// NewHandler builds the getEventsV2 handler. It decodes the params itself,
// not through methods.NewHandler, so an unknown field fails.
func NewHandler(limits Limits) jrpc2.Handler {
	return func(ctx context.Context, r *jrpc2.Request) (any, error) {
		req, err := decodeRequest(r.ParamString(), limits.MaxLimit)
		if err != nil {
			return nil, err
		}
		return getEventsV2(ctx, limits, req)
	}
}

// decodeRequest decodes the params, rejecting unknown fields. A failure is
// reported by field name, in the client's terms, never as the decoder's
// message: that names Go types the client has no use for.
func decodeRequest(params string, maxLimit uint) (*protocol.GetEventsV2Request, error) {
	var req protocol.GetEventsV2Request
	if params == "" {
		return &req, nil
	}
	dec := json.NewDecoder(strings.NewReader(params))
	dec.DisallowUnknownFields()
	err := dec.Decode(&req)
	if err == nil {
		if _, err := dec.Token(); !errors.Is(err, io.EOF) {
			return nil, invalidParams("params must be a single object")
		}
		return &req, nil
	}
	var typeErr *json.UnmarshalTypeError
	switch {
	case errors.As(err, &typeErr) && typeErr.Field == "":
		return nil, invalidParams("params must be an object")
	case errors.As(err, &typeErr) && typeErr.Field == "limit":
		return nil, invalidParams(fmt.Sprintf("limit must be between 1 and %d", maxLimit))
	case errors.As(err, &typeErr):
		return nil, invalidParams(fmt.Sprintf("%s must be %s", fieldName(typeErr), wantedKind(typeErr)))
	}
	// Unknown field, or malformed JSON. The decoder's text is the client's
	// own input reflected back, minus the package prefix.
	return nil, invalidParams(clientMessage(err))
}

// fieldName is the failing field's path. encoding/json reports an array
// element under the array's own name, so the element is spelled out.
func fieldName(e *json.UnmarshalTypeError) string {
	if e.Field == "filters" && e.Type.Kind() == reflect.Struct {
		return "each filter"
	}
	return e.Field
}

// wantedKind names the JSON kind a field takes, for a type error's message.
func wantedKind(e *json.UnmarshalTypeError) string {
	switch e.Type.Kind() {
	case reflect.String:
		return "a string"
	case reflect.Slice:
		return "an array"
	case reflect.Struct:
		return "an object"
	default:
		return "a number"
	}
}

// clientMessage drops the package prefixes internal errors carry. They say
// which layer produced the error, which a client has no use for.
func clientMessage(err error) string {
	message := err.Error()
	for _, prefix := range []string{"json: ", "query: ", "rpcv2: "} {
		message = strings.TrimPrefix(message, prefix)
	}
	return message
}

func invalidParams(message string) error {
	return jrpcError(message, protocol.InvalidParamsErrorData{
		Reason: protocol.ErrorReasonInvalidParams,
	})
}

// getEventsV2 classifies every failure in one place.
func getEventsV2(
	ctx context.Context, limits Limits, req *protocol.GetEventsV2Request,
) (protocol.GetEventsV2Response, error) {
	view, err := query.ViewFrom(ctx)
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
	cursor, limit, err := requestCursor(limits, req, oldest, latest)
	if err != nil {
		return protocol.GetEventsV2Response{}, err
	}
	// Checked on the cursor path too: a cursor carries the filters and is
	// not signed, so a hand-built one could ask for any number of lookups.
	if err := checkTermBudget(cursor.Scope.Filters, limits.TermBudget); err != nil {
		return protocol.GetEventsV2Response{}, err
	}
	// A range with no ledgers in it is finished before it starts. Only a
	// below-genesis max produces one; the pager would call it inverted.
	if s := &cursor.Scope; s.MaxLedger != nil && s.MinLedger > *s.MaxLedger {
		return response(&query.EventPage{Next: cursor, Status: query.ScanComplete}, req.Format, oldest, latest)
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
	limits Limits, req *protocol.GetEventsV2Request, oldest, latest uint32,
) (query.EventCursor, int, error) {
	// Check the operator's limit first. req.Valid checks
	// protocol.MaxLimitV2, and whichever check runs first is the number
	// the error names.
	limit := limits.DefaultLimit
	if req.Limit != nil {
		if *req.Limit == 0 || *req.Limit > limits.MaxLimit {
			return query.EventCursor{}, 0, &protocol.InvalidParamsError{
				Message: fmt.Sprintf("limit must be between 1 and %d", limits.MaxLimit),
				Data:    protocol.InvalidParamsErrorData{Reason: protocol.ErrorReasonInvalidParams},
			}
		}
		limit = *req.Limit
	}
	// min makes the conversion provably safe. The suppression stays because
	// CI's gosec still reports it; nolintlint is suppressed with it, because
	// the newer gosec does not.
	pageLimit := int(min(limit, math.MaxInt32)) //nolint:gosec,nolintlint
	if err := req.Valid(protocol.DefaultMaxFiltersV2); err != nil {
		return query.EventCursor{}, 0, err
	}
	if req.Cursor != "" {
		cursor, err := query.DecodeEventCursor(req.Cursor)
		if err != nil {
			return query.EventCursor{}, 0, err
		}
		if err := validateCursorFilters(cursor.Scope.Filters); err != nil {
			return query.EventCursor{}, 0, err
		}
		return *cursor, pageLimit, nil
	}
	scope, err := eventScope(req, oldest, latest)
	if err != nil {
		return query.EventCursor{}, 0, err
	}
	return query.EventCursor{Scope: scope}, pageLimit, nil
}

// validateCursorFilters rejects filter shapes a v2 request cannot build,
// which only a hand-built cursor carries. The codec and the pager accept
// them because the v1 adapter will mint them. Two matter here: a clause
// with no constraint is a full scan that the term budget counts as zero,
// and a diagnostic type or a topic-count clause names events v2 never
// serves.
func validateCursorFilters(filters []event.Filter) error {
	for i := range filters {
		f := &filters[i]
		hasTopic := slices.ContainsFunc(f.Topics[:], func(t []byte) bool { return len(t) > 0 })
		switch {
		case len(f.ContractID) == 0 && f.EventType == nil && !hasTopic:
			return fmt.Errorf("%w: filter %d has no constraint", query.ErrCursorMalformed, i)
		case f.EventType != nil && *f.EventType != xdr.ContractEventTypeContract &&
			*f.EventType != xdr.ContractEventTypeSystem:
			return fmt.Errorf("%w: filter %d names event type %d", query.ErrCursorMalformed, i, *f.EventType)
		case f.TopicCount != (event.TopicCountFilter{}):
			return fmt.Errorf("%w: filter %d carries a topic count", query.ErrCursorMalformed, i)
		}
	}
	return nil
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
		ScannedLedger: responseScannedLedger(&page.Next),
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

// responseScannedLedger translates the cursor's watermark for the wire. A
// cursor uses 0 to mean "nothing covered yet". On the wire that 0 would
// claim the whole range, so report the ledger just outside it instead.
func responseScannedLedger(next *query.EventCursor) uint32 {
	if next.ScannedLedger != 0 {
		return next.ScannedLedger
	}
	if next.Scope.Dir == query.Descending {
		// Saturate, so a scope topped at MaxUint32 cannot wrap to 0.
		if *next.Scope.MaxLedger == math.MaxUint32 {
			return math.MaxUint32
		}
		return *next.Scope.MaxLedger + 1
	}
	return next.Scope.MinLedger - 1
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
		return jrpcError(clientMessage(err), protocol.LedgerOutOfRangeErrorData{
			MissingLedger: outOfRange.Requested,
			OldestLedger:  outOfRange.Oldest,
			LatestLedger:  outOfRange.Latest,
		})
	}
	switch {
	case errors.Is(err, query.ErrCursorMalformed),
		errors.Is(err, query.ErrCursorUnknownVersion),
		errors.Is(err, query.ErrPositionMismatch):
		return jrpcError(clientMessage(err), protocol.CursorMalformedErrorData{
			OldestLedger: oldest,
			LatestLedger: latest,
		})
	case errors.Is(err, query.ErrInvalidLimit),
		errors.Is(err, errJSONInputFormatUnsupported):
		return invalidParams(clientMessage(err))
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
	"xdrInputFormat \"json\" is not supported yet")

// eventScope converts a validated range-form request into the pager's scope.
// An absent ascending maxLedger stays nil, the open bound. A descending one
// is pinned to latest, so every page of the session shares one top edge.
func eventScope(
	req *protocol.GetEventsV2Request, oldest, latest uint32,
) (query.EventScope, error) {
	// A below-genesis minLedger is raised, not rejected: no ledger exists
	// below genesis, so the range keeps the same ledgers. The max is never
	// raised, because that would add genesis to a range that excluded it.
	scope := query.EventScope{MinLedger: max(req.MinLedger, chunk.FirstLedgerSeq)}
	if req.Order == protocol.OrderDescending {
		scope.Dir = query.Descending
		// With no max from the client, a low edge past the tip is out of
		// range.
		if req.MaxLedger == 0 && scope.MinLedger > latest {
			return query.EventScope{}, &query.RangeError{
				Requested: scope.MinLedger, Oldest: oldest, Latest: latest,
			}
		}
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
	// The response field is int32, so a sequence past its range would go
	// out negative. v1 rejects the same shape.
	if p.LedgerSequence > math.MaxInt32 {
		return protocol.EventInfoV2{}, fmt.Errorf(
			"rpcv2: ledger sequence %d exceeds supported range", p.LedgerSequence)
	}

	info := protocol.EventInfoV2{
		EventType:      eventType,
		Ledger:         int32(p.LedgerSequence),
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
