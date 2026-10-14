// Package eventsapi serves queryEvents for both daemons. It holds the
// handler and the conversions between the SDK's request and response types
// and the engine's form (eventquery.EventScope, eventquery.Filter). The
// engine is injected as an eventquery.Querier.
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
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/eventquery"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/methods"
)

// Limits are the page caps an operator configures, which both events
// methods have.
type Limits struct {
	// MaxLimit is protocol.QueryEventsMaxLimit unless an operator lowers it.
	MaxLimit uint
	// DefaultLimit applies when the request sets no limit.
	DefaultLimit uint
}

// DefaultTermBudget is the getEvents v2 proposal's default term budget.
const DefaultTermBudget uint = 15

// QueryEventsLimits adds the term budget. Only queryEvents has one: v1's own caps
// bound a request's terms already, so a budget there could only reject
// requests v1 accepts.
type QueryEventsLimits struct {
	Limits

	TermBudget uint32
}

// NewQueryEventsHandler builds the queryEvents handler. It decodes the params itself,
// not through methods.NewHandler, so an unknown field fails.
func NewQueryEventsHandler(
	querier eventquery.Querier, limits QueryEventsLimits, logger *supportlog.Entry,
) jrpc2.Handler {
	return func(ctx context.Context, r *jrpc2.Request) (any, error) {
		req, err := decodeRequest(r.ParamString(), limits.MaxLimit)
		if err != nil {
			return nil, err
		}
		return QueryEvents(ctx, querier, limits, logger, req)
	}
}

// decodeRequest decodes the params, rejecting unknown fields. A failure is
// reported by field name, in the client's terms, never as the decoder's
// message: that names Go types the client has no use for.
func decodeRequest(params string, maxLimit uint) (*protocol.QueryEventsRequest, error) {
	var req protocol.QueryEventsRequest
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

// QueryEvents serves one request against querier and classifies every
// failure in one place. Exported for the daemons' own tests.
func QueryEvents(
	ctx context.Context, querier eventquery.Querier, limits QueryEventsLimits, logger *supportlog.Entry,
	req *protocol.QueryEventsRequest,
) (protocol.QueryEventsResponse, error) {
	session, err := querier.Begin(ctx)
	if err != nil {
		return protocol.QueryEventsResponse{}, responseError(err, 0, 0, logger)
	}
	defer func() {
		if cerr := session.Close(); cerr != nil {
			logger.WithError(cerr).Warn("queryEvents: closing the read session")
		}
	}()
	oldest, latest := session.OldestLedger(), session.LatestLedger()
	resp, err := serve(ctx, session, limits, req, oldest, latest)
	if err != nil {
		return protocol.QueryEventsResponse{}, responseError(err, oldest, latest, logger)
	}
	return resp, nil
}

func serve(
	ctx context.Context, view eventquery.Session, limits QueryEventsLimits,
	req *protocol.QueryEventsRequest, oldest, latest uint32,
) (protocol.QueryEventsResponse, error) {
	cursor, limit, err := requestCursor(limits, req, oldest, latest)
	if err != nil {
		return protocol.QueryEventsResponse{}, err
	}
	// Checked on the cursor path too: a cursor carries the filters and is
	// not signed, so a hand-built one could ask for any number of lookups.
	if err := checkTermBudget(cursor.Scope.Filters, limits.TermBudget); err != nil {
		return protocol.QueryEventsResponse{}, err
	}
	// A range with no ledgers in it is finished before it starts. Only a
	// below-genesis max produces one; the pager would call it inverted.
	if s := &cursor.Scope; s.MaxLedger != nil && s.MinLedger > *s.MaxLedger {
		return response(&eventquery.EventPage{Next: cursor, Status: eventquery.ScanComplete}, req.Format, oldest, latest)
	}
	page, err := view.QueryEvents(ctx, cursor, limit)
	if err != nil {
		return protocol.QueryEventsResponse{}, err
	}
	return response(page, req.Format, oldest, latest)
}

// requestCursor turns either request shape into the cursor the pager
// advances, plus the limit to advance it by.
func requestCursor(
	limits QueryEventsLimits, req *protocol.QueryEventsRequest, oldest, latest uint32,
) (eventquery.EventCursor, int, error) {
	// Check the operator's limit first. req.Valid checks
	// protocol.QueryEventsMaxLimit, and whichever check runs first is the number
	// the error names.
	limit := limits.DefaultLimit
	if req.Limit != nil {
		if *req.Limit == 0 || *req.Limit > limits.MaxLimit {
			return eventquery.EventCursor{}, 0, &protocol.InvalidParamsError{
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
	if err := req.Valid(protocol.QueryEventsDefaultMaxFilters); err != nil {
		return eventquery.EventCursor{}, 0, err
	}
	if req.Cursor != "" {
		cursor, err := eventquery.DecodeEventCursor(req.Cursor)
		if err != nil {
			return eventquery.EventCursor{}, 0, err
		}
		if err := validateCursorFilters(cursor.Scope.Filters); err != nil {
			return eventquery.EventCursor{}, 0, err
		}
		return *cursor, pageLimit, nil
	}
	scope, err := eventScope(req, oldest, latest)
	if err != nil {
		return eventquery.EventCursor{}, 0, err
	}
	return eventquery.EventCursor{Scope: scope}, pageLimit, nil
}

// validateCursorFilters rejects filter shapes no queryEvents request can build, so
// only a hand-built cursor carries them: a clause with no constraint (a
// match-all the term budget counts as zero), a type outside contract and
// system, and a topic-count clause. The pager accepts all three because
// the v1 handler builds them directly, as values rather than tokens; this
// handler is the codec's only encoder.
func validateCursorFilters(filters []eventquery.Filter) error {
	for i := range filters {
		f := &filters[i]
		hasTopic := slices.ContainsFunc(f.Topics[:], func(t []byte) bool { return len(t) > 0 })
		switch {
		case len(f.ContractID) == 0 && f.EventType == nil && !hasTopic:
			return fmt.Errorf("%w: filter %d has no constraint", eventquery.ErrCursorMalformed, i)
		case f.EventType != nil && *f.EventType != xdr.ContractEventTypeContract &&
			*f.EventType != xdr.ContractEventTypeSystem:
			return fmt.Errorf("%w: filter %d names event type %d", eventquery.ErrCursorMalformed, i, *f.EventType)
		case f.TopicCount != (eventquery.TopicCountFilter{}):
			return fmt.Errorf("%w: filter %d carries a topic count", eventquery.ErrCursorMalformed, i)
		}
	}
	return nil
}

// checkTermBudget reports both numbers on rejection. A client needs them
// to see how far over it is and split the eventquery.
func checkTermBudget(filters []eventquery.Filter, budget uint32) error {
	used := uint32(eventquery.CountDistinctTerms(filters)) //nolint:gosec // a term count cannot be negative
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
	page *eventquery.EventPage, format string, oldest, latest uint32,
) (protocol.QueryEventsResponse, error) {
	resp := protocol.QueryEventsResponse{
		Events:        make([]protocol.EventInfo, 0, len(page.Events)),
		ScanStatus:    responseScanStatus(page.Status),
		ScannedLedger: responseScannedLedger(&page.Next),
		OldestLedger:  oldest,
		LatestLedger:  latest,
	}
	for i := range page.Events {
		info, err := EventInfo(&page.Events[i], format)
		if err != nil {
			return protocol.QueryEventsResponse{}, err
		}
		resp.Events = append(resp.Events, info)
	}
	if page.Status != eventquery.ScanComplete {
		token, err := page.Next.Encode()
		if err != nil {
			return protocol.QueryEventsResponse{}, fmt.Errorf("rpcv2: mint cursor: %w", err)
		}
		resp.Cursor = token
	}
	return resp, nil
}

// responseScannedLedger translates the cursor's watermark for the wire. A
// cursor uses 0 to mean "nothing covered yet". On the wire that 0 would
// claim the whole range, so report the ledger just outside it instead.
func responseScannedLedger(next *eventquery.EventCursor) uint32 {
	if next.ScannedLedger != 0 {
		return next.ScannedLedger
	}
	if next.Scope.Dir == eventquery.Descending {
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
func responseError(err error, oldest, latest uint32, logger *supportlog.Entry) error {
	var invalid *protocol.InvalidParamsError
	if errors.As(err, &invalid) {
		return jrpcError(invalid.Message, invalid.Data)
	}
	var outOfRange *eventquery.RangeError
	if errors.As(err, &outOfRange) {
		return jrpcError(clientMessage(err), protocol.LedgerOutOfRangeErrorData{
			MissingLedger: outOfRange.Requested,
			OldestLedger:  outOfRange.Oldest,
			LatestLedger:  outOfRange.Latest,
		})
	}
	switch {
	case errors.Is(err, eventquery.ErrCursorMalformed),
		errors.Is(err, eventquery.ErrCursorUnknownVersion),
		errors.Is(err, eventquery.ErrPositionMismatch):
		return jrpcError(clientMessage(err), protocol.CursorMalformedErrorData{
			OldestLedger: oldest,
			LatestLedger: latest,
		})
	case errors.Is(err, eventquery.ErrInvalidLimit),
		errors.Is(err, errJSONInputFormatUnsupported):
		return invalidParams(clientMessage(err))
	}
	// Cancellation and the deadline pass through. jrpc2 codes them
	// itself, and those codes say more than an internal error.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	// Nothing above claimed it, so this is the server's fault, not the
	// request's: a stored event the reader could not make sense of, a
	// cursor this server minted and cannot re-encode, or a store failure.
	// The client gets the message; an operator needs it in the log too,
	// since the response is gone the moment it is sent.
	logger.WithError(err).Error("queryEvents: unclassified failure, serving an internal error")
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

// errJSONInputFormatUnsupported: xdrInputFormat "json" is a spec option this
// server does not serve. The term index matches on a topic's canonical XDR
// bytes; xdr2json can produce them, but the handler is not wired to it.
var errJSONInputFormatUnsupported = errors.New(
	"xdrInputFormat \"json\" is not supported yet")

// eventScope converts a validated range-form request into the pager's scope.
// An absent ascending maxLedger stays nil, the open bound. A descending one
// is pinned to latest, so every page of the session shares one top edge.
func eventScope(
	req *protocol.QueryEventsRequest, oldest, latest uint32,
) (eventquery.EventScope, error) {
	// A below-genesis minLedger is raised, not rejected: no ledger exists
	// below genesis, so the range keeps the same ledgers. The max is never
	// raised, because that would add genesis to a range that excluded it.
	scope := eventquery.EventScope{MinLedger: max(req.MinLedger, eventquery.FirstLedgerSeq)}
	if req.Order == protocol.OrderDescending {
		scope.Dir = eventquery.Descending
		// With no max from the client, a low edge past the tip is out of
		// range.
		if req.MaxLedger == 0 && scope.MinLedger > latest {
			return eventquery.EventScope{}, &eventquery.RangeError{
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
		scope.Filters = make([]eventquery.Filter, len(req.Filters))
		for i := range req.Filters {
			f, err := eventFilter(&req.Filters[i], req.XDRInputFormat)
			if err != nil {
				return eventquery.EventScope{}, fmt.Errorf("filters[%d]: %w", i, err)
			}
			scope.Filters[i] = f
		}
	}
	return scope, nil
}

// eventFilter converts one request filter into the store's matching form.
// queryEvents filters carry no arity, so TopicCount stays the wildcard.
func eventFilter(f *protocol.QueryEventsFilter, xdrInputFormat string) (eventquery.Filter, error) {
	var out eventquery.Filter
	if f.ContractID != "" {
		raw, err := strkey.Decode(strkey.VersionByteContract, f.ContractID)
		if err != nil {
			return eventquery.Filter{}, fmt.Errorf("contractId: %w", err)
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
		return eventquery.Filter{}, fmt.Errorf("unsupported event type %q", f.EventType)
	}
	for i, topic := range f.Topics() {
		if topic == nil {
			continue
		}
		// The check is here, not above the loop, so a filter with no topics
		// is served whatever format it declares.
		if xdrInputFormat == protocol.FormatJSON {
			return eventquery.Filter{}, errJSONInputFormatUnsupported
		}
		// A base64 topic is a JSON string of the ScVal's XDR bytes. Valid
		// checked them, and they are what the term index is keyed on.
		var view xdr.ScValView
		if err := json.Unmarshal(topic, &view); err != nil {
			return eventquery.Filter{}, fmt.Errorf("topic%d: %w", i, err)
		}
		out.Topics[i] = []byte(view)
	}
	return out, nil
}

func responseScanStatus(s eventquery.ScanStatus) string {
	switch s {
	case eventquery.ScanComplete:
		return protocol.ScanStatusComplete
	case eventquery.ScanWaitingForLedgers:
		return protocol.ScanStatusWaitingForLedgers
	case eventquery.ScanOldestReached:
		return protocol.ScanStatusOldestReached
	default:
		return protocol.ScanStatusHasMore
	}
}

// EventInfo builds one response event from a stored event payload.
func EventInfo(p *eventquery.Payload, format string) (protocol.EventInfo, error) {
	cursor := protocol.Cursor{Ledger: p.LedgerSequence, Tx: p.TxIdx, Op: p.OpIdx, Event: p.EventIdx}
	info, err := methods.EventInfoFromView(xdr.ContractEventView(p.ContractEventBytes), cursor,
		time.Unix(p.LedgerClosedAt, 0).UTC().Format(time.RFC3339), p.TxHash.HexString(), format)
	if err != nil {
		return protocol.EventInfo{}, fmt.Errorf("rpcv2: %w", err)
	}
	// Stricter than v1, which also renders diagnostic events.
	if err := checkResponseEventType(info.EventType); err != nil {
		return protocol.EventInfo{}, err
	}
	return info, nil
}

// checkResponseEventType: ingest stores contract and system events only, so only diagnostic is refused.
func checkResponseEventType(name string) error {
	if name != protocol.EventTypeDiagnostic {
		return nil
	}
	return fmt.Errorf("rpcv2: stored event has type %q;"+
		" this endpoint serves contract and system events only", name)
}
