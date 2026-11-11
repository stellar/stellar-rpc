package methods

import (
	"context"
	"encoding/base64"
	"fmt"
	"math"
	"time"

	"github.com/creachadair/jrpc2"
	"github.com/pkg/errors"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/support/collections/set"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/xdr2json"
)

const (
	LedgerScanLimit = 10000
	maxEventTypes   = 3
)

type eventsRPCHandler struct {
	dbReader         store.EventReader
	maxLimit         uint
	defaultLimit     uint
	maxResponseBytes int // zero means no limit
	logger           *log.Entry
}

func combineContractIDs(filters []protocol.EventFilter) ([][]byte, error) {
	contractIDSet := set.NewSet[string](protocol.MaxFiltersLimit * protocol.MaxContractIDsLimit)
	contractIDs := make([][]byte, 0, len(contractIDSet))

	for _, filter := range filters {
		// A filter with no contract IDs matches events from any contract, so
		// the combined DB-level restriction must be dropped entirely.
		if len(filter.ContractIDs) == 0 {
			return nil, nil
		}
		for _, contractID := range filter.ContractIDs {
			if !contractIDSet.Contains(contractID) {
				contractIDSet.Add(contractID)
				id, err := strkey.Decode(strkey.VersionByteContract, contractID)
				if err != nil {
					return nil, fmt.Errorf("invalid contract ID: %v", contractID)
				}
				contractIDs = append(contractIDs, id)
			}
		}
	}

	return contractIDs, nil
}

func combineEventTypes(filters []protocol.EventFilter) []int {
	eventTypes := set.NewSet[int](maxEventTypes)

	for _, filter := range filters {
		// A filter with no event types matches events of any type, so the
		// combined DB-level restriction must be dropped entirely.
		if len(filter.EventType) == 0 {
			return nil
		}
		for _, eventType := range filter.EventType.Keys() {
			eventTypeXDR := protocol.GetEventTypeXDRFromEventType()[eventType]
			eventTypes.Add(int(eventTypeXDR))
		}
	}
	uniqueEventTypes := make([]int, 0, maxEventTypes)
	for eventType := range eventTypes {
		uniqueEventTypes = append(uniqueEventTypes, eventType)
	}
	return uniqueEventTypes
}

func combineTopics(filters []protocol.EventFilter) store.TopicFilters {
	topicFilters := make(store.TopicFilters, 0, len(filters))

	for _, filter := range filters {
		if len(filter.Topics) == 0 {
			return nil
		}

		// Each topic is an OR...
		for _, topicFilter := range filter.Topics {
			conditions := make(store.TopicFilter, 0, len(topicFilter))
			// ...but each segment within a topic is an AND.
			for i, segmentFilter := range topicFilter {
				if segmentFilter.Wildcard != nil || segmentFilter.ScVal == nil {
					continue // skip wildcards but keep position of segment
				}
				conditions = append(conditions, store.TopicCondition{
					Column: i + 1, // columns start with `topic1`
					Value:  segmentFilter.ScVal,
				})
			}

			// This means a topic full of wildcards, making it dominate any
			// other filter.
			if len(conditions) == 0 {
				return nil
			}
			topicFilters = append(topicFilters, conditions)
		}
	}

	return topicFilters
}

// TODO: remove this linter exclusions
//
//nolint:cyclop,funlen
func (h eventsRPCHandler) getEvents(ctx context.Context, request protocol.GetEventsRequest,
) (protocol.GetEventsResponse, error) {
	if err := request.Valid(h.maxLimit); err != nil {
		return protocol.GetEventsResponse{}, &jrpc2.Error{
			Code: jrpc2.InvalidParams, Message: err.Error(),
		}
	}

	// Range and scan share one snapshot, so a validated startLedger cannot be
	// trimmed away between the check and the query.
	readTx, err := h.dbReader.NewTx(ctx)
	if err != nil {
		return protocol.GetEventsResponse{}, &jrpc2.Error{
			Code: jrpc2.InternalError, Message: err.Error(),
		}
	}
	defer func() {
		_ = readTx.Done()
	}()

	ledgerRange, err := readTx.GetLedgerRange(ctx)
	if err != nil {
		return protocol.GetEventsResponse{}, &jrpc2.Error{
			Code: jrpc2.InternalError, Message: err.Error(),
		}
	}

	start := protocol.Cursor{Ledger: request.StartLedger}
	limit := h.defaultLimit
	if request.Pagination != nil {
		if request.Pagination.Cursor != nil {
			start = *request.Pagination.Cursor
			// increment event index because, when paginating, we start with the
			// item right after the cursor
			start.Event++
		}
		if request.Pagination.Limit > 0 {
			limit = request.Pagination.Limit
		}
	}
	endLedger := start.Ledger + LedgerScanLimit
	// endLedger should not exceed ledger retention window
	endLedger = min(ledgerRange.LastLedger.Sequence+1, endLedger)
	if request.EndLedger != 0 {
		endLedger = min(request.EndLedger, endLedger)
	}

	end := protocol.Cursor{Ledger: endLedger}
	cursorRange := protocol.CursorRange{Start: start, End: end}

	if start.Ledger < ledgerRange.FirstLedger.Sequence || start.Ledger > ledgerRange.LastLedger.Sequence {
		return protocol.GetEventsResponse{}, &jrpc2.Error{
			Code: jrpc2.InvalidRequest,
			Message: fmt.Sprintf(
				"startLedger must be within the ledger range: %d - %d",
				ledgerRange.FirstLedger.Sequence,
				ledgerRange.LastLedger.Sequence,
			),
		}
	}

	contractIDs, err := combineContractIDs(request.Filters)
	if err != nil {
		return protocol.GetEventsResponse{}, &jrpc2.Error{
			Code: jrpc2.InvalidParams, Message: err.Error(),
		}
	}

	topics := combineTopics(request.Filters)
	eventTypes := combineEventTypes(request.Filters)

	matcher, err := protocol.NewEventMatcher(request.Filters)
	if err != nil {
		return protocol.GetEventsResponse{}, &jrpc2.Error{
			Code: jrpc2.InvalidParams, Message: err.Error(),
		}
	}

	results := []protocol.EventInfo{}
	budget := PageBudget{Limit: h.maxResponseBytes}
	// procErr keeps the callback's own error out of the reader's wrap, as getTransactions does,
	// so a render failure stays a plain (system) error rather than an InvalidRequest.
	var procErr error
	var eventViewScanFunction store.ViewScanFunction = func(
		eventView xdr.DiagnosticEventView, cursor protocol.Cursor, ledgerCloseTimestamp int64, txHash *xdr.Hash,
	) (bool, error) {
		if procErr = ctx.Err(); procErr != nil {
			return false, procErr
		}
		var event xdr.ContractEventView
		if event, procErr = eventView.Event(); procErr != nil {
			return false, procErr
		}
		// Topics wait until type and contract id pass, so a rejected event never sizes them.
		head, err := eventHeader(event)
		if err != nil {
			procErr = errors.Wrap(err, "could not parse event")
			return false, procErr
		}
		if !matcher.MatchesTypeAndContract(head.typ, head.cid) {
			return true, nil
		}
		body, err := eventBody(head.v0)
		if err != nil {
			procErr = errors.Wrap(err, "could not parse event")
			return false, procErr
		}
		if !matcher.Matches(head.typ, head.cid, body.topics) {
			return true, nil
		}
		info, err := eventInfo(head, body, cursor,
			time.Unix(ledgerCloseTimestamp, 0).UTC().Format(time.RFC3339), txHash.HexString(), request.Format)
		if err != nil {
			procErr = errors.Wrap(err, "could not parse event")
			return false, procErr
		}
		if !budget.Fits(EventInfoBytes(info)) {
			return false, nil
		}
		results = append(results, info)
		return uint(len(results)) < limit, nil
	}

	err = readTx.GetEvents(ctx, cursorRange, contractIDs, topics, eventTypes, eventViewScanFunction)
	switch {
	case procErr != nil:
		return protocol.GetEventsResponse{}, procErr
	case err != nil:
		return protocol.GetEventsResponse{}, &jrpc2.Error{
			Code: jrpc2.InvalidRequest, Message: err.Error(),
		}
	}

	var cursor string
	if budget.full || uint(len(results)) == limit {
		lastEvent := results[len(results)-1]
		cursor = lastEvent.ID
	} else {
		// cursor represents end of the search window if events does not reach limit
		// here endLedger is always exclusive when fetching events
		// so search window is max Cursor value with endLedger - 1
		maxCursor := protocol.MaxCursor
		maxCursor.Ledger = endLedger - 1
		cursor = maxCursor.String()
	}

	return protocol.GetEventsResponse{
		Events: results,
		Cursor: cursor,

		LatestLedger:          ledgerRange.LastLedger.Sequence,
		OldestLedger:          ledgerRange.FirstLedger.Sequence,
		LatestLedgerCloseTime: ledgerRange.LastLedger.CloseTime,
		OldestLedgerCloseTime: ledgerRange.FirstLedger.CloseTime,
	}, nil
}

// EventInfoFromView renders one stored ContractEvent into the v1 wire type; rpcv2's eventsapi wraps it.
func EventInfoFromView(
	ev xdr.ContractEventView, cursor protocol.Cursor, ledgerClosedAt, txHash, format string,
) (protocol.EventInfo, error) {
	head, err := eventHeader(ev)
	if err != nil {
		return protocol.EventInfo{}, errors.Wrap(err, "malformed event")
	}
	body, err := eventBody(head.v0)
	if err != nil {
		return protocol.EventInfo{}, errors.Wrap(err, "malformed event")
	}
	return eventInfo(head, body, cursor, ledgerClosedAt, txHash, format)
}

// eventHead is the event's fields above the body, plus the V0 body view to read on demand.
type eventHead struct {
	typ xdr.ContractEventType
	cid []byte // nil when absent
	v0  xdr.ContractEventV0View
}

// eventV0 is the V0 body's topics and data as raw XDR.
type eventV0 struct {
	topics [][]byte
	data   []byte
}

// eventHeader reads type and contract id without sizing the body.
func eventHeader(ev xdr.ContractEventView) (eventHead, error) {
	var h eventHead
	typ, err := ev.Type()
	if err != nil {
		return eventHead{}, err
	}
	if h.typ, err = typ.Value(); err != nil {
		return eventHead{}, err
	}
	opt, err := ev.ContractId()
	if err != nil {
		return eventHead{}, err
	}
	cid, ok, err := opt.Unwrap()
	if err != nil {
		return eventHead{}, err
	}
	if ok {
		if h.cid, err = cid.Raw(); err != nil {
			return eventHead{}, err
		}
	}
	body, err := ev.Body()
	if err != nil {
		return eventHead{}, err
	}
	if h.v0, err = body.V0(); err != nil { // fails on a non-V0 body, replacing "unknown event version"
		return eventHead{}, err
	}
	return h, nil
}

// eventBody locates topics and data in one pass; All returns each topic trimmed.
func eventBody(v0 xdr.ContractEventV0View) (eventV0, error) {
	f, err := v0.Fields()
	if err != nil {
		return eventV0{}, err
	}
	views, err := f.Topics.All()
	if err != nil {
		return eventV0{}, err
	}
	b := eventV0{topics: make([][]byte, len(views)), data: []byte(f.Data)}
	for i, t := range views {
		b.topics[i] = []byte(t)
	}
	return b, nil
}

func eventInfo(
	head eventHead, body eventV0, cursor protocol.Cursor, ledgerClosedAt, txHash, format string,
) (protocol.EventInfo, error) {
	if cursor.Ledger > math.MaxInt32 {
		return protocol.EventInfo{}, fmt.Errorf("ledger sequence %d exceeds supported range", cursor.Ledger)
	}

	typeName, ok := eventTypeName(head.typ)
	if !ok {
		return protocol.EventInfo{}, fmt.Errorf("unknown XDR ContractEventType type: %d", head.typ)
	}

	info := protocol.EventInfo{
		EventType:       typeName,
		Ledger:          int32(cursor.Ledger),
		LedgerClosedAt:  ledgerClosedAt,
		ID:              cursor.String(),
		TransactionHash: txHash,
		OpIndex:         cursor.Op,
		TxIndex:         cursor.Tx,
	}

	if head.cid != nil {
		info.ContractID = strkey.MustEncode(strkey.VersionByteContract, head.cid)
	}

	var err error
	switch format {
	case protocol.FormatJSON:
		if info.TopicJSON, err = jsonifySlice(xdr.ScVal{}, body.topics); err != nil {
			return protocol.EventInfo{}, err
		}
		if info.ValueJSON, err = xdr2json.ConvertBytes(xdr.ScVal{}, body.data); err != nil {
			return protocol.EventInfo{}, err
		}
	default:
		info.TopicXDR = make([]string, len(body.topics))
		for i, segment := range body.topics {
			info.TopicXDR[i] = base64.StdEncoding.EncodeToString(segment)
		}
		info.ValueXDR = base64.StdEncoding.EncodeToString(body.data)
	}

	return info, nil
}

// eventTypeName is protocol.GetEventTypeFromEventTypeXDR without the per-call map; ok is false outside the enum.
func eventTypeName(t xdr.ContractEventType) (string, bool) {
	switch t {
	case xdr.ContractEventTypeSystem:
		return protocol.EventTypeSystem, true
	case xdr.ContractEventTypeContract:
		return protocol.EventTypeContract, true
	case xdr.ContractEventTypeDiagnostic:
		return protocol.EventTypeDiagnostic, true
	default:
		return "", false
	}
}

// NewGetEventsHandler returns a json rpc handler to fetch and filter events
func NewGetEventsHandler(
	logger *log.Entry,
	dbReader store.EventReader,
	maxLimit uint,
	defaultLimit uint,
	maxResponseBytes uint,
) jrpc2.Handler {
	eventsHandler := eventsRPCHandler{
		dbReader:         dbReader,
		maxLimit:         maxLimit,
		defaultLimit:     defaultLimit,
		maxResponseBytes: ByteLimit(maxResponseBytes),
		logger:           logger,
	}
	return NewHandler(eventsHandler.getEvents)
}
