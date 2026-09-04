package methods

import (
	"context"
	"encoding/base64"
	"encoding/json"
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
	dbReader     store.EventReader
	maxLimit     uint
	defaultLimit uint
	logger       *log.Entry
	ledgerReader store.LedgerReader
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

func combineTopics(filters []protocol.EventFilter) (store.TopicFilters, error) {
	topicFilters := make(store.TopicFilters, 0, len(filters))

	for _, filter := range filters {
		if len(filter.Topics) == 0 {
			return nil, nil
		}

		// Each topic is an OR...
		for _, topicFilter := range filter.Topics {
			conditions := make(store.TopicFilter, 0, len(topicFilter))
			// ...but each segment within a topic is an AND.
			for i, segmentFilter := range topicFilter {
				if segmentFilter.Wildcard != nil || segmentFilter.ScVal == nil {
					continue // skip wildcards but keep position of segment
				}
				encodedTopic, err := segmentFilter.ScVal.MarshalBinary()
				if err != nil {
					return nil, fmt.Errorf("failed to marshal segment: %w", err)
				}
				conditions = append(conditions, store.TopicCondition{
					Column: i + 1, // columns start with `topic1`
					Value:  encodedTopic,
				})
			}

			// This means a topic full of wildcards, making it dominate any
			// other filter.
			if len(conditions) == 0 {
				return nil, nil
			}
			topicFilters = append(topicFilters, conditions)
		}
	}

	return topicFilters, nil
}

type entry struct {
	cursor               protocol.Cursor
	ledgerCloseTimestamp int64
	eventView            xdr.DiagnosticEventView
	txHash               *xdr.Hash
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

	ledgerRange, err := h.ledgerReader.GetLedgerRange(ctx)
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

	found := make([]entry, 0, limit)

	contractIDs, err := combineContractIDs(request.Filters)
	if err != nil {
		return protocol.GetEventsResponse{}, &jrpc2.Error{
			Code: jrpc2.InvalidParams, Message: err.Error(),
		}
	}

	topics, err := combineTopics(request.Filters)
	if err != nil {
		return protocol.GetEventsResponse{}, &jrpc2.Error{
			Code: jrpc2.InvalidParams, Message: err.Error(),
		}
	}

	eventTypes := combineEventTypes(request.Filters)

	// Scan function to apply filters
	var eventViewScanFunction store.ViewScanFunction = func(
		eventView xdr.DiagnosticEventView, cursor protocol.Cursor, ledgerCloseTimestamp int64, txHash *xdr.Hash,
	) bool {
		if request.Matches(eventView) {
			found = append(found, entry{cursor, ledgerCloseTimestamp, eventView, txHash})
		}
		return uint(len(found)) < limit
	}

	err = h.dbReader.GetEvents(ctx, cursorRange, contractIDs, topics, eventTypes, eventViewScanFunction)
	if err != nil {
		return protocol.GetEventsResponse{}, &jrpc2.Error{
			Code: jrpc2.InvalidRequest, Message: err.Error(),
		}
	}

	results := make([]protocol.EventInfo, 0, len(found))
	for _, entry := range found {
		info, err := eventInfoForEvent(
			entry.eventView,
			entry.cursor,
			time.Unix(entry.ledgerCloseTimestamp, 0).UTC().Format(time.RFC3339),
			entry.txHash.HexString(),
			request.Format,
		)
		if err != nil {
			return protocol.GetEventsResponse{}, errors.Wrap(err, "could not parse event")
		}
		results = append(results, info)
	}

	var cursor string
	if uint(len(results)) == limit {
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

func eventInfoForEvent(
	eventView xdr.DiagnosticEventView,
	cursor protocol.Cursor,
	ledgerClosedAt, txHash, format string,
) (protocol.EventInfo, error) {
	var (
		xdrType xdr.ContractEventType
		topics  [][]byte
		dataRaw []byte
		cidRaw  []byte
	)
	err := xdr.TryVoid(func() {
		ev := eventView.MustEvent()
		xdrType = ev.MustType().MustValue()
		v0 := ev.MustBody().MustV0() // panics on a non-V0 body, replacing "unknown event version"
		for t := range v0.MustTopics().MustIter() {
			topics = append(topics, t.MustRaw())
		}
		dataRaw = v0.MustData().MustRaw()
		if cid, ok := ev.MustContractId().MustUnwrap(); ok {
			cidRaw = cid.MustRaw()
		}
	})
	if err != nil {
		return protocol.EventInfo{}, errors.Wrap(err, "malformed event")
	}

	if cursor.Ledger > math.MaxInt32 {
		return protocol.EventInfo{}, fmt.Errorf("ledger sequence %d exceeds supported range", cursor.Ledger)
	}

	info := protocol.EventInfo{
		EventType:       protocol.GetEventTypeFromEventTypeXDR()[xdrType],
		Ledger:          int32(cursor.Ledger),
		LedgerClosedAt:  ledgerClosedAt,
		ID:              cursor.String(),
		TransactionHash: txHash,
		OpIndex:         cursor.Op,
		TxIndex:         cursor.Tx,
	}

	if cidRaw != nil {
		info.ContractID = strkey.MustEncode(
			strkey.VersionByteContract,
			cidRaw,
		)
	}

	switch format {
	case protocol.FormatJSON:
		// json encode the topic
		info.TopicJSON = make([]json.RawMessage, 0, protocol.MaxTopicCount)
		for _, topicView := range topics {
			topic, err := xdr2json.ConvertBytes(xdr.ScVal{}, topicView)
			if err != nil {
				return protocol.EventInfo{}, err
			}
			info.TopicJSON = append(info.TopicJSON, topic)
		}

		var convErr error
		info.ValueJSON, convErr = xdr2json.ConvertBytes(xdr.ScVal{}, dataRaw)
		if convErr != nil {
			return protocol.EventInfo{}, convErr
		}

	default:
		// base64-xdr encode the topic
		topic := make([]string, 0, protocol.MaxTopicCount)
		for _, segment := range topics {
			topic = append(topic, base64.StdEncoding.EncodeToString(segment))
		}
		info.TopicXDR = topic
		info.ValueXDR = base64.StdEncoding.EncodeToString(dataRaw) // base64-xdr encode the data
	}

	return info, nil
}

// NewGetEventsHandler returns a json rpc handler to fetch and filter events
func NewGetEventsHandler(
	logger *log.Entry,
	dbReader store.EventReader,
	maxLimit uint,
	defaultLimit uint,
	ledgerReader store.LedgerReader,
) jrpc2.Handler {
	eventsHandler := eventsRPCHandler{
		dbReader:     dbReader,
		maxLimit:     maxLimit,
		defaultLimit: defaultLimit,
		logger:       logger,
		ledgerReader: ledgerReader,
	}
	return NewHandler(eventsHandler.getEvents)
}
