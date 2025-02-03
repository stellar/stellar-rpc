package methods

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/creachadair/jrpc2"
	"github.com/pkg/errors"

	"github.com/stellar/go/strkey"
	"github.com/stellar/go/support/collections/set"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/xdr2json"
	"github.com/stellar/stellar-rpc/protocol"
)

const (
	LedgerScanLimit = 10000
	maxEventTypes   = 3
)

type eventsRPCHandler struct {
	dbReader     db.EventReader
	maxLimit     uint
	defaultLimit uint
	logger       *log.Entry
	ledgerReader db.LedgerReader
}

func combineContractIDs(filters []protocol.EventFilter) ([][]byte, error) {
	contractIDSet := set.NewSet[string](protocol.MaxFiltersLimit * protocol.MaxContractIDsLimit)
	contractIDs := make([][]byte, 0, len(contractIDSet))

	for _, filter := range filters {
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

func combineTopics(filters []protocol.EventFilter) ([][][]byte, error) {
	encodedTopicsList := make([][][]byte, protocol.MaxTopicCount)

	for _, filter := range filters {
		if len(filter.Topics) == 0 {
			return [][][]byte{}, nil
		}

		for _, topicFilter := range filter.Topics {
			for i, segmentFilter := range topicFilter {
				if segmentFilter.Wildcard == nil && segmentFilter.ScVal != nil {
					encodedTopic, err := segmentFilter.ScVal.MarshalBinary()
					if err != nil {
						return [][][]byte{}, fmt.Errorf("failed to marshal segment: %w", err)
					}
					encodedTopicsList[i] = append(encodedTopicsList[i], encodedTopic)
				}
			}
		}
	}

	return encodedTopicsList, nil
}

type entry struct {
	cursor               protocol.Cursor
	ledgerCloseTimestamp int64
	event                xdr.DiagnosticEvent
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
			// increment event index because, when paginating, we start with the item right after the cursor
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
	eventScanFunction := func(
		event xdr.DiagnosticEvent, cursor protocol.Cursor, ledgerCloseTimestamp int64, txHash *xdr.Hash,
	) bool {
		if request.Matches(event) {
			found = append(found, entry{cursor, ledgerCloseTimestamp, event, txHash})
		}
		return uint(len(found)) < limit
	}

	err = h.dbReader.GetEvents(ctx, cursorRange, contractIDs, topics, eventTypes, eventScanFunction)
	if err != nil {
		return protocol.GetEventsResponse{}, &jrpc2.Error{
			Code: jrpc2.InvalidRequest, Message: err.Error(),
		}
	}

	results := make([]protocol.EventInfo, 0, len(found))
	for _, entry := range found {
		info, err := eventInfoForEvent(
			entry.event,
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
		LatestLedger: ledgerRange.LastLedger.Sequence,
		Events:       results,
		Cursor:       cursor,
	}, nil
}

func eventInfoForEvent(
	event xdr.DiagnosticEvent,
	cursor protocol.Cursor,
	ledgerClosedAt, txHash, format string,
) (protocol.EventInfo, error) {
	v0, ok := event.Event.Body.GetV0()
	if !ok {
		return protocol.EventInfo{}, errors.New("unknown event version")
	}

	eventType, ok := protocol.GetEventTypeFromEventTypeXDR()[event.Event.Type]
	if !ok {
		return protocol.EventInfo{}, fmt.Errorf("unknown XDR ContractEventType type: %d", event.Event.Type)
	}

	info := protocol.EventInfo{
		EventType:                eventType,
		Ledger:                   int32(cursor.Ledger),
		LedgerClosedAt:           ledgerClosedAt,
		ID:                       cursor.String(),
		PagingToken:              cursor.String(),
		InSuccessfulContractCall: event.InSuccessfulContractCall,
		TransactionHash:          txHash,
	}

	switch format {
	case protocol.FormatJSON:
		// json encode the topic
		info.TopicJSON = make([]json.RawMessage, 0, protocol.MaxTopicCount)
		for _, topic := range v0.Topics {
			topic, err := xdr2json.ConvertInterface(topic)
			if err != nil {
				return protocol.EventInfo{}, err
			}
			info.TopicJSON = append(info.TopicJSON, topic)
		}

		var convErr error
		info.ValueJSON, convErr = xdr2json.ConvertInterface(v0.Data)
		if convErr != nil {
			return protocol.EventInfo{}, convErr
		}

	default:
		// base64-xdr encode the topic
		topic := make([]string, 0, protocol.MaxTopicCount)
		for _, segment := range v0.Topics {
			seg, err := xdr.MarshalBase64(segment)
			if err != nil {
				return protocol.EventInfo{}, err
			}
			topic = append(topic, seg)
		}

		// base64-xdr encode the data
		data, err := xdr.MarshalBase64(v0.Data)
		if err != nil {
			return protocol.EventInfo{}, err
		}

		info.TopicXDR = topic
		info.ValueXDR = data
	}

	if event.Event.ContractId != nil {
		info.ContractID = strkey.MustEncode(
			strkey.VersionByteContract,
			(*event.Event.ContractId)[:])
	}
	return info, nil
}

// NewGetEventsHandler returns a json rpc handler to fetch and filter events
func NewGetEventsHandler(
	logger *log.Entry,
	dbReader db.EventReader,
	maxLimit uint,
	defaultLimit uint,
	ledgerReader db.LedgerReader,
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
