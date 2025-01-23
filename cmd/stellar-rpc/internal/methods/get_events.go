package methods

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/creachadair/jrpc2"
	"github.com/pkg/errors"

	"github.com/stellar/go/strkey"
	"github.com/stellar/go/support/collections/set"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/xdr2json"
)

const (
	LedgerScanLimit     = 10000
	maxContractIDsLimit = 5
	maxTopicsLimit      = 5
	maxFiltersLimit     = 5
	maxEventTypes       = 3
)

type eventTypeSet map[string]interface{}

func (e eventTypeSet) valid() error {
	for key := range e {
		switch key {
		case EventTypeSystem, EventTypeContract, EventTypeDiagnostic:
			// ok
		default:
			return errors.New("if set, type must be either 'system', 'contract' or 'diagnostic'")
		}
	}
	return nil
}

func (e *eventTypeSet) UnmarshalJSON(data []byte) error {
	if len(data) == 0 {
		*e = map[string]interface{}{}
		return nil
	}
	var joined string
	if err := json.Unmarshal(data, &joined); err != nil {
		return err
	}
	*e = map[string]interface{}{}
	if len(joined) == 0 {
		return nil
	}
	for _, key := range strings.Split(joined, ",") {
		(*e)[key] = nil
	}
	return nil
}

func (e eventTypeSet) MarshalJSON() ([]byte, error) {
	keys := make([]string, 0, len(e))
	for key := range e {
		keys = append(keys, key)
	}
	return json.Marshal(strings.Join(keys, ","))
}

func (e eventTypeSet) Keys() []string {
	keys := make([]string, 0, len(e))
	for key := range e {
		keys = append(keys, key)
	}
	return keys
}

func (e eventTypeSet) matches(event xdr.ContractEvent) bool {
	if len(e) == 0 {
		return true
	}
	_, ok := e[getEventTypeFromEventTypeXDR()[event.Type]]
	return ok
}

type EventInfo struct {
	EventType      string `json:"type"`
	Ledger         int32  `json:"ledger"`
	LedgerClosedAt string `json:"ledgerClosedAt"`
	ContractID     string `json:"contractId"`
	ID             string `json:"id"`

	// Deprecated: PagingToken field is deprecated, please use Cursor at top level for pagination
	PagingToken              string `json:"pagingToken"`
	InSuccessfulContractCall bool   `json:"inSuccessfulContractCall"`
	TransactionHash          string `json:"txHash"`

	// TopicXDR is a base64-encoded list of ScVals
	TopicXDR  []string          `json:"topic,omitempty"`
	TopicJSON []json.RawMessage `json:"topicJson,omitempty"`

	// ValueXDR is a base64-encoded ScVal
	ValueXDR  string          `json:"value,omitempty"`
	ValueJSON json.RawMessage `json:"valueJson,omitempty"`
}

type GetEventsRequest struct {
	StartLedger uint32             `json:"startLedger,omitempty"`
	EndLedger   uint32             `json:"endLedger,omitempty"`
	Filters     []EventFilter      `json:"filters"`
	Pagination  *PaginationOptions `json:"pagination,omitempty"`
	Format      string             `json:"xdrFormat,omitempty"`
}

func (g *GetEventsRequest) Valid(maxLimit uint) error {
	if err := IsValidFormat(g.Format); err != nil {
		return err
	}

	// Validate the paging limit (if it exists)
	if g.Pagination != nil && g.Pagination.Cursor != nil {
		if g.StartLedger != 0 || g.EndLedger != 0 {
			return errors.New("ledger ranges and cursor cannot both be set")
		}
	} else if g.StartLedger <= 0 {
		// Note: Endledger == 0 indicates it's unset (unlimited)
		return errors.New("startLedger must be positive")
	}

	if g.Pagination != nil && g.Pagination.Limit > maxLimit {
		return fmt.Errorf("limit must not exceed %d", maxLimit)
	}

	// Validate filters
	if len(g.Filters) > maxFiltersLimit {
		return errors.New("maximum 5 filters per request")
	}
	for i, filter := range g.Filters {
		if err := filter.Valid(); err != nil {
			return fmt.Errorf("filter %d invalid: %w", i+1, err)
		}
	}

	return nil
}

func (g *GetEventsRequest) Matches(event xdr.DiagnosticEvent) bool {
	if len(g.Filters) == 0 {
		return true
	}
	for _, filter := range g.Filters {
		if filter.Matches(event) {
			return true
		}
	}
	return false
}

const (
	EventTypeSystem     = "system"
	EventTypeContract   = "contract"
	EventTypeDiagnostic = "diagnostic"
)

func getEventTypeFromEventTypeXDR() map[xdr.ContractEventType]string {
	return map[xdr.ContractEventType]string{
		xdr.ContractEventTypeSystem:     EventTypeSystem,
		xdr.ContractEventTypeContract:   EventTypeContract,
		xdr.ContractEventTypeDiagnostic: EventTypeDiagnostic,
	}
}

func getEventTypeXDRFromEventType() map[string]xdr.ContractEventType {
	return map[string]xdr.ContractEventType{
		EventTypeSystem:     xdr.ContractEventTypeSystem,
		EventTypeContract:   xdr.ContractEventTypeContract,
		EventTypeDiagnostic: xdr.ContractEventTypeDiagnostic,
	}
}

type EventFilter struct {
	EventType   eventTypeSet  `json:"type,omitempty"`
	ContractIDs []string      `json:"contractIds,omitempty"`
	Topics      []TopicFilter `json:"topics,omitempty"`
}

func (e *EventFilter) Valid() error {
	if err := e.EventType.valid(); err != nil {
		return errors.Wrap(err, "filter type invalid")
	}
	if len(e.ContractIDs) > maxContractIDsLimit {
		return errors.New("maximum 5 contract IDs per filter")
	}
	if len(e.Topics) > maxTopicsLimit {
		return errors.New("maximum 5 topics per filter")
	}
	for i, id := range e.ContractIDs {
		_, err := strkey.Decode(strkey.VersionByteContract, id)
		if err != nil {
			return fmt.Errorf("contract ID %d invalid", i+1)
		}
	}
	for i, topic := range e.Topics {
		if err := topic.Valid(); err != nil {
			return fmt.Errorf("topic %d invalid: %w", i+1, err)
		}
	}
	return nil
}

func (e *EventFilter) Matches(event xdr.DiagnosticEvent) bool {
	return e.EventType.matches(event.Event) && e.matchesContractIDs(event.Event) && e.matchesTopics(event.Event)
}

func (e *EventFilter) matchesContractIDs(event xdr.ContractEvent) bool {
	if len(e.ContractIDs) == 0 {
		return true
	}
	if event.ContractId == nil {
		return false
	}
	needle := strkey.MustEncode(strkey.VersionByteContract, (*event.ContractId)[:])
	for _, id := range e.ContractIDs {
		if id == needle {
			return true
		}
	}
	return false
}

func (e *EventFilter) matchesTopics(event xdr.ContractEvent) bool {
	if len(e.Topics) == 0 {
		return true
	}
	v0, ok := event.Body.GetV0()
	if !ok {
		return false
	}
	for _, topicFilter := range e.Topics {
		if topicFilter.Matches(v0.Topics) {
			return true
		}
	}
	return false
}

type TopicFilter []SegmentFilter

func (t *TopicFilter) Valid() error {
	if len(*t) < db.MinTopicCount {
		return errors.New("topic must have at least one segment")
	}
	if len(*t) > db.MaxTopicCount {
		return errors.New("topic cannot have more than 4 segments")
	}
	for i, segment := range *t {
		if err := segment.Valid(); err != nil {
			return fmt.Errorf("segment %d invalid: %w", i+1, err)
		}
	}
	return nil
}

// An event matches a topic filter iff:
//   - the event has EXACTLY as many topic segments as the filter AND
//   - each segment either: matches exactly OR is a wildcard.
func (t TopicFilter) Matches(event []xdr.ScVal) bool {
	if len(event) != len(t) {
		return false
	}

	for i, segmentFilter := range t {
		if !segmentFilter.Matches(event[i]) {
			return false
		}
	}

	return true
}

type SegmentFilter struct {
	wildcard *string
	scval    *xdr.ScVal
}

func (s *SegmentFilter) Matches(segment xdr.ScVal) bool {
	if s.wildcard != nil && *s.wildcard == "*" {
		return true
	} else if s.scval != nil {
		if !s.scval.Equals(segment) {
			return false
		}
	} else {
		panic("invalid segmentFilter")
	}

	return true
}

func (s *SegmentFilter) Valid() error {
	if s.wildcard != nil && s.scval != nil {
		return errors.New("cannot set both wildcard and scval")
	}
	if s.wildcard == nil && s.scval == nil {
		return errors.New("must set either wildcard or scval")
	}
	if s.wildcard != nil && *s.wildcard != "*" {
		return errors.New("wildcard must be '*'")
	}
	return nil
}

func (s *SegmentFilter) UnmarshalJSON(p []byte) error {
	s.wildcard = nil
	s.scval = nil

	var tmp string
	if err := json.Unmarshal(p, &tmp); err != nil {
		return err
	}
	if tmp == "*" {
		s.wildcard = &tmp
	} else {
		var out xdr.ScVal
		if err := xdr.SafeUnmarshalBase64(tmp, &out); err != nil {
			return err
		}
		s.scval = &out
	}
	return nil
}

type PaginationOptions struct {
	Cursor *db.Cursor `json:"cursor,omitempty"`
	Limit  uint       `json:"limit,omitempty"`
}

type GetEventsResponse struct {
	Events       []EventInfo `json:"events"`
	LatestLedger uint32      `json:"latestLedger"`
	// Cursor represents last populated event ID if total events reach the limit
	// or end of the search window
	Cursor string `json:"cursor"`
}

type eventsRPCHandler struct {
	dbReader     db.EventReader
	maxLimit     uint
	defaultLimit uint
	logger       *log.Entry
	ledgerReader db.LedgerReader
}

func combineContractIDs(filters []EventFilter) ([][]byte, error) {
	contractIDSet := set.NewSet[string](maxFiltersLimit * maxContractIDsLimit)
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

func combineEventTypes(filters []EventFilter) []int {
	eventTypes := set.NewSet[int](maxEventTypes)

	for _, filter := range filters {
		for _, eventType := range filter.EventType.Keys() {
			eventTypeXDR := getEventTypeXDRFromEventType()[eventType]
			eventTypes.Add(int(eventTypeXDR))
		}
	}
	uniqueEventTypes := make([]int, 0, maxEventTypes)
	for eventType := range eventTypes {
		uniqueEventTypes = append(uniqueEventTypes, eventType)
	}
	return uniqueEventTypes
}

func combineTopics(filters []EventFilter) ([][][]byte, error) {
	encodedTopicsList := make([][][]byte, db.MaxTopicCount)

	for _, filter := range filters {
		if len(filter.Topics) == 0 {
			return [][][]byte{}, nil
		}

		for _, topicFilter := range filter.Topics {
			for i, segmentFilter := range topicFilter {
				if segmentFilter.wildcard == nil && segmentFilter.scval != nil {
					encodedTopic, err := segmentFilter.scval.MarshalBinary()
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
	cursor               db.Cursor
	ledgerCloseTimestamp int64
	event                xdr.DiagnosticEvent
	txHash               *xdr.Hash
}

func (h eventsRPCHandler) getEvents(ctx context.Context, request GetEventsRequest) (GetEventsResponse, error) {
	if err := request.Valid(h.maxLimit); err != nil {
		return GetEventsResponse{}, &jrpc2.Error{
			Code: jrpc2.InvalidParams, Message: err.Error(),
		}
	}

	ledgerRange, err := h.ledgerReader.GetLedgerRange(ctx)
	if err != nil {
		return GetEventsResponse{}, &jrpc2.Error{
			Code: jrpc2.InternalError, Message: err.Error(),
		}
	}

	start := db.Cursor{Ledger: request.StartLedger}
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

	end := db.Cursor{Ledger: endLedger}
	cursorRange := db.CursorRange{Start: start, End: end}

	if start.Ledger < ledgerRange.FirstLedger.Sequence || start.Ledger > ledgerRange.LastLedger.Sequence {
		return GetEventsResponse{}, &jrpc2.Error{
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
		return GetEventsResponse{}, &jrpc2.Error{
			Code: jrpc2.InvalidParams, Message: err.Error(),
		}
	}

	topics, err := combineTopics(request.Filters)
	if err != nil {
		return GetEventsResponse{}, &jrpc2.Error{
			Code: jrpc2.InvalidParams, Message: err.Error(),
		}
	}

	eventTypes := combineEventTypes(request.Filters)

	// Scan function to apply filters
	eventScanFunction := func(
		event xdr.DiagnosticEvent, cursor db.Cursor, ledgerCloseTimestamp int64, txHash *xdr.Hash,
	) bool {
		if request.Matches(event) {
			found = append(found, entry{cursor, ledgerCloseTimestamp, event, txHash})
		}
		return uint(len(found)) < limit
	}

	err = h.dbReader.GetEvents(ctx, cursorRange, contractIDs, topics, eventTypes, eventScanFunction)
	if err != nil {
		return GetEventsResponse{}, &jrpc2.Error{
			Code: jrpc2.InvalidRequest, Message: err.Error(),
		}
	}

	results := make([]EventInfo, 0, len(found))
	for _, entry := range found {
		info, err := eventInfoForEvent(
			entry.event,
			entry.cursor,
			time.Unix(entry.ledgerCloseTimestamp, 0).UTC().Format(time.RFC3339),
			entry.txHash.HexString(),
			request.Format,
		)
		if err != nil {
			return GetEventsResponse{}, errors.Wrap(err, "could not parse event")
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
		maxCursor := db.MaxCursor
		maxCursor.Ledger = endLedger - 1
		cursor = maxCursor.String()
	}

	return GetEventsResponse{
		LatestLedger: ledgerRange.LastLedger.Sequence,
		Events:       results,
		Cursor:       cursor,
	}, nil
}

func eventInfoForEvent(
	event xdr.DiagnosticEvent,
	cursor db.Cursor,
	ledgerClosedAt, txHash, format string,
) (EventInfo, error) {
	v0, ok := event.Event.Body.GetV0()
	if !ok {
		return EventInfo{}, errors.New("unknown event version")
	}

	eventType, ok := getEventTypeFromEventTypeXDR()[event.Event.Type]
	if !ok {
		return EventInfo{}, fmt.Errorf("unknown XDR ContractEventType type: %d", event.Event.Type)
	}

	info := EventInfo{
		EventType:                eventType,
		Ledger:                   int32(cursor.Ledger),
		LedgerClosedAt:           ledgerClosedAt,
		ID:                       cursor.String(),
		PagingToken:              cursor.String(),
		InSuccessfulContractCall: event.InSuccessfulContractCall,
		TransactionHash:          txHash,
	}

	switch format {
	case FormatJSON:
		// json encode the topic
		info.TopicJSON = make([]json.RawMessage, 0, db.MaxTopicCount)
		for _, topic := range v0.Topics {
			topic, err := xdr2json.ConvertInterface(topic)
			if err != nil {
				return EventInfo{}, err
			}
			info.TopicJSON = append(info.TopicJSON, topic)
		}

		var convErr error
		info.ValueJSON, convErr = xdr2json.ConvertInterface(v0.Data)
		if convErr != nil {
			return EventInfo{}, convErr
		}

	default:
		// base64-xdr encode the topic
		topic := make([]string, 0, db.MaxTopicCount)
		for _, segment := range v0.Topics {
			seg, err := xdr.MarshalBase64(segment)
			if err != nil {
				return EventInfo{}, err
			}
			topic = append(topic, seg)
		}

		// base64-xdr encode the data
		data, err := xdr.MarshalBase64(v0.Data)
		if err != nil {
			return EventInfo{}, err
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
