package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
)

const (
	GetEventsMethodName = "getEvents"
	MaxFiltersLimit     = 5
	MaxTopicsLimit      = 5
	MaxContractIDsLimit = 5
	MinTopicCount       = 1
	MaxTopicCount       = 4
)

type EventInfo struct {
	EventType      string `json:"type"`
	Ledger         int32  `json:"ledger"`
	LedgerClosedAt string `json:"ledgerClosedAt"`
	ContractID     string `json:"contractId"`
	ID             string `json:"id"`
	OpIndex        uint32 `json:"operationIndex"`
	TxIndex        uint32 `json:"transactionIndex"`

	InSuccessfulContractCall bool   `json:"inSuccessfulContractCall"`
	TransactionHash          string `json:"txHash"`

	// TopicXDR is a base64-encoded list of ScVals
	TopicXDR  []string          `json:"topic,omitempty"`
	TopicJSON []json.RawMessage `json:"topicJson,omitempty"`

	// ValueXDR is a base64-encoded ScVal
	ValueXDR  string          `json:"value,omitempty"`
	ValueJSON json.RawMessage `json:"valueJson,omitempty"`
}

const (
	EventTypeSystem     = "system"
	EventTypeContract   = "contract"
	EventTypeDiagnostic = "diagnostic"
)

func GetEventTypeFromEventTypeXDR() map[xdr.ContractEventType]string {
	return map[xdr.ContractEventType]string{
		xdr.ContractEventTypeSystem:     EventTypeSystem,
		xdr.ContractEventTypeContract:   EventTypeContract,
		xdr.ContractEventTypeDiagnostic: EventTypeDiagnostic,
	}
}

func GetEventTypeXDRFromEventType() map[string]xdr.ContractEventType {
	return map[string]xdr.ContractEventType{
		EventTypeSystem:     xdr.ContractEventTypeSystem,
		EventTypeContract:   xdr.ContractEventTypeContract,
		EventTypeDiagnostic: xdr.ContractEventTypeDiagnostic,
	}
}

func (e *EventFilter) Valid() error {
	if err := e.EventType.valid(); err != nil {
		return fmt.Errorf("filter type invalid: %w", err)
	}
	if len(e.ContractIDs) > MaxContractIDsLimit {
		return errors.New("maximum 5 contract IDs per filter")
	}
	if len(e.Topics) > MaxTopicsLimit {
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

type EventTypeSet map[string]interface{} //nolint:recvcheck

func (e EventTypeSet) valid() error {
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

func (e *EventTypeSet) UnmarshalJSON(data []byte) error {
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

func (e EventTypeSet) MarshalJSON() ([]byte, error) {
	keys := make([]string, 0, len(e))
	for key := range e {
		keys = append(keys, key)
	}
	return json.Marshal(strings.Join(keys, ","))
}

func (e EventTypeSet) Keys() []string {
	keys := make([]string, 0, len(e))
	for key := range e {
		keys = append(keys, key)
	}
	return keys
}

func (e EventTypeSet) matches(event xdr.ContractEvent) bool {
	if len(e) == 0 {
		return true
	}
	_, ok := e[GetEventTypeFromEventTypeXDR()[event.Type]]
	return ok
}

type EventFilter struct {
	EventType   EventTypeSet  `json:"type,omitempty"`
	ContractIDs []string      `json:"contractIds,omitempty"`
	Topics      []TopicFilter `json:"topics,omitempty"`
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
		return errors.New("startLedger must be positive")
	}

	if g.Pagination != nil && g.Pagination.Limit > maxLimit {
		return fmt.Errorf("limit must not exceed %d", maxLimit)
	}

	// Validate filters
	if len(g.Filters) > MaxFiltersLimit {
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
	return slices.Contains(e.ContractIDs, needle)
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

// Valid checks if the filter is properly structured:
// - must have at least one segment (excluding trailing flexible wildcard "**").
// - cannot have more than 4 segments total (excluding trailing "**").
// - each segment must be valid. Flexible length wildcard "**" is only allowed at the end.
// Returns an error if any validation fails.
func (t TopicFilter) Valid() error {
	var topics []SegmentFilter
	if t.flexibleTopicLengthAllowed() {
		topics = t[:len(t)-1]
	} else {
		topics = t
	}
	if len(t) < MinTopicCount {
		return errors.New("topic must have at least one segment")
	}
	if len(t) > MaxTopicCount {
		return errors.New("topic cannot have more than 4 segments")
	}
	for i, segment := range topics {
		if err := segment.Valid(); err != nil {
			return fmt.Errorf("segment %d invalid: %w", i+1, err)
		}
	}
	return nil
}

// Check if the last segment is flexible length matching wildcard "**"
func (t TopicFilter) flexibleTopicLengthAllowed() bool {
	if len(t) == 0 {
		return false
	}
	last := t[len(t)-1]
	return last.Wildcard != nil && *last.Wildcard == "**"
}

// Matches a topic filter if:
//   - number of event segments matches the number of filter segments,
//     unless flexible topic length is allowed (indicated by a trailing "**")
//     in which case the event can have more topics than the filter segments.
//   - each segment either: matches exactly OR is a wildcard.
func (t TopicFilter) Matches(event []xdr.ScVal) bool {
	var topics []SegmentFilter
	switch {
	case t.flexibleTopicLengthAllowed():
		if len(event) < len(t)-1 {
			return false
		}
		// exclude flexible segment
		topics = t[:len(t)-1]

	case len(event) != len(t):
		// flexible length matching not allowed, event must match filter length exactly
		return false

	default:
		topics = t
	}

	for i, segmentFilter := range topics {
		if !segmentFilter.Matches(event[i]) {
			return false
		}
	}

	return true
}

type SegmentFilter struct {
	Wildcard *string
	ScVal    *xdr.ScVal
}

func (s *SegmentFilter) Matches(segment xdr.ScVal) bool {
	switch {
	case s.Wildcard != nil && *s.Wildcard == "*":
		return true
	case s.ScVal != nil:
		if !s.ScVal.Equals(segment) {
			return false
		}
	default:
		panic("invalid segmentFilter")
	}

	return true
}

func (s *SegmentFilter) Valid() error {
	if s.Wildcard != nil && s.ScVal != nil {
		return errors.New("cannot set both wildcard and scval")
	}
	if s.Wildcard == nil && s.ScVal == nil {
		return errors.New("must set either wildcard or scval")
	}
	if s.Wildcard != nil && *s.Wildcard != "*" {
		return errors.New("wildcard must be '*'")
	}
	return nil
}

func (s *SegmentFilter) UnmarshalJSON(p []byte) error {
	s.Wildcard = nil
	s.ScVal = nil

	var tmp string
	if err := json.Unmarshal(p, &tmp); err != nil {
		return err
	}
	if tmp == "*" {
		s.Wildcard = &tmp
	} else {
		var out xdr.ScVal
		if err := xdr.SafeUnmarshalBase64(tmp, &out); err != nil {
			return err
		}
		s.ScVal = &out
	}
	return nil
}

type PaginationOptions struct {
	Cursor *Cursor `json:"cursor,omitempty"`
	Limit  uint    `json:"limit,omitempty"`
}

type GetEventsResponse struct {
	Events []EventInfo `json:"events"`
	// Cursor represents last populated event ID if total events reach the limit
	// or end of the search window
	Cursor string `json:"cursor"`

	LatestLedger          uint32 `json:"latestLedger"`
	OldestLedger          uint32 `json:"oldestLedger"`
	LatestLedgerCloseTime int64  `json:"latestLedgerCloseTime,string"`
	OldestLedgerCloseTime int64  `json:"oldestLedgerCloseTime,string"`
}
