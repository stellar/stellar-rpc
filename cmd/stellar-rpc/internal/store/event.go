package store

import (
	"context"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// TopicCondition represents a single topic column equality constraint, where
// the column field matches the `topicN` column within the database.
type TopicCondition struct {
	Column int
	Value  []byte
}

// TopicFilter is a conjunction (AND) of `TopicCondition`s. All conditions must
// match for the filter to be satisfied.
type TopicFilter []TopicCondition

// TopicFilters is a disjunction (OR) of `TopicFilter`s. If any `TopicFilter`
// matches a row, the DB event is considered a candidate for further filtering.
type TopicFilters []TopicFilter

type ScanFunction func(
	event xdr.DiagnosticEvent,
	cursor protocol.Cursor,
	ledgerCloseTimestamp int64,
	txHash *xdr.Hash,
) bool

// EventReader has all the public methods to fetch events from the backend.
type EventReader interface {
	GetEvents(
		ctx context.Context,
		cursorRange protocol.CursorRange,
		contractIDs [][]byte,
		topics TopicFilters,
		eventTypes []int,
		f ScanFunction,
	) error
}
