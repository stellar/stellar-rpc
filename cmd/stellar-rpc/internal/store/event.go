package store

import (
	"context"
	"fmt"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/toid"
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

// StageSentinels maps a V4 top-level TransactionEvent stage to the
// (TxIdx, OpIdx) sentinels the v1 getEvents cursor encoding uses. applyIdx is
// the 1-based transaction apply index (consumed by the AfterTx arm). This is
// the SINGLE definition of the cursor-encoding policy for both storage
// backends: the full-history view path (PayloadsFromLedgerEvents) and the legacy
// SQL path (sqlitedb/event.go's InsertEvents) both consume it, so a new stage or
// sentinel revision lands in one place and getEvents cursors stay compatible
// across backends by construction.
func StageSentinels(stage xdr.TransactionEventStage, applyIdx uint32) (uint32, uint32, error) {
	switch stage {
	case xdr.TransactionEventStageTransactionEventStageBeforeAllTxs:
		return 0, 0, nil
	case xdr.TransactionEventStageTransactionEventStageAfterAllTxs:
		return uint32(toid.TransactionMask), 0, nil
	case xdr.TransactionEventStageTransactionEventStageAfterTx:
		return applyIdx, uint32(toid.OperationMask), nil
	default:
		return 0, 0, fmt.Errorf("events: unhandled event stage %v", stage)
	}
}
