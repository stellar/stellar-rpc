package query

import "github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/eventquery"

// The cursor, scope, page and range types are the shared eventquery
// package's; these names keep this package's callers unchanged.
type (
	EventCursor   = eventquery.EventCursor
	EventScope    = eventquery.EventScope
	EventPosition = eventquery.EventPosition
	Direction     = eventquery.Direction
	RangeError    = eventquery.RangeError
	RangeOutcome  = eventquery.RangeOutcome
	ScanStatus    = eventquery.ScanStatus
	EventPage     = eventquery.EventPage
	WalkResult    = eventquery.WalkResult
)

const (
	Ascending  = eventquery.Ascending
	Descending = eventquery.Descending

	RangeServe        = eventquery.RangeServe
	RangeBeyondLatest = eventquery.RangeBeyondLatest
	RangeBelowFloor   = eventquery.RangeBelowFloor

	ScanHasMore           = eventquery.ScanHasMore
	ScanComplete          = eventquery.ScanComplete
	ScanWaitingForLedgers = eventquery.ScanWaitingForLedgers
	ScanOldestReached     = eventquery.ScanOldestReached

	MaxScanLedgers = eventquery.MaxScanLedgers
)

var (
	ErrCursorMalformed      = eventquery.ErrCursorMalformed
	ErrCursorUnknownVersion = eventquery.ErrCursorUnknownVersion
	ErrInvertedRange        = eventquery.ErrInvertedRange
	ErrPositionMismatch     = eventquery.ErrPositionMismatch
	ErrInvalidLimit         = eventquery.ErrInvalidLimit
)

// DecodeEventCursor is eventquery.DecodeEventCursor.
func DecodeEventCursor(token string) (*EventCursor, error) {
	return eventquery.DecodeEventCursor(token)
}
