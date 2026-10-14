package sqlitedb

// The queryEvents engine over the SQLite event table. It is the rpcv1
// counterpart of the chunk-walking pager in rpcv2/query/event_page.go and
// shares its cursor arithmetic: the same validation, resume bounds, scan
// window, and page assembly, so a cursor minted by either daemon reads the
// same on the other. Only the scan differs: one ordered range query over
// the id column instead of a term-index walk.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/eventquery"
)

// EventQuerier serves queryEvents from the SQLite event table; it is the
// rpcv1 daemon's eventquery.Querier.
type EventQuerier struct {
	log *log.Entry
	db  *DB
}

func NewEventQuerier(log *log.Entry, db *DB) *EventQuerier {
	return &EventQuerier{log: log, db: db}
}

// Begin pins one read snapshot, so the bounds and every row the page reads
// agree, as getEvents does through EventReaderTx.
func (q *EventQuerier) Begin(ctx context.Context) (eventquery.Session, error) {
	tx, err := newLedgerReaderTx(ctx, q.db)
	if err != nil {
		return nil, err
	}
	lr, err := tx.GetLedgerRange(ctx)
	if err != nil {
		return nil, errors.Join(err, tx.Done())
	}
	return &eventQuerySession{
		tx:     tx,
		q:      tx.tx,
		oldest: lr.FirstLedger.Sequence,
		latest: lr.LastLedger.Sequence,
	}, nil
}

// eventQuerySession is one request's read snapshot.
type eventQuerySession struct {
	tx     ledgerReaderTx
	q      readDB
	oldest uint32
	latest uint32
}

func (s *eventQuerySession) OldestLedger() uint32 { return s.oldest }
func (s *eventQuerySession) LatestLedger() uint32 { return s.latest }
func (s *eventQuerySession) Close() error         { return s.tx.Done() }

// QueryEvents advances cursor by one page of at most limit events. Errors
// are the query package's, as eventsapi.EventQuerySession requires.
func (s *eventQuerySession) QueryEvents(
	ctx context.Context, cursor eventquery.EventCursor, limit int,
) (*eventquery.EventPage, error) {
	if err := eventquery.ValidateCursor(&cursor, limit); err != nil {
		return nil, err
	}
	desc := cursor.Scope.Dir == eventquery.Descending
	lo, hi, reenter := eventquery.ResumeBounds(&cursor)
	if lo > hi {
		return &eventquery.EventPage{Next: cursor, Status: eventquery.ScanComplete}, nil
	}
	lo, hi, outcome, err := eventquery.ClampRangeWithin(cursor.Scope.Dir, lo, hi, s.oldest, s.latest)
	if err != nil {
		return nil, err
	}
	switch outcome {
	case eventquery.RangeBeyondLatest:
		return &eventquery.EventPage{Next: cursor, Status: eventquery.ScanWaitingForLedgers}, nil
	case eventquery.RangeBelowFloor:
		return &eventquery.EventPage{Next: cursor, Status: eventquery.ScanOldestReached}, nil
	case eventquery.RangeServe:
	}
	lo, hi, truncated := eventquery.ClipScanWindow(lo, hi, eventquery.MaxScanLedgers, desc)
	walk, err := s.walk(ctx, lo, hi, cursor.Scope.Filters, reenter, desc, limit)
	if err != nil {
		return nil, err
	}
	return eventquery.AssemblePage(cursor, walk, lo, hi, truncated, s.oldest, s.latest), nil
}

// walk scans [lo, hi] in id order and matches each row in Go, the way
// getEvents does: coarse SQL predicates narrow the rows, the matcher
// decides. It stops after limit matches plus one, the one that tells the
// watermark where the next page starts.
func (s *eventQuerySession) walk(
	ctx context.Context, lo, hi uint32, filters []eventquery.Filter,
	reenter *eventquery.EventPosition, desc bool, limit int,
) (eventquery.WalkResult, error) {
	start := protocol.Cursor{Ledger: lo}.String()
	end := protocol.Cursor{Ledger: hi + 1}.String()
	rowQ := sq.Select("id", "event_data", "transaction_hash", "ledger_close_time").
		From(eventTableName).
		Where(sq.GtOrEq{"id": start}).
		Where(sq.Lt{"id": end})
	if reenter != nil {
		// The re-entry point is inside [lo, hi] (eventquery.ResumeBounds starts
		// the window at its ledger). Check it names a stored event, then
		// serve strictly past it in walk order.
		posID, err := s.verifyPosition(ctx, reenter)
		if err != nil {
			return eventquery.WalkResult{}, err
		}
		if desc {
			rowQ = rowQ.Where(sq.Lt{"id": posID})
		} else {
			rowQ = rowQ.Where(sq.Gt{"id": posID})
		}
	}
	rowQ = coarsePredicates(rowQ, filters)
	if desc {
		rowQ = rowQ.OrderBy("id DESC")
	} else {
		rowQ = rowQ.OrderBy("id ASC")
	}

	rows, err := s.q.Query(ctx, rowQ)
	if err != nil {
		return eventquery.WalkResult{}, fmt.Errorf("sqlitedb: queryEvents scan: %w", err)
	}
	defer rows.Close()

	matcher := eventquery.NewMatcher(filters)
	var walk eventquery.WalkResult
	var lastID string
	var row struct {
		id              string
		eventData       sql.RawBytes
		transactionHash sql.RawBytes
		ledgerCloseTime int64
	}
	for rows.Next() {
		if err := rows.Scan(&row.id, &row.eventData, &row.transactionHash, &row.ledgerCloseTime); err != nil {
			return eventquery.WalkResult{}, fmt.Errorf("sqlitedb: queryEvents scan row: %w", err)
		}
		raw, err := contractEventBytes(row.eventData)
		if err != nil {
			return eventquery.WalkResult{}, fmt.Errorf("sqlitedb: event %s: %w", row.id, err)
		}
		if ok, err := matcher.Matches(raw); err != nil {
			return eventquery.WalkResult{}, fmt.Errorf("sqlitedb: event %s: %w", row.id, err)
		} else if !ok {
			continue
		}
		cur, err := protocol.ParseCursor(row.id)
		if err != nil {
			return eventquery.WalkResult{}, fmt.Errorf("sqlitedb: event %s: %w", row.id, err)
		}
		if len(walk.Events) >= limit {
			// The page is full and the next match is known: every ledger
			// strictly before its ledger, in walk order, is fully covered.
			l := cur.Ledger
			walk.NextUnserved = &l
			return s.finishLast(ctx, walk, lastID)
		}
		walk.Events = append(walk.Events, payloadOf(cur, row.transactionHash, row.ledgerCloseTime, raw))
		walk.Last = &eventquery.EventPosition{Ledger: cur.Ledger, Tx: cur.Tx, Op: cur.Op, Event: cur.Event}
		lastID = row.id
	}
	if err := rows.Err(); err != nil {
		return eventquery.WalkResult{}, fmt.Errorf("sqlitedb: queryEvents scan: %w", err)
	}
	// Rows ended: every candidate in [lo, hi] was checked, so the far
	// ledger is fully covered even when nothing was delivered.
	covered := hi
	if desc {
		covered = lo
	}
	walk.CoveredThrough = &covered
	walk.Finished = true
	return s.finishLast(ctx, walk, lastID)
}

// contractEventBytes unwraps the stored DiagnosticEvent to its ContractEvent
// XDR, the form the matcher and the payload carry.
func contractEventBytes(diagnostic []byte) ([]byte, error) {
	contractEvent, err := xdr.DiagnosticEventView(diagnostic).Event()
	if err != nil {
		return nil, err
	}
	return contractEvent.Raw()
}

// payloadOf copies one row into a Payload. raw aliases the row buffer, so
// the event bytes are copied.
func payloadOf(cur protocol.Cursor, txHash []byte, closedAt int64, raw []byte) eventquery.Payload {
	p := eventquery.Payload{
		LedgerSequence:     cur.Ledger,
		TxIdx:              cur.Tx,
		OpIdx:              cur.Op,
		EventIdx:           cur.Event,
		LedgerClosedAt:     closedAt,
		ContractEventBytes: append([]byte(nil), raw...),
	}
	copy(p.TxHash[:], txHash)
	return p
}

// finishLast fills in the last delivered event's LedgerOrdinal. The scan
// cannot count it, because the coarse predicates hide the ledger's other
// rows, so it is one extra count per page.
func (s *eventQuerySession) finishLast(
	ctx context.Context, walk eventquery.WalkResult, lastID string,
) (eventquery.WalkResult, error) {
	if walk.Last == nil {
		return walk, nil
	}
	ord, err := s.ordinalOf(ctx, walk.Last.Ledger, lastID)
	if err != nil {
		return eventquery.WalkResult{}, err
	}
	walk.Last.LedgerOrdinal = ord
	return walk, nil
}

// ordinalOf is the index of id among all stored events of its ledger, in
// id order: the position the rpcv2 pager calls LedgerOrdinal.
func (s *eventQuerySession) ordinalOf(ctx context.Context, ledger uint32, id string) (uint32, error) {
	var n uint32
	err := s.q.GetRaw(ctx, &n,
		"SELECT COUNT(*) FROM "+eventTableName+" WHERE id >= ? AND id < ?",
		protocol.Cursor{Ledger: ledger}.String(), id)
	if err != nil {
		return 0, fmt.Errorf("sqlitedb: event ordinal: %w", err)
	}
	return n, nil
}

// verifyPosition checks that the cursor's re-entry point names a stored
// event at the ordinal it claims, and returns that event's id. Any
// disagreement is ErrPositionMismatch, as on rpcv2: within-ledger order is
// deterministic, so a mismatch is a wrong cursor.
func (s *eventQuerySession) verifyPosition(ctx context.Context, pos *eventquery.EventPosition) (string, error) {
	id := protocol.Cursor{Ledger: pos.Ledger, Tx: pos.Tx, Op: pos.Op, Event: pos.Event}.String()
	var n int
	err := s.q.GetRaw(ctx, &n, "SELECT COUNT(*) FROM "+eventTableName+" WHERE id = ?", id)
	if err != nil {
		return "", fmt.Errorf("sqlitedb: resume position: %w", err)
	}
	if n == 0 {
		return "", fmt.Errorf("%w: ledger %d has no stored event (tx %d, op %d, event %d)",
			eventquery.ErrPositionMismatch, pos.Ledger, pos.Tx, pos.Op, pos.Event)
	}
	ord, err := s.ordinalOf(ctx, pos.Ledger, id)
	if err != nil {
		return "", err
	}
	if ord != pos.LedgerOrdinal {
		return "", fmt.Errorf("%w: ledger %d event (tx %d, op %d, event %d) is at index %d, cursor says %d",
			eventquery.ErrPositionMismatch, pos.Ledger, pos.Tx, pos.Op, pos.Event, ord, pos.LedgerOrdinal)
	}
	return id, nil
}

const eventTypeColumn = "event_type"

// coarsePredicates pushes to SQL whatever every filter constrains, so the
// predicate can only drop rows no filter would match. A filter that leaves
// a field open makes that field's predicate impossible, since filters are
// OR. Diagnostic events are never served, so the type predicate is always
// at least contract-or-system.
func coarsePredicates(rowQ sq.SelectBuilder, filters []eventquery.Filter) sq.SelectBuilder {
	contractIDs := make([][]byte, 0, len(filters))
	eventTypes := make([]int, 0, len(filters))
	var topics sq.Or
	allContract, allType, allTopic := len(filters) > 0, len(filters) > 0, len(filters) > 0
	// When every filter names a contract, the (contract_id, id) index bounds the
	// scan far better than (topic1, id): common topics such as "transfer" match
	// millions of rows per window. The unary plus hides topic1 from the planner
	// so it cannot pick that index.
	topicColPrefix := "+"
	for i := range filters {
		if len(filters[i].ContractID) == 0 {
			topicColPrefix = ""
			break
		}
	}
	for i := range filters {
		f := &filters[i]
		if len(f.ContractID) == 0 {
			allContract = false
		} else {
			contractIDs = append(contractIDs, f.ContractID)
		}
		if f.EventType == nil {
			allType = false
		} else {
			eventTypes = append(eventTypes, int(*f.EventType))
		}
		var ands sq.And
		for col, topic := range f.Topics {
			if len(topic) > 0 {
				ands = append(ands, sq.Eq{fmt.Sprintf("%stopic%d", topicColPrefix, col+1): topic})
			}
		}
		if len(ands) == 0 {
			allTopic = false
		} else {
			topics = append(topics, ands)
		}
	}
	if allContract {
		rowQ = rowQ.Where(sq.Eq{"contract_id": contractIDs})
	}
	if allType {
		rowQ = rowQ.Where(sq.Eq{eventTypeColumn: eventTypes})
	} else {
		rowQ = rowQ.Where(sq.Eq{eventTypeColumn: []int{
			int(xdr.ContractEventTypeContract), int(xdr.ContractEventTypeSystem),
		}})
	}
	if allTopic {
		rowQ = rowQ.Where(topics)
	}
	return rowQ
}
