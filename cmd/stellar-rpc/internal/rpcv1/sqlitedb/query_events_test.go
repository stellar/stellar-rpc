package sqlitedb

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/eventquery"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
)

const (
	qeFirstLedger = uint32(2)
	qeLastLedger  = uint32(6)
)

var (
	qeContractA = xdr.ContractId([32]byte{1})
	qeContractB = xdr.ContractId([32]byte{2})
)

// seedQueryEventsDB stores ledgers 2..6 with three single-event
// transactions each, alternating contracts A, B, A.
func seedQueryEventsDB(t *testing.T) *DB {
	t.Helper()
	db := NewTestDB(t)
	ctx := t.Context()
	sym := xdr.ScSymbol("COUNTER")
	topic := xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym}
	write, err := NewReadWriter(log.DefaultLogger, db, host.MakeNoOpDaemon(), 10, passphrase).NewTx(ctx)
	require.NoError(t, err)
	var last xdr.LedgerCloseMeta
	for seq := qeFirstLedger; seq <= qeLastLedger; seq++ {
		last = ledgerCloseMetaWithEvents(seq, time.Now().Unix(),
			transactionMetaWithEvents(contractEvent(qeContractA, xdr.ScVec{topic}, topic)),
			transactionMetaWithEvents(contractEvent(qeContractB, xdr.ScVec{topic}, topic)),
			transactionMetaWithEvents(contractEvent(qeContractA, xdr.ScVec{topic}, topic)),
		)
		require.NoError(t, write.LedgerWriter().InsertLedger(last))
		require.NoError(t, write.EventWriter().InsertEvents(last))
	}
	require.NoError(t, write.Commit(last, nil))
	return db
}

// storedEvents is the number of event rows in [lo, hi]: the fixture adds
// transaction-level events the test does not spell out.
func storedEvents(t *testing.T, db *DB, lo, hi uint32) int {
	t.Helper()
	var n int
	require.NoError(t, db.GetRaw(t.Context(), &n, "SELECT COUNT(*) FROM events WHERE id >= ? AND id < ?",
		protocol.Cursor{Ledger: lo}.String(), protocol.Cursor{Ledger: hi + 1}.String()))
	require.NotZero(t, n)
	return n
}

// idLess orders events by their v1 id: (ledger, tx, op, event).
func idLess(a, b *eventquery.Payload) bool {
	x := [4]uint32{a.LedgerSequence, a.TxIdx, a.OpIdx, a.EventIdx}
	y := [4]uint32{b.LedgerSequence, b.TxIdx, b.OpIdx, b.EventIdx}
	for i := range x {
		if x[i] != y[i] {
			return x[i] < y[i]
		}
	}
	return false
}

func openSession(t *testing.T, db *DB) eventquery.Session {
	t.Helper()
	s, err := NewEventQuerier(log.DefaultLogger, db).Begin(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s
}

// drain pages through the cursor chain until a terminal status and
// returns every event in delivery order plus the final status.
func drain(
	ctx context.Context, t *testing.T, s eventquery.Session, scope eventquery.EventScope, limit int,
) ([]eventquery.Payload, eventquery.ScanStatus) {
	t.Helper()
	cursor := eventquery.EventCursor{Scope: scope}
	var out []eventquery.Payload
	for range 100 {
		page, err := s.QueryEvents(ctx, cursor, limit)
		require.NoError(t, err)
		out = append(out, page.Events...)
		if page.Status != eventquery.ScanHasMore {
			return out, page.Status
		}
		// Round-trip the cursor through the codec, as the wire does.
		token, err := page.Next.Encode()
		require.NoError(t, err)
		next, err := eventquery.DecodeEventCursor(token)
		require.NoError(t, err)
		cursor = *next
	}
	t.Fatal("no terminal status after 100 pages")
	return nil, 0
}

func TestQueryEvents_SQLite_AscendingOpenRange(t *testing.T) {
	db := seedQueryEventsDB(t)
	s := openSession(t, db)
	require.Equal(t, qeFirstLedger, s.OldestLedger())
	require.Equal(t, qeLastLedger, s.LatestLedger())

	for _, limit := range []int{1, 2, 4, 100} {
		events, status := drain(t.Context(), t, s, eventquery.EventScope{MinLedger: qeFirstLedger}, limit)
		require.Equal(t, eventquery.ScanWaitingForLedgers, status, "limit %d", limit)
		require.Len(t, events, storedEvents(t, db, qeFirstLedger, qeLastLedger), "limit %d", limit)
		for i := 1; i < len(events); i++ {
			require.True(t, idLess(&events[i-1], &events[i]), "order at %d", i)
		}
	}
}

func TestQueryEvents_SQLite_DescendingClosedRange(t *testing.T) {
	db := seedQueryEventsDB(t)
	s := openSession(t, db)
	maxLedger := qeLastLedger
	scope := eventquery.EventScope{MinLedger: qeFirstLedger, MaxLedger: &maxLedger, Dir: eventquery.Descending}
	for _, limit := range []int{1, 2, 4, 100} {
		events, status := drain(t.Context(), t, s, scope, limit)
		require.Equal(t, eventquery.ScanComplete, status, "limit %d", limit)
		require.Len(t, events, storedEvents(t, db, qeFirstLedger, qeLastLedger), "limit %d", limit)
		for i := 1; i < len(events); i++ {
			require.True(t, idLess(&events[i], &events[i-1]), "order at %d", i)
		}
	}
}

func TestQueryEvents_SQLite_ContractFilter(t *testing.T) {
	db := seedQueryEventsDB(t)
	s := openSession(t, db)
	maxLedger := uint32(4)
	scope := eventquery.EventScope{
		MinLedger: 3, MaxLedger: &maxLedger,
		Filters: []eventquery.Filter{{ContractID: qeContractB[:]}},
	}
	events, status := drain(t.Context(), t, s, scope, 1)
	require.Equal(t, eventquery.ScanComplete, status)
	require.Len(t, events, 2)
	for _, e := range events {
		require.Equal(t, uint32(2), e.TxIdx) // contract B is the second tx of every ledger
		require.Contains(t, []uint32{3, 4}, e.LedgerSequence)
	}
}

func TestQueryEvents_SQLite_PositionMismatch(t *testing.T) {
	db := seedQueryEventsDB(t)
	s := openSession(t, db)
	ctx := t.Context()
	page, err := s.QueryEvents(ctx, eventquery.EventCursor{Scope: eventquery.EventScope{MinLedger: qeFirstLedger}}, 2)
	require.NoError(t, err)
	require.NotNil(t, page.Next.Position)
	require.Equal(t, uint32(1), page.Next.Position.LedgerOrdinal)

	// A resume through the honest cursor works.
	_, err = s.QueryEvents(ctx, page.Next, 2)
	require.NoError(t, err)

	bad := page.Next
	pos := *bad.Position
	pos.LedgerOrdinal = 2
	bad.Position = &pos
	_, err = s.QueryEvents(ctx, bad, 2)
	require.ErrorIs(t, err, eventquery.ErrPositionMismatch)
}

func TestQueryEvents_SQLite_BelowFloorAndBeyondTip(t *testing.T) {
	db := seedQueryEventsDB(t)
	s := openSession(t, db)
	ctx := t.Context()

	page, err := s.QueryEvents(ctx, eventquery.EventCursor{Scope: eventquery.EventScope{MinLedger: qeLastLedger + 5}}, 10)
	require.NoError(t, err)
	require.Equal(t, eventquery.ScanWaitingForLedgers, page.Status)

	maxLedger := qeLastLedger + 5
	page, err = s.QueryEvents(ctx, eventquery.EventCursor{Scope: eventquery.EventScope{
		MinLedger: qeFirstLedger, MaxLedger: &maxLedger, Dir: eventquery.Descending,
	}}, 10)
	require.NoError(t, err)
	require.Equal(t, eventquery.ScanWaitingForLedgers, page.Status)
}
