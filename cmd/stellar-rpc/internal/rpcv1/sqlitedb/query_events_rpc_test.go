package sqlitedb

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/handler"
	"github.com/creachadair/jrpc2/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/eventsapi"
)

// The queryEvents method end to end on rpcv1: the shared handler over the
// SQLite engine, called through an in-process JSON-RPC server.
//
//nolint:funlen // one server, four scenarios
func TestQueryEvents_SQLite_JSONRPC(t *testing.T) {
	db := seedQueryEventsDB(t)
	h := eventsapi.NewQueryEventsHandler(NewEventQuerier(log.DefaultLogger, db), eventsapi.QueryEventsLimits{
		Limits:     eventsapi.Limits{MaxLimit: 1000, DefaultLimit: 100},
		TermBudget: uint32(eventsapi.DefaultTermBudget),
	}, log.DefaultLogger)
	local := server.NewLocal(handler.Map{protocol.QueryEventsMethodName: h}, nil)
	t.Cleanup(func() { _ = local.Close() })
	c := local.Client
	ctx := context.Background()

	call := func(req protocol.QueryEventsRequest) protocol.QueryEventsResponse {
		t.Helper()
		var resp protocol.QueryEventsResponse
		require.NoError(t, c.CallResult(ctx, protocol.QueryEventsMethodName, req, &resp))
		return resp
	}
	total := storedEvents(t, db, qeFirstLedger, qeLastLedger)

	t.Run("ascending pages to the tip", func(t *testing.T) {
		limit := uint(7)
		var ids []string
		req := protocol.QueryEventsRequest{MinLedger: qeFirstLedger, Limit: &limit}
		for range 100 {
			resp := call(req)
			assert.Equal(t, qeFirstLedger, resp.OldestLedger)
			assert.Equal(t, qeLastLedger, resp.LatestLedger)
			for _, e := range resp.Events {
				ids = append(ids, e.ID)
			}
			if resp.ScanStatus != protocol.ScanStatusHasMore {
				require.Equal(t, protocol.ScanStatusWaitingForLedgers, resp.ScanStatus)
				require.NotEmpty(t, resp.Cursor, "a non-terminal status carries a cursor")
				require.Equal(t, qeLastLedger, resp.ScannedLedger)
				break
			}
			require.NotEmpty(t, resp.Cursor)
			req = protocol.QueryEventsRequest{Cursor: resp.Cursor, Limit: &limit}
		}
		require.Len(t, ids, total)
		for i := 1; i < len(ids); i++ {
			require.Less(t, ids[i-1], ids[i], "ids ascend")
		}
	})

	t.Run("descending closed range completes", func(t *testing.T) {
		limit := uint(5)
		var ids []string
		req := protocol.QueryEventsRequest{
			MinLedger: qeFirstLedger, MaxLedger: qeLastLedger, Order: protocol.OrderDescending, Limit: &limit,
		}
		for range 100 {
			resp := call(req)
			for _, e := range resp.Events {
				ids = append(ids, e.ID)
			}
			if resp.ScanStatus != protocol.ScanStatusHasMore {
				require.Equal(t, protocol.ScanStatusComplete, resp.ScanStatus)
				require.Empty(t, resp.Cursor, "a complete scan carries no cursor")
				break
			}
			req = protocol.QueryEventsRequest{Cursor: resp.Cursor, Limit: &limit}
		}
		require.Len(t, ids, total)
		for i := 1; i < len(ids); i++ {
			require.Greater(t, ids[i-1], ids[i], "ids descend")
		}
	})

	t.Run("contract filter", func(t *testing.T) {
		contractB, err := strkey.Encode(strkey.VersionByteContract, qeContractB[:])
		require.NoError(t, err)
		resp := call(protocol.QueryEventsRequest{
			MinLedger: 3, MaxLedger: 4,
			Filters: []protocol.QueryEventsFilter{{ContractID: contractB}},
		})
		require.Equal(t, protocol.ScanStatusComplete, resp.ScanStatus)
		require.Len(t, resp.Events, 2)
		for _, e := range resp.Events {
			assert.Equal(t, contractB, e.ContractID)
		}
	})

	t.Run("below the retention floor is out of range", func(t *testing.T) {
		var resp protocol.QueryEventsResponse
		err := c.CallResult(ctx, protocol.QueryEventsMethodName,
			protocol.QueryEventsRequest{MinLedger: 2, MaxLedger: 3}, &resp)
		// The floor is ledger 2 here, so this one is served.
		require.NoError(t, err)

		// Trim ledger 2 away and the same request is out of range.
		_, err = db.ExecRaw(ctx, "DELETE FROM events WHERE id < ?", protocol.Cursor{Ledger: 3}.String())
		require.NoError(t, err)
		_, err = db.ExecRaw(ctx, "DELETE FROM ledger_close_meta WHERE sequence < 3")
		require.NoError(t, err)
		err = c.CallResult(ctx, protocol.QueryEventsMethodName,
			protocol.QueryEventsRequest{MinLedger: 2, MaxLedger: 3}, &resp)
		var jerr *jrpc2.Error
		require.ErrorAs(t, err, &jerr)
		var data protocol.LedgerOutOfRangeErrorData
		require.NoError(t, json.Unmarshal(jerr.Data, &data))
		assert.Equal(t, protocol.ErrorReasonLedgerOutOfRange, data.Reason)
		assert.Equal(t, uint32(2), data.MissingLedger)
		assert.Equal(t, uint32(3), data.OldestLedger)
	})
}
