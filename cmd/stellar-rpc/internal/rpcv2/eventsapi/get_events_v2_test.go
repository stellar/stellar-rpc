package eventsapi

// These run against a real read view, so a resume goes back through the
// codec. Only the response surface is asserted; the walk is the pager's test.

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/creachadair/jrpc2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/adapters"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rpcv2test"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/hotchunk"
)

const testChunk = chunk.ID(5)

func testLimits() Limits {
	return Limits{
		TermBudget:   protocol.DefaultTermBudgetV2,
		MaxLimit:     protocol.MaxLimitV2,
		DefaultLimit: 100,
	}
}

// seedView seeds chunk 5 and returns a read view over it:
//
//	F+0: a0, a1   (contract 0xAA)
//	F+1: b0       (contract 0xBB)
//	F+2: no events
func seedView(t *testing.T) (context.Context, uint32) {
	t.Helper()
	logger := rpcv2test.SilentLogger()
	cat, _ := rpcv2test.OpenTestCatalogWith(t, geometry.ChunksPerTxhashIndex, logger)
	r := query.NewRegistry(cat, geometry.NewRetention(0, testChunk))

	first := testChunk.FirstLedger()
	contractA := xdr.ContractId(testContractRaw(0xAA))
	contractB := xdr.ContractId(testContractRaw(0xBB))
	ledgers := [][]xdr.ContractEvent{
		{
			rpcv2test.SymbolContractEvent(contractA, "a0", "a0"),
			rpcv2test.SymbolContractEvent(contractA, "a1", "a1"),
		},
		{rpcv2test.SymbolContractEvent(contractB, "b0", "b0")},
		{},
	}
	lcms := make([][]byte, len(ledgers))
	for i, evs := range ledgers {
		seq := first + uint32(i)
		if len(evs) == 0 {
			lcms[i] = rpcv2test.ZeroTxLCMBytes(t, seq)
			continue
		}
		lcms[i] = rpcv2test.EventsLCMBytes(t, seq, evs...)
	}
	rpcv2test.SeedHotChunkLCMs(t, cat, testChunk,
		func(db *hotchunk.DB) { r.PublishHandle(testChunk, db) }, lcms...)
	r.SetLatestLedger(first+2, 0)

	view, err := r.NewReadView()
	require.NoError(t, err)
	t.Cleanup(view.Release)
	return adapters.WithView(context.Background(), view), first
}

// requireErrorData asserts the code and reason, then decodes the data
// payload into the given struct. Pass nil to skip that.
func requireErrorData(t *testing.T, err error, wantReason string, into any) {
	t.Helper()
	var jerr *jrpc2.Error
	require.ErrorAs(t, err, &jerr)
	assert.Equal(t, jrpc2.InvalidParams, jerr.Code)
	var reason struct {
		Reason string `json:"reason"`
	}
	require.NoError(t, json.Unmarshal(jerr.Data, &reason))
	assert.Equal(t, wantReason, reason.Reason)
	if into != nil {
		require.NoError(t, json.Unmarshal(jerr.Data, into))
	}
}

func TestGetEventsV2_AscendingPagesToComplete(t *testing.T) {
	ctx, first := seedView(t)

	page1, err := getEventsV2(ctx, testLimits(), &protocol.GetEventsV2Request{
		MinLedger: first,
		Limit:     new(uint(2)),
	})
	require.NoError(t, err)
	assert.Len(t, page1.Events, 2)
	assert.Equal(t, protocol.ScanStatusHasMore, page1.ScanStatus)
	assert.NotEmpty(t, page1.Cursor)
	assert.Equal(t, first, uint32(page1.Events[0].Ledger))
	assert.Equal(t, first, uint32(page1.Events[1].Ledger))

	page2, err := getEventsV2(ctx, testLimits(), &protocol.GetEventsV2Request{
		Cursor: page1.Cursor,
	})
	require.NoError(t, err)
	require.Len(t, page2.Events, 1)
	assert.Equal(t, first+1, uint32(page2.Events[0].Ledger))
	// The top edge follows the tip, so a finished walk still waits.
	assert.Equal(t, protocol.ScanStatusWaitingForLedgers, page2.ScanStatus)
	assert.NotEmpty(t, page2.Cursor)
}

// A page that covers no whole ledger reports the ledger just outside the
// range, not the cursor's 0. Descending, 0 would claim the whole range.
func TestGetEventsV2_ScannedLedgerOnAPageThatCoveredNothing(t *testing.T) {
	t.Run("ascending", func(t *testing.T) {
		ctx, first := seedView(t)
		one := uint(1)
		resp, err := getEventsV2(ctx, testLimits(), &protocol.GetEventsV2Request{
			MinLedger: first,
			Limit:     &one,
		})
		require.NoError(t, err)
		require.Len(t, resp.Events, 1, "the page fills inside the first ledger")
		assert.Equal(t, first-1, resp.ScannedLedger)
	})

	t.Run("descending", func(t *testing.T) {
		ctx, first := seedView(t)
		one := uint(1)
		resp, err := getEventsV2(ctx, testLimits(), &protocol.GetEventsV2Request{
			MinLedger: first,
			MaxLedger: first,
			Order:     protocol.OrderDescending,
			Limit:     &one,
		})
		require.NoError(t, err)
		require.Len(t, resp.Events, 1, "the page fills inside the top ledger")
		assert.Equal(t, first+1, resp.ScannedLedger)
	})
}

func TestGetEventsV2_ClosedRangeCompletesWithoutCursor(t *testing.T) {
	ctx, first := seedView(t)

	resp, err := getEventsV2(ctx, testLimits(), &protocol.GetEventsV2Request{
		MinLedger: first,
		MaxLedger: first + 2,
	})
	require.NoError(t, err)
	assert.Len(t, resp.Events, 3, "the request sets no limit, so the default of 100 covers the fixture")
	assert.Equal(t, protocol.ScanStatusComplete, resp.ScanStatus)
	assert.Empty(t, resp.Cursor, "a finished query carries no cursor")
	assert.Equal(t, first, resp.OldestLedger)
	assert.Equal(t, first+2, resp.LatestLedger)
}

// A minLedger below genesis is the client's "from the beginning", so the
// retention floor decides the outcome.
func TestGetEventsV2_MinLedgerBelowGenesisFollowsTheFloorRules(t *testing.T) {
	t.Run("ascending is out of range", func(t *testing.T) {
		ctx, first := seedView(t)
		_, err := getEventsV2(ctx, testLimits(), &protocol.GetEventsV2Request{
			MinLedger: 1,
			MaxLedger: first + 2,
		})
		require.Error(t, err)
		var data protocol.LedgerOutOfRangeErrorData
		requireErrorData(t, err, protocol.ErrorReasonLedgerOutOfRange, &data)
		assert.Equal(t, uint32(chunk.FirstLedgerSeq), data.MissingLedger)
		assert.Equal(t, first, data.OldestLedger)
	})

	t.Run("descending reaches the oldest served ledger", func(t *testing.T) {
		ctx, first := seedView(t)
		resp, err := getEventsV2(ctx, testLimits(), &protocol.GetEventsV2Request{
			MinLedger: 1,
			MaxLedger: first + 2,
			Order:     protocol.OrderDescending,
		})
		require.NoError(t, err)
		assert.Len(t, resp.Events, 3)
		assert.Equal(t, protocol.ScanStatusOldestReached, resp.ScanStatus)
	})
}

func TestGetEventsV2_TermBudgetRejectsBothRequestShapes(t *testing.T) {
	// Three distinct topic values, so three distinct index terms.
	filters := make([]protocol.EventFilterV2, 0, 3)
	for _, label := range []string{"a0", "a1", "b0"} {
		_, raw := symbolScVal(t, label)
		filters = append(filters, protocol.EventFilterV2{Topic0: requestTopic(t, raw)})
	}
	limits := testLimits()
	limits.TermBudget = 2

	t.Run("range request", func(t *testing.T) {
		ctx, first := seedView(t)
		_, err := getEventsV2(ctx, limits, &protocol.GetEventsV2Request{
			MinLedger: first,
			Filters:   filters,
		})
		require.Error(t, err)
		var data protocol.InvalidParamsErrorData
		requireErrorData(t, err, protocol.ErrorReasonInvalidParams, &data)
		assert.Equal(t, uint32(3), data.TermsUsed)
		assert.Equal(t, uint32(2), data.TermBudget)
	})

	t.Run("cursor request", func(t *testing.T) {
		ctx, first := seedView(t)
		scope, err := eventScope(&protocol.GetEventsV2Request{
			MinLedger: first,
			Filters:   filters,
		}, testOldest, first+2)
		require.NoError(t, err)
		token, err := (&query.EventCursor{Scope: scope}).Encode()
		require.NoError(t, err)

		_, err = getEventsV2(ctx, limits, &protocol.GetEventsV2Request{Cursor: token})
		require.Error(t, err)
		var data protocol.InvalidParamsErrorData
		requireErrorData(t, err, protocol.ErrorReasonInvalidParams, &data)
		assert.Equal(t, uint32(3), data.TermsUsed)
	})
}

// Only a forged cursor can carry an inverted scope. Checked on the mapping,
// because the codec refuses to encode one.
func TestResponseErrorInvertedRangeIsCursorMalformed(t *testing.T) {
	err := responseError(fmt.Errorf("%w: [3, 2]", query.ErrInvertedRange), 2, 9)

	var data protocol.CursorMalformedErrorData
	requireErrorData(t, err, protocol.ErrorReasonCursorMalformed, &data)
	assert.Equal(t, uint32(2), data.OldestLedger)
	assert.Equal(t, uint32(9), data.LatestLedger)
}

func TestGetEventsV2_MalformedCursorReportsTheServedRange(t *testing.T) {
	ctx, first := seedView(t)

	_, err := getEventsV2(ctx, testLimits(), &protocol.GetEventsV2Request{
		Cursor: "gec1_not-a-real-token",
	})
	require.Error(t, err)
	var data protocol.CursorMalformedErrorData
	requireErrorData(t, err, protocol.ErrorReasonCursorMalformed, &data)
	assert.Equal(t, first, data.OldestLedger)
	assert.Equal(t, first+2, data.LatestLedger)
}

func TestGetEventsV2_JSONInputFormatIsRejected(t *testing.T) {
	ctx, first := seedView(t)
	_, raw := symbolScVal(t, "a0")

	_, err := getEventsV2(ctx, testLimits(), &protocol.GetEventsV2Request{
		MinLedger:      first,
		XDRInputFormat: protocol.FormatJSON,
		Filters:        []protocol.EventFilterV2{{Topic0: requestTopic(t, raw)}},
	})
	require.Error(t, err)
	requireErrorData(t, err, protocol.ErrorReasonInvalidParams, nil)
}

func TestGetEventsV2_InvalidRequestIsRejectedBeforeAnyRead(t *testing.T) {
	ctx, first := seedView(t)

	_, err := getEventsV2(ctx, testLimits(), &protocol.GetEventsV2Request{
		MinLedger: first,
		Format:    "protobuf",
	})
	require.Error(t, err)
	requireErrorData(t, err, protocol.ErrorReasonInvalidParams, nil)
}

func TestGetEventsV2_WithoutAViewIsAnError(t *testing.T) {
	_, err := getEventsV2(context.Background(), testLimits(), &protocol.GetEventsV2Request{
		MinLedger: chunk.FirstLedgerSeq,
	})
	require.Error(t, err)
	var jerr *jrpc2.Error
	require.ErrorAs(t, err, &jerr)
	assert.Equal(t, jrpc2.InternalError, jerr.Code,
		"a missing view is a server fault, not a client error")
}

func TestGetEventsV2_LimitOverTheOperatorCapIsRejected(t *testing.T) {
	limits := testLimits()
	limits.MaxLimit = 2

	// The second case is over protocol.MaxLimitV2 too, and that check must
	// not answer first.
	for _, over := range []uint{3, protocol.MaxLimitV2 + 1} {
		t.Run(fmt.Sprintf("limit %d over the cap", over), func(t *testing.T) {
			ctx, first := seedView(t)
			_, err := getEventsV2(ctx, limits, &protocol.GetEventsV2Request{
				MinLedger: first,
				Limit:     &over,
			})
			require.Error(t, err)
			requireErrorData(t, err, protocol.ErrorReasonInvalidParams, nil)
			assert.Contains(t, err.Error(), "between 1 and 2")
		})
	}

	// Zero is rejected either way, but the message must still name this
	// node's cap rather than the SDK's ceiling.
	t.Run("limit 0", func(t *testing.T) {
		ctx, first := seedView(t)
		zero := uint(0)
		_, err := getEventsV2(ctx, limits, &protocol.GetEventsV2Request{
			MinLedger: first,
			Limit:     &zero,
		})
		require.Error(t, err)
		requireErrorData(t, err, protocol.ErrorReasonInvalidParams, nil)
		assert.Contains(t, err.Error(), "between 1 and 2")
	})

	t.Run("at the cap", func(t *testing.T) {
		ctx, first := seedView(t)
		resp, err := getEventsV2(ctx, limits, &protocol.GetEventsV2Request{
			MinLedger: first,
			Limit:     new(uint(2)),
		})
		require.NoError(t, err)
		assert.Len(t, resp.Events, 2)
	})
}
