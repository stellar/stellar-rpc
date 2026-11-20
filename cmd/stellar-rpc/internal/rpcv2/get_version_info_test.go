package rpcv2

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rpcv2test"
)

// getVersionInfo reports the served methods and the configured request
// limits next to the v1 fields, and queryEvents enforces the same limits.
func TestJSONRPCHandler_GetVersionInfoAdvertisesMethodsAndLimits(t *testing.T) {
	cfg := defaultsConfig(t)
	termBudget, maxFilters, maxLimit := uint(7), uint(2), uint(500)
	cfg.Service.Methods.QueryEvents.TermBudget = &termBudget
	cfg.Service.Methods.QueryEvents.MaxFilters = &maxFilters
	cfg.Service.Methods.QueryEvents.MaxItemsPerResponse = &maxLimit
	batch, events, ledgers, txs := uint(4), uint(30), uint(3), uint(40)
	cfg.Service.MaxRequestBatchSize = &batch
	cfg.Service.Methods.GetEvents.MaxItemsPerResponse = &events
	cfg.Service.Methods.GetLedgers.MaxItemsPerResponse = &ledgers
	cfg.Service.Methods.GetTransactions.MaxItemsPerResponse = &txs
	handler := newJSONRPCHandler(cfg, testHandlerParams(t, seedServingRegistry(t)))
	srv := httptest.NewServer(handler)
	t.Cleanup(func() {
		srv.Close()
		handler.Close()
	})

	out := rpcv2test.PostRPC(t, srv.URL, protocol.GetVersionInfoMethodName, `{}`)
	require.Nil(t, out.Error)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(out.Result, &fields))
	for _, base := range []string{"version", "commitHash", "buildTimestamp", "captiveCoreVersion", "protocolVersion"} {
		assert.Contains(t, fields, base)
	}

	var result protocol.GetVersionInfoResponse
	require.NoError(t, json.Unmarshal(out.Result, &result))
	assert.Equal(t, []string{
		"getEvents", "getFeeStats", "getHealth", "getLatestLedger", "getLedgerEntries", "getLedgers",
		"getNetwork", "getTransaction", "getTransactions", "getVersionInfo", "queryEvents",
		"sendTransaction", "simulateTransaction",
	}, result.Methods)
	assert.Equal(t, &protocol.VersionInfoLimits{
		MaxBatchSize:    4,
		GetEvents:       &protocol.PageLimits{MaxLimit: 30},
		GetLedgers:      &protocol.PageLimits{MaxLimit: 3},
		GetTransactions: &protocol.PageLimits{MaxLimit: 40},
		QueryEvents:     &protocol.QueryEventsLimits{TermBudget: 7, MaxFilters: 2, MaxLimit: 500},
	}, result.Limits)

	filter := `{"type":"contract"}`
	over := rpcv2test.PostRPC(t, srv.URL, protocol.QueryEventsMethodName,
		`{"minLedger":2,"filters":[`+strings.Repeat(filter+",", 2)+filter+`]}`)
	require.NotNil(t, over.Error)
	assert.Contains(t, over.Error.Message, "1 to 2 filters")
}

// The default config advertises the documented defaults.
func TestJSONRPCHandler_GetVersionInfoDefaultLimits(t *testing.T) {
	url := newTestRPCServer(t, seedServingRegistry(t))

	out := rpcv2test.PostRPC(t, url, protocol.GetVersionInfoMethodName, `{}`)
	require.Nil(t, out.Error)
	var result struct {
		Limits    json.RawMessage `json:"limits"`
		Retention json.RawMessage `json:"retention"`
	}
	require.NoError(t, json.Unmarshal(out.Result, &result))
	assert.JSONEq(t, `{
		"maxBatchSize": 10,
		"getEvents": {"maxLimit": 10000},
		"getLedgers": {"maxLimit": 20},
		"getTransactions": {"maxLimit": 200},
		"queryEvents": {"termBudget": 15, "maxFilters": 256, "maxLimit": 1000}
	}`, string(result.Limits))
	assert.JSONEq(t, `{"keeps": "all", "fromLedger": 2}`, string(result.Retention))
}

// getVersionInfo reports a full-history node from its pinned earliest ledger,
// and a windowed node by its window in ledgers.
func TestVersionInfoRetention(t *testing.T) {
	assert.Equal(t, &protocol.Retention{Keeps: protocol.RetentionAll, FromLedger: 60_000_002},
		versionInfoRetention(geometry.NewRetention(0, chunk.IDFromLedger(60_000_002))))
	assert.Equal(t, &protocol.Retention{Keeps: protocol.RetentionLatest, Ledgers: 130_000},
		versionInfoRetention(geometry.NewRetention(13, chunk.IDFromLedger(60_000_002))))
}
