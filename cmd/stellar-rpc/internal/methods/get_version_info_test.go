package methods

import (
	"encoding/json"
	"testing"

	"github.com/creachadair/jrpc2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
)

func callGetVersionInfo(t *testing.T, h jrpc2.Handler) map[string]json.RawMessage {
	t.Helper()
	resp, err := h(t.Context(), &jrpc2.Request{})
	require.NoError(t, err)
	body, err := json.Marshal(resp)
	require.NoError(t, err)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(body, &fields))
	return fields
}

// getVersionInfo leaves the report fields out when they are nil, as rpcv1
// passes them, and reports them when given.
func TestGetVersionInfoReportsDeploymentOnlyWhenGiven(t *testing.T) {
	logger := log.New()
	daemon := host.MakeNoOpDaemon()

	fields := callGetVersionInfo(t, NewGetVersionInfoHandler(logger, &ConstantLedgerReader{}, daemon, VersionInfoReport{}))
	assert.NotContains(t, fields, "methods")
	assert.NotContains(t, fields, "limits")
	assert.NotContains(t, fields, "retention")
	assert.JSONEq(t, `20`, string(fields["protocolVersion"]))

	report := VersionInfoReport{
		Methods: []string{protocol.GetVersionInfoMethodName},
		Limits: &protocol.VersionInfoLimits{
			QueryEvents: &protocol.QueryEventsLimits{TermBudget: 15, MaxFilters: 256, MaxLimit: 1000},
		},
		Retention: &protocol.Retention{Keeps: protocol.RetentionAll, FromLedger: 2},
	}
	fields = callGetVersionInfo(t, NewGetVersionInfoHandler(logger, &ConstantLedgerReader{}, daemon, report))
	assert.JSONEq(t, `["getVersionInfo"]`, string(fields["methods"]))
	assert.JSONEq(t, `{"queryEvents":{"termBudget":15,"maxFilters":256,"maxLimit":1000}}`,
		string(fields["limits"]))
	assert.JSONEq(t, `{"keeps":"all","fromLedger":2}`, string(fields["retention"]))
}
