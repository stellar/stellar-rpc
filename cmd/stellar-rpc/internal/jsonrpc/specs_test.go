package jsonrpc

import (
	"context"
	"testing"
	"time"

	"github.com/creachadair/jrpc2"
	"github.com/stretchr/testify/assert"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
)

func testSpecDeps() SpecDeps {
	return SpecDeps{
		Daemon:           host.MakeNoOpDaemon(),
		GetEventsHandler: func(context.Context, *jrpc2.Request) (any, error) { return "ok", nil },
	}
}

func fullSpecLimits() SpecLimits {
	limits := SpecLimits{}
	for _, name := range []string{
		protocol.GetHealthMethodName,
		protocol.GetEventsMethodName,
		protocol.GetNetworkMethodName,
		protocol.GetVersionInfoMethodName,
		protocol.GetLatestLedgerMethodName,
		protocol.GetLedgersMethodName,
		protocol.GetLedgerEntriesMethodName,
		protocol.GetTransactionMethodName,
		protocol.GetTransactionsMethodName,
		protocol.SendTransactionMethodName,
		protocol.SimulateTransactionMethodName,
		protocol.GetFeeStatsMethodName,
	} {
		limits[name] = MethodLimits{QueueLimit: 1, RequestDurationLimit: time.Second}
	}
	return limits
}

func TestApply_FillsEveryServedMethodsLimits(t *testing.T) {
	limits := fullSpecLimits()
	specs := limits.Apply(BuildHandlerSpecs(testSpecDeps()))
	assert.Len(t, specs, len(limits))
	for _, s := range specs {
		assert.Equal(t, limits[s.MethodName].QueueLimit, s.QueueLimit)
		assert.Equal(t, limits[s.MethodName].RequestDurationLimit, s.RequestDurationLimit)
		assert.NotNil(t, s.Handler)
	}
}

func TestApply_PanicsOnAMethodWithoutLimits(t *testing.T) {
	limits := fullSpecLimits()
	delete(limits, protocol.GetFeeStatsMethodName)
	assert.PanicsWithValue(t, "jsonrpc: no limits configured for method getFeeStats", func() {
		limits.Apply(BuildHandlerSpecs(testSpecDeps()))
	})
}

func TestApply_PanicsOnLimitsForAnUnservedMethod(t *testing.T) {
	limits := fullSpecLimits()
	limits["getFoo"] = MethodLimits{QueueLimit: 1, RequestDurationLimit: time.Second}
	assert.PanicsWithValue(t, "jsonrpc: limits configured for unserved method getFoo", func() {
		limits.Apply(BuildHandlerSpecs(testSpecDeps()))
	})
}

func TestApply_CoversDaemonAppendedMethods(t *testing.T) {
	limits := fullSpecLimits()
	limits["getFoo"] = MethodLimits{QueueLimit: 7, RequestDurationLimit: time.Minute}
	stub := func(context.Context, *jrpc2.Request) (any, error) { return "ok", nil }

	specs := BuildHandlerSpecs(testSpecDeps())
	specs = append(specs, HandlerSpec{MethodName: "getFoo", Handler: stub})
	specs = limits.Apply(specs)

	assert.Len(t, specs, len(limits))
	last := specs[len(specs)-1]
	assert.Equal(t, "getFoo", last.MethodName)
	assert.Equal(t, uint(7), last.QueueLimit)
	assert.Equal(t, time.Minute, last.RequestDurationLimit)
}

func TestApply_PanicsOnAnAppendedMethodWithoutLimits(t *testing.T) {
	stub := func(context.Context, *jrpc2.Request) (any, error) { return "ok", nil }
	specs := BuildHandlerSpecs(testSpecDeps())
	specs = append(specs, HandlerSpec{MethodName: "getFoo", Handler: stub})
	assert.PanicsWithValue(t, "jsonrpc: no limits configured for method getFoo", func() {
		fullSpecLimits().Apply(specs)
	})
}

func TestBuildHandlerSpecs_PanicsWithoutGetEventsHandler(t *testing.T) {
	assert.PanicsWithValue(t, "jsonrpc: SpecDeps.GetEventsHandler is required — events handlers are per-daemon",
		func() {
			BuildHandlerSpecs(SpecDeps{Daemon: host.MakeNoOpDaemon()})
		})
}

func TestSpecLimits_LongestDuration(t *testing.T) {
	limits := fullSpecLimits()
	limits[protocol.SimulateTransactionMethodName] = MethodLimits{QueueLimit: 1, RequestDurationLimit: time.Minute}
	assert.Equal(t, time.Minute, limits.LongestDuration())
	assert.Equal(t, time.Duration(0), SpecLimits{}.LongestDuration())
}
