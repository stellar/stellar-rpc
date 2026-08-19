package jsonrpc

import (
	"context"
	"testing"
	"time"

	"github.com/creachadair/jrpc2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
)

type stubDaemon struct{}

func (stubDaemon) MetricsRegistry() *prometheus.Registry { return prometheus.NewRegistry() }
func (stubDaemon) MetricsNamespace() string              { return "test" }
func (stubDaemon) CoreClient() host.CoreClient           { return nil }
func (stubDaemon) FastCoreClient() host.FastCoreClient   { return nil }
func (stubDaemon) CoreVersion() string                   { return "" }

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

func TestBuildHandlerSpecs_ServesEveryLimitedMethod(t *testing.T) {
	limits := fullSpecLimits()
	specs := BuildHandlerSpecs(SpecDeps{Daemon: stubDaemon{}}, limits)
	assert.Len(t, specs, len(limits))
	for _, s := range specs {
		assert.Equal(t, limits[s.MethodName].QueueLimit, s.QueueLimit)
		assert.Equal(t, limits[s.MethodName].RequestDurationLimit, s.RequestDurationLimit)
		assert.NotNil(t, s.Handler)
	}
}

func TestBuildHandlerSpecs_PanicsOnAMethodWithoutLimits(t *testing.T) {
	limits := fullSpecLimits()
	delete(limits, protocol.GetFeeStatsMethodName)
	assert.PanicsWithValue(t, "jsonrpc: no limits configured for method getFeeStats", func() {
		BuildHandlerSpecs(SpecDeps{Daemon: stubDaemon{}}, limits)
	})
}

func TestBuildHandlerSpecs_PanicsOnLimitsForAnUnservedMethod(t *testing.T) {
	limits := fullSpecLimits()
	limits["getFoo"] = MethodLimits{QueueLimit: 1, RequestDurationLimit: time.Second}
	assert.PanicsWithValue(t, "jsonrpc: limits configured for unserved method getFoo", func() {
		BuildHandlerSpecs(SpecDeps{Daemon: stubDaemon{}}, limits)
	})
}

func TestBuildHandlerSpecs_AppendsExtraMethodsWithTheirLimits(t *testing.T) {
	limits := fullSpecLimits()
	limits["getFoo"] = MethodLimits{QueueLimit: 7, RequestDurationLimit: time.Minute}
	stub := func(context.Context, *jrpc2.Request) (any, error) { return "ok", nil }

	specs := BuildHandlerSpecs(SpecDeps{Daemon: stubDaemon{}}, limits,
		ExtraMethod{MethodName: "getFoo", Handler: stub})

	assert.Len(t, specs, len(limits))
	last := specs[len(specs)-1]
	assert.Equal(t, "getFoo", last.MethodName)
	assert.Equal(t, uint(7), last.QueueLimit)
	assert.Equal(t, time.Minute, last.RequestDurationLimit)
}

func TestBuildHandlerSpecs_PanicsOnAnExtraMethodWithoutLimits(t *testing.T) {
	stub := func(context.Context, *jrpc2.Request) (any, error) { return "ok", nil }
	assert.PanicsWithValue(t, "jsonrpc: no limits configured for method getFoo", func() {
		BuildHandlerSpecs(SpecDeps{Daemon: stubDaemon{}}, fullSpecLimits(),
			ExtraMethod{MethodName: "getFoo", Handler: stub})
	})
}

func TestSpecLimits_LongestDuration(t *testing.T) {
	limits := fullSpecLimits()
	limits[protocol.SimulateTransactionMethodName] = MethodLimits{QueueLimit: 1, RequestDurationLimit: time.Minute}
	assert.Equal(t, time.Minute, limits.LongestDuration())
	assert.Equal(t, time.Duration(0), SpecLimits{}.LongestDuration())
}
