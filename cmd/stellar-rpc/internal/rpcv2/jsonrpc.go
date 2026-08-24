package rpcv2

import (
	"context"
	"errors"
	"time"

	"github.com/creachadair/jrpc2"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/jsonrpc"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/methods"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/network"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/adapters"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/feewindow"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/observability"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

// handlerParams carries everything newJSONRPCHandler composes into the method
// table besides the config: the per-attempt readers over the query router, the
// process-wide core-backed pieces, and the sinks.
type handlerParams struct {
	daemon            host.Daemon
	logger            *supportlog.Entry
	handlerMetrics    *jsonrpc.HandlerMetrics
	metrics           observability.Metrics
	ledgerReader      store.LedgerReader
	transactionReader store.TransactionReader
	feeWindows        *feewindow.FeeWindows
	preflightGetter   methods.PreflightGetter
	networkPassphrase string
	retentionWindow   uint32
}

// getEventsV2MethodName is the one method v2 serves that v1 does not, so no
// shared protocol constant exists for it yet.
const getEventsV2MethodName = "getEventsV2"

// newJSONRPCHandler maps the v2 config onto the shared method-spec builder —
// the v2 counterpart of rpcv1.NewJSONRPCHandler. The handlers are the shared
// internal/methods constructors, unmodified; only their inputs are v2's (the
// router-backed adapters, the daemon-owned fee windows, captive-core state).
// getEvents is the one exception: a stub that reports not-implemented until
// #774 ships it.
func newJSONRPCHandler(cfg config.Config, p handlerParams) jsonrpc.Handler {
	m := cfg.Service.Methods
	specs := jsonrpc.BuildHandlerSpecs(
		jsonrpc.SpecDeps{
			Daemon:            p.daemon,
			Logger:            p.logger,
			PreflightGetter:   p.preflightGetter,
			LedgerReader:      p.ledgerReader,
			TransactionReader: p.transactionReader,
			FeeStats:          p.feeWindows,

			GetEventsHandler: notImplemented("getEvents is not implemented by this service yet (issue #774 adds it);" +
				" use the existing RPC service for events meanwhile"),

			// No DataStoreLedgerReader: getLedgers can fall back to a bulk
			// datastore for ledgers below local retention, but the full-history
			// daemon IS the deep-history store, so it serves everything locally.
			NetworkPassphrase: p.networkPassphrase,
			FriendbotURL:      m.GetNetwork.FriendbotURL,

			RetentionWindow:         p.retentionWindow,
			MaxHealthyLedgerLatency: deref(m.GetHealth.MaxHealthyLedgerLatency),

			MaxLedgersLimit:          deref(m.GetLedgers.MaxItemsPerResponse),
			DefaultLedgersLimit:      deref(m.GetLedgers.DefaultItemsPerResponse),
			MaxTransactionsLimit:     deref(m.GetTransactions.MaxItemsPerResponse),
			DefaultTransactionsLimit: deref(m.GetTransactions.DefaultItemsPerResponse),
		})
	specs = append(specs, jsonrpc.HandlerSpec{
		MethodName: getEventsV2MethodName,
		Handler: notImplemented("getEventsV2 is not implemented by this service yet (issue #774 adds it);" +
			" use the existing RPC service for events meanwhile"),
	})
	specs = specLimits(m).Apply(specs)
	metrics := observability.MetricsOrNop(p.metrics)
	for i := range specs {
		specs[i].Handler = wrapAdapterRequest(specs[i].Handler, metrics)
	}

	return jsonrpc.NewHandler(jsonrpc.Params{
		Daemon:                p.daemon,
		Logger:                p.logger,
		Metrics:               p.handlerMetrics,
		Specs:                 specs,
		GlobalQueueLimit:      deref(cfg.Service.MaxConcurrentRequests),
		GlobalDurationWarning: deref(cfg.Service.RequestExecutionWarningThreshold),
		GlobalDurationLimit:   deref(cfg.Service.MaxRequestExecutionDuration),
	})
}

// specLimits maps [service.methods] onto the shared limits table. Both the
// method table and the deletion grace (deriveLifecycleGrace) read it, so the
// two cannot disagree on a method's budget.
func specLimits(m config.MethodsConfig) jsonrpc.SpecLimits {
	lim := func(queue *uint, dur *time.Duration) jsonrpc.MethodLimits {
		return jsonrpc.MethodLimits{QueueLimit: deref(queue), RequestDurationLimit: deref(dur)}
	}
	return jsonrpc.SpecLimits{
		protocol.GetHealthMethodName:        lim(m.GetHealth.QueueLimit, m.GetHealth.MaxExecutionDuration),
		protocol.GetEventsMethodName:        lim(m.GetEvents.QueueLimit, m.GetEvents.MaxExecutionDuration),
		getEventsV2MethodName:               lim(m.GetEventsV2.QueueLimit, m.GetEventsV2.MaxExecutionDuration),
		protocol.GetNetworkMethodName:       lim(m.GetNetwork.QueueLimit, m.GetNetwork.MaxExecutionDuration),
		protocol.GetVersionInfoMethodName:   lim(m.GetVersionInfo.QueueLimit, m.GetVersionInfo.MaxExecutionDuration),
		protocol.GetLatestLedgerMethodName:  lim(m.GetLatestLedger.QueueLimit, m.GetLatestLedger.MaxExecutionDuration),
		protocol.GetLedgersMethodName:       lim(m.GetLedgers.QueueLimit, m.GetLedgers.MaxExecutionDuration),
		protocol.GetLedgerEntriesMethodName: lim(m.GetLedgerEntries.QueueLimit, m.GetLedgerEntries.MaxExecutionDuration),
		protocol.GetTransactionMethodName:   lim(m.GetTransaction.QueueLimit, m.GetTransaction.MaxExecutionDuration),
		protocol.GetTransactionsMethodName:  lim(m.GetTransactions.QueueLimit, m.GetTransactions.MaxExecutionDuration),
		protocol.SendTransactionMethodName:  lim(m.SendTransaction.QueueLimit, m.SendTransaction.MaxExecutionDuration),
		protocol.SimulateTransactionMethodName: lim(
			m.SimulateTransaction.QueueLimit, m.SimulateTransaction.MaxExecutionDuration),
		protocol.GetFeeStatsMethodName: lim(m.GetFeeStats.QueueLimit, m.GetFeeStats.MaxExecutionDuration),
	}
}

// notImplemented is the stub for a method in the table but not built yet. An
// explicit error, never an empty success: an empty page would tell a paging
// client "nothing exists", which is a lie. jrpc2.MethodNotFound (-32601) is the
// spec's "method does not exist / is not available".
func notImplemented(message string) jrpc2.Handler {
	return func(context.Context, *jrpc2.Request) (any, error) {
		return nil, &jrpc2.Error{Code: jrpc2.MethodNotFound, Message: message}
	}
}

// wrapAdapterRequest is the per-request scope around every handler: it plants
// the adapters' shared read view (adapters.WithSharedView, released after the
// handler returns) and rewrites the routing/lifecycle failures the shared
// handlers can only report as generic internal errors. The original error
// survives the handler through a context mark (see adapters.WithErrorMark);
// this wrapper classifies it after the handler fails:
//
//   - a request that raced a store handoff (query.ErrUnavailable or
//     stores.ErrStoreClosed) becomes network.ErrTemporarilyUnavailable — both
//     conditions self-heal, so retry is the honest instruction;
//   - a scan the router rejected as below the servable window
//     (*query.RangeError) becomes the v1-style invalid-request rejection;
//   - everything else passes through untouched.
func wrapAdapterRequest(h jrpc2.Handler, metrics observability.Metrics) jrpc2.Handler {
	return func(ctx context.Context, req *jrpc2.Request) (any, error) {
		ctx, mark := adapters.WithErrorMark(ctx)
		ctx, releaseView := adapters.WithSharedView(ctx)
		// Deferred so a panicking handler cannot leak the view: the duration
		// limiter recovers panics above this frame and keeps the process
		// serving, so a skipped release would orphan the RocksDB snapshot.
		defer releaseView()
		result, err := h(ctx, req)
		if err == nil {
			return result, nil
		}
		marked := mark.Err()
		var rangeErr *query.RangeError
		switch {
		case errors.As(marked, &rangeErr):
			return nil, &jrpc2.Error{Code: jrpc2.InvalidRequest, Message: rangeErr.Error()}
		case errors.Is(marked, stores.ErrStoreClosed):
			// A dead context reaches no client and says nothing about the
			// grace period (see observability.Metrics.StoreClosedServed).
			if ctx.Err() == nil {
				metrics.StoreClosedServed()
			}
			fallthrough
		case errors.Is(marked, query.ErrUnavailable):
			return nil, network.ErrTemporarilyUnavailable
		}
		return result, err
	}
}

// graceMargin is the slack deriveLifecycleGrace adds on top of the longest
// request timeout. It covers the gap between a request's deadline firing and
// its handler goroutine actually stopping: the duration limiter answers the
// client at the deadline but the handler keeps running until it observes its
// canceled context, which scan loops only check between iterations.
const graceMargin = 30 * time.Second

// deriveLifecycleGrace computes the deferred-deletion grace period from the
// serving timeouts: the longest time any request can run, plus graceMargin.
//
// INVARIANT: the request timeouts and the grace period move together. There is
// no reader refcount — after a lifecycle run demotes a resource, this grace is
// the ONLY thing separating an in-flight request from os.Remove. Deriving it
// here (instead of a constant) means an operator raising a method's
// max_execution_duration automatically widens the grace; a constant would let
// the two drift until a slow request reads deleted files.
func deriveLifecycleGrace(svc config.ServiceConfig) time.Duration {
	// The global HTTP-layer limit bounds every request, including any future
	// method only it covers, so it participates in the max alongside the
	// per-method budgets.
	longest := max(deref(svc.MaxRequestExecutionDuration), specLimits(svc.Methods).LongestDuration())
	return longest + graceMargin
}
