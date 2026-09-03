package rpcv2

import (
	"context"
	"math"
	"time"

	"github.com/creachadair/jrpc2"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/jsonrpc"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/methods"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/eventsapi"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/feewindow"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/observability"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

// handlerParams carries everything newJSONRPCHandler composes into the method
// table besides the config: the per-run readers over the query router, the
// process-wide core-backed pieces, and the sinks.
type handlerParams struct {
	daemon            host.Daemon
	logger            *supportlog.Entry
	metrics           observability.Metrics
	registry          *query.Registry
	ledgerReader      store.LedgerReader
	transactionReader store.TransactionReader
	feeWindows        *feewindow.FeeWindows
	preflightGetter   methods.PreflightGetter
	networkPassphrase string
	retentionWindow   uint32
}

// newJSONRPCHandler maps the v2 config onto the shared method-spec builder —
// the v2 counterpart of rpcv1.NewJSONRPCHandler. The handlers are the shared
// internal/methods constructors, unmodified; only their inputs are v2's (the
// router-backed adapters, the daemon-owned fee windows, captive-core state).
// getEventsV2 is the exception. v1 has no such method, so eventsapi
// implements it natively. v1 getEvents stays a not-implemented stub until the
// v1 shim lands.
func newJSONRPCHandler(cfg config.Config, p handlerParams) jsonrpc.Handler {
	m := cfg.Service.Methods
	specs := jsonrpc.BuildHandlerSpecs(
		jsonrpc.HandlerDeps{
			Daemon:            p.daemon,
			Logger:            p.logger,
			PreflightGetter:   p.preflightGetter,
			LedgerReader:      p.ledgerReader,
			TransactionReader: p.transactionReader,
			FeeStats:          p.feeWindows,

			GetEventsHandler: notImplemented(protocol.GetEventsMethodName),

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
		MethodName: protocol.GetEventsV2MethodName,
		Handler: eventsapi.NewHandler(eventsapi.Limits{
			TermBudget:   uint32(min(deref(m.GetEventsV2.TermBudget), math.MaxUint32)), //nolint:gosec // min clamps it
			MaxLimit:     deref(m.GetEventsV2.MaxItemsPerResponse),
			DefaultLimit: deref(m.GetEventsV2.DefaultItemsPerResponse),
		}),
	})
	specs = limitsByMethod(m).Apply(specs)
	for i := range specs {
		specs[i].Handler = wrapAdapterRequest(specs[i].Handler, p.registry)
		if specs[i].MethodName == protocol.GetHealthMethodName {
			specs[i].Handler = gateHealthOnFirstCommit(specs[i].Handler, p.registry)
		}
	}

	return jsonrpc.NewHandler(jsonrpc.Params{
		Daemon:                p.daemon,
		Logger:                p.logger,
		Specs:                 specs,
		GlobalQueueLimit:      deref(cfg.Service.MaxConcurrentRequests),
		GlobalDurationWarning: deref(cfg.Service.RequestExecutionWarningThreshold),
		GlobalDurationLimit:   deref(cfg.Service.MaxRequestExecutionDuration),
	})
}

// limitsByMethod maps [service.methods] onto the shared limits table. Both the
// method table and the deletion grace (deriveLifecycleGrace) read it, so the
// two cannot disagree on a method's budget.
func limitsByMethod(m config.MethodsConfig) jsonrpc.LimitsByMethod {
	lim := func(queue *uint, dur *time.Duration) jsonrpc.MethodLimits {
		return jsonrpc.MethodLimits{QueueLimit: deref(queue), RequestDurationLimit: deref(dur)}
	}
	return jsonrpc.LimitsByMethod{
		protocol.GetHealthMethodName:        lim(m.GetHealth.QueueLimit, m.GetHealth.MaxExecutionDuration),
		protocol.GetEventsMethodName:        lim(m.GetEvents.QueueLimit, m.GetEvents.MaxExecutionDuration),
		protocol.GetEventsV2MethodName:      lim(m.GetEventsV2.QueueLimit, m.GetEventsV2.MaxExecutionDuration),
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

// notImplemented is the stub for a method in the table but not built yet; its
// one use dies when the v1 events shim lands. An explicit error, never an
// empty success: an empty page would tell a paging client "nothing exists",
// which is a lie. jrpc2.MethodNotFound (-32601) is the spec's "method does
// not exist / is not available".
func notImplemented(method string) jrpc2.Handler {
	message := method + " is not implemented by this service yet (issue #774 adds it);" +
		" use the existing RPC service for events meanwhile"
	return func(context.Context, *jrpc2.Request) (any, error) {
		return nil, &jrpc2.Error{Code: jrpc2.MethodNotFound, Message: message}
	}
}

// wrapAdapterRequest is the per-request scope around every handler: it
// acquires the request's read view up front, plants it on the context
// (adapters.WithView), and releases it after the handler returns.
//
// The wrapper does NOT classify adapter errors. The routing and lifecycle
// failures it used to remap to "retry" are unreachable within the serving
// model (validation shares the scan's snapshot; coverage publishes before
// discard; the window gates close the prune race; a request old enough to see
// a deferred close was answered by the duration limiter long before). Each
// condition is counted at its production site instead — see the
// serving-invariant counters in observability.NewPrometheusMetrics.
func wrapAdapterRequest(h jrpc2.Handler, registry *query.Registry) jrpc2.Handler {
	return func(ctx context.Context, req *jrpc2.Request) (any, error) {
		view, err := registry.NewReadView()
		if err != nil {
			return nil, err
		}
		// Deferred so a panicking handler cannot leak the view: the duration
		// limiter recovers panics above this frame and keeps the process
		// serving, so a skipped release would orphan the RocksDB snapshot.
		defer view.Release()
		return h(query.WithView(ctx, view), req)
	}
}

// gateHealthOnFirstCommit fails getHealth until this run commits a ledger.
// Close times survive restarts in the durable stores, so freshness alone
// cannot tell a working node from one whose ingestion never started. v1
// keeps the shared behavior.
func gateHealthOnFirstCommit(h jrpc2.Handler, registry *query.Registry) jrpc2.Handler {
	return func(ctx context.Context, req *jrpc2.Request) (any, error) {
		if !registry.HasCommittedSinceBoot() {
			return nil, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: "ingestion has not committed a ledger since this process started",
			}
		}
		return h(ctx, req)
	}
}

// graceMargin is the slack deriveLifecycleGrace adds on top of the longest
// request timeout. It covers the gap between a request's deadline firing and
// its handler goroutine actually stopping: the duration limiter answers the
// client at the deadline but the handler keeps running until it observes its
// canceled context, which scan loops only check between iterations. (Verified:
// the loops in stores/event, stores/txhash, packfile and adapters do check
// per iteration. The orchestration layers above them do not, which is why the
// margin is 30s and not zero.)
//
// jsonrpc.Handler.Shutdown cancels handler contexts too, at teardown. That can
// only shorten a handler's life, so this stays an upper bound.
const graceMargin = 30 * time.Second

// deriveLifecycleGrace computes the deferred-deletion grace period from the
// serving timeouts: the longest time any request can run, plus graceMargin.
//
// INVARIANT: the request timeouts and the grace period move together.
// Deriving it here (instead of a constant) means an operator raising a
// method's max_execution_duration automatically widens the grace; a constant
// would let the two drift until a slow request reads deleted files.
//
// What the grace buys is AVAILABILITY, not memory safety: it covers a view
// that predates a demotion opening a cold artifact for the FIRST time
// afterwards (query-routing-design.md, "Filesystem unlink semantics alone").
// Memory safety comes from ownership and drain barriers — rocksdb's per-op
// read lock, CloseIfIdle declining under an in-flight op — because the
// deadline bounds the response, not the handler goroutine.
//
// The handler drain is best-effort (readShutdownTimeout here, rpcv1's
// defaultShutdownGracePeriod there) and the daemon closes its stores when it
// expires, so store ownership is the backstop and not this. Relaxing the
// per-op read lock as an optimization would silently turn a drain timeout into
// a use-after-free, and nothing in the lifecycle code would show why.
func deriveLifecycleGrace(svc config.ServiceConfig) time.Duration {
	// The global HTTP-layer limit bounds every request, including any future
	// method only it covers, so it participates in the max alongside the
	// per-method budgets.
	longest := max(deref(svc.MaxRequestExecutionDuration), limitsByMethod(svc.Methods).LongestDuration())
	if longest > math.MaxInt64-graceMargin {
		// Adding the margin would wrap to a NEGATIVE duration, which
		// lifecycle.WithLifecycleDefaults reads as unset and silently replaces
		// with its 5-minute default — the narrowest grace in the codebase,
		// under the widest budget an operator can configure. Config validates
		// only a minimum. Saturate instead.
		return math.MaxInt64
	}
	return longest + graceMargin
}
