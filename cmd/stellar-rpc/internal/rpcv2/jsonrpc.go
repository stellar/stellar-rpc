package rpcv2

import (
	"context"
	"time"

	"github.com/creachadair/jrpc2"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/jsonrpc"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/methods"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcdatastore"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/adapters"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/feewindow"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/observability"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

const (
	// Decoded output size limits for XDR unmarshaling of user-supplied input
	// (v1's values, internal/rpcv1/jsonrpc.go).
	ledgerKeyDecodeMaxMemory   = 16 * 1024   // 16 KB
	transactionDecodeMaxMemory = 1024 * 1024 // 1 MB

	// errCodeTemporarilyUnavailable is this repo's implementation-defined
	// JSON-RPC code for a request that failed against a store mid-handoff and
	// will succeed on retry. Sits in the -3200x band next to the network
	// package's -32001 (request timeout) and -32003 (internal issue).
	errCodeTemporarilyUnavailable = -32002
)

// handlerParams carries everything newJSONRPCHandler composes into the method
// table besides the config: the per-attempt readers over the query router, the
// process-wide core-backed pieces, and the sinks.
type handlerParams struct {
	daemon            host.Daemon
	logger            *supportlog.Entry
	metrics           observability.Metrics
	ledgerReader      store.LedgerReader
	transactionReader store.TransactionReader
	feeWindows        *feewindow.FeeWindows
	preflightGetter   methods.PreflightGetter
	networkPassphrase string
	retentionWindow   uint32
}

// newJSONRPCHandler maps the v2 config onto the shared method-spec list and
// hands it to the shared jsonrpc builder — the v2 counterpart of
// rpcv1.NewJSONRPCHandler. The handlers are the shared internal/methods
// constructors, unmodified; only their inputs are v2's (the router-backed
// adapters, the daemon-owned fee windows, captive-core state). getEvents is
// the one exception: a stub that reports not-implemented until #774 ships it.
//
//nolint:funlen // one spec entry per served method, mirroring rpcv1
func newJSONRPCHandler(cfg config.Config, p handlerParams) jsonrpc.Handler {
	m := cfg.Service.Methods
	// getLedgers can fall back to a bulk datastore for ledgers below local
	// retention; the full-history daemon IS the deep-history store, so none is
	// configured and the handler serves everything locally.
	var noDatastore rpcdatastore.LedgerReader

	specs := []jsonrpc.HandlerSpec{
		{
			MethodName: protocol.GetHealthMethodName,
			Handler: methods.NewHealthCheck(
				p.retentionWindow, p.ledgerReader, deref(m.GetHealth.MaxHealthyLedgerLatency)),
			QueueLimit:           deref(m.GetHealth.QueueLimit),
			RequestDurationLimit: deref(m.GetHealth.MaxExecutionDuration),
		},
		{
			MethodName: protocol.GetEventsMethodName,
			Handler: notImplemented("getEvents is not implemented by this service yet (issue #774 adds it);" +
				" use the existing RPC service for events meanwhile"),
			QueueLimit:           deref(m.GetEvents.QueueLimit),
			RequestDurationLimit: deref(m.GetEvents.MaxExecutionDuration),
		},
		{
			MethodName: protocol.GetNetworkMethodName,
			// No friendbot URL: the full-history daemon serves history networks,
			// and v2 has no friendbot config key. The handler omits the field.
			Handler:              methods.NewGetNetworkHandler(p.networkPassphrase, "", p.ledgerReader),
			QueueLimit:           deref(m.GetNetwork.QueueLimit),
			RequestDurationLimit: deref(m.GetNetwork.MaxExecutionDuration),
		},
		{
			MethodName:           protocol.GetVersionInfoMethodName,
			Handler:              methods.NewGetVersionInfoHandler(p.logger, p.ledgerReader, p.daemon),
			QueueLimit:           deref(m.GetVersionInfo.QueueLimit),
			RequestDurationLimit: deref(m.GetVersionInfo.MaxExecutionDuration),
		},
		{
			MethodName:           protocol.GetLatestLedgerMethodName,
			Handler:              methods.NewGetLatestLedgerHandler(p.ledgerReader),
			QueueLimit:           deref(m.GetLatestLedger.QueueLimit),
			RequestDurationLimit: deref(m.GetLatestLedger.MaxExecutionDuration),
		},
		{
			MethodName: protocol.GetLedgersMethodName,
			Handler: methods.NewGetLedgersHandler(p.ledgerReader,
				deref(m.GetLedgers.MaxItemsPerResponse), deref(m.GetLedgers.DefaultItemsPerResponse),
				noDatastore, p.logger),
			QueueLimit:           deref(m.GetLedgers.QueueLimit),
			RequestDurationLimit: deref(m.GetLedgers.MaxExecutionDuration),
		},
		{
			MethodName: protocol.GetLedgerEntriesMethodName,
			Handler: methods.NewGetLedgerEntriesHandler(p.logger,
				p.daemon.FastCoreClient(), p.ledgerReader,
				xdr.DecodeOptions{MaxMemoryBytes: ledgerKeyDecodeMaxMemory}),
			QueueLimit:           deref(m.GetLedgerEntries.QueueLimit),
			RequestDurationLimit: deref(m.GetLedgerEntries.MaxExecutionDuration),
		},
		{
			MethodName:           protocol.GetTransactionMethodName,
			Handler:              methods.NewGetTransactionHandler(p.logger, p.transactionReader, p.ledgerReader),
			QueueLimit:           deref(m.GetTransaction.QueueLimit),
			RequestDurationLimit: deref(m.GetTransaction.MaxExecutionDuration),
		},
		{
			MethodName: protocol.GetTransactionsMethodName,
			Handler: methods.NewGetTransactionsHandler(p.logger, p.ledgerReader,
				deref(m.GetTransactions.MaxItemsPerResponse), deref(m.GetTransactions.DefaultItemsPerResponse),
				p.networkPassphrase),
			QueueLimit:           deref(m.GetTransactions.QueueLimit),
			RequestDurationLimit: deref(m.GetTransactions.MaxExecutionDuration),
		},
		{
			MethodName: protocol.SendTransactionMethodName,
			Handler: methods.NewSendTransactionHandler(
				p.daemon, p.logger, p.ledgerReader, p.networkPassphrase,
				xdr.DecodeOptions{MaxMemoryBytes: transactionDecodeMaxMemory}),
			QueueLimit:           deref(m.SendTransaction.QueueLimit),
			RequestDurationLimit: deref(m.SendTransaction.MaxExecutionDuration),
		},
		{
			MethodName: protocol.SimulateTransactionMethodName,
			Handler: methods.NewSimulateTransactionHandler(
				p.logger, p.ledgerReader, p.daemon.FastCoreClient(), p.preflightGetter,
				xdr.DecodeOptions{MaxMemoryBytes: transactionDecodeMaxMemory}),
			QueueLimit:           deref(m.SimulateTransaction.QueueLimit),
			RequestDurationLimit: deref(m.SimulateTransaction.MaxExecutionDuration),
		},
		{
			MethodName:           protocol.GetFeeStatsMethodName,
			Handler:              methods.NewGetFeeStatsHandler(p.feeWindows, p.ledgerReader, p.logger),
			QueueLimit:           deref(m.GetFeeStats.QueueLimit),
			RequestDurationLimit: deref(m.GetFeeStats.MaxExecutionDuration),
		},
	}
	for i := range specs {
		specs[i].Handler = mapAdapterErrors(specs[i].Handler, p.metrics)
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

// notImplemented is the stub for a method in the table but not built yet. An
// explicit error, never an empty success: an empty page would tell a paging
// client "nothing exists", which is a lie. jrpc2.MethodNotFound (-32601) is the
// spec's "method does not exist / is not available".
func notImplemented(message string) jrpc2.Handler {
	return func(context.Context, *jrpc2.Request) (any, error) {
		return nil, &jrpc2.Error{Code: jrpc2.MethodNotFound, Message: message}
	}
}

// mapAdapterErrors rewrites the routing/lifecycle failures the shared handlers
// can only report as generic internal errors. The shared handlers flatten
// every store error (see adapters.WithErrorMark for the mechanism that
// preserves them anyway):
//
//   - a request that raced a store handoff (adapters hit query.ErrUnavailable
//     or stores.ErrStoreClosed) becomes -32002 "temporarily unavailable" — both
//     conditions self-heal, so retry is the honest instruction;
//   - a scan the router rejected as below the servable window
//     (*query.RangeError) becomes the v1-style invalid-request rejection;
//   - everything else passes through untouched.
//
// A store closed under an in-flight request also counts on the
// StoreClosedServed metric: reaching a client at all means the request
// outlived the deletion grace period, which operators must see.
func mapAdapterErrors(h jrpc2.Handler, metrics observability.Metrics) jrpc2.Handler {
	return func(ctx context.Context, req *jrpc2.Request) (any, error) {
		ctx, mark := adapters.WithErrorMark(ctx)
		result, err := h(ctx, req)
		if err == nil {
			return result, nil
		}
		if mark.StoreClosed() {
			metrics.StoreClosedServed()
		}
		if rangeErr := mark.RangeError(); rangeErr != nil {
			return result, &jrpc2.Error{Code: jrpc2.InvalidRequest, Message: rangeErr.Error()}
		}
		if mark.Transient() {
			return result, &jrpc2.Error{
				Code:    errCodeTemporarilyUnavailable,
				Message: "temporarily unavailable — a store serving this request was being replaced; retry",
			}
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
	m := svc.Methods
	longest := deref(svc.MaxRequestExecutionDuration)
	for _, d := range []*time.Duration{
		m.GetHealth.MaxExecutionDuration,
		m.GetNetwork.MaxExecutionDuration,
		m.GetVersionInfo.MaxExecutionDuration,
		m.GetLatestLedger.MaxExecutionDuration,
		m.GetTransaction.MaxExecutionDuration,
		m.GetTransactions.MaxExecutionDuration,
		m.GetLedgers.MaxExecutionDuration,
		m.GetEvents.MaxExecutionDuration,
		m.GetFeeStats.MaxExecutionDuration,
		m.SendTransaction.MaxExecutionDuration,
		m.SimulateTransaction.MaxExecutionDuration,
		m.GetLedgerEntries.MaxExecutionDuration,
	} {
		longest = max(longest, deref(d))
	}
	return longest + graceMargin
}
