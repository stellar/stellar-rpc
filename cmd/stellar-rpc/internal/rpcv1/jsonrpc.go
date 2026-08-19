package rpcv1

import (
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/jsonrpc"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/methods"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcdatastore"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/feewindow"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

// Handler is the HTTP handler which serves the Soroban JSON RPC responses
type Handler = jsonrpc.Handler

type HandlerParams struct {
	FeeStatWindows        *feewindow.FeeWindows
	TransactionReader     store.TransactionReader
	EventReader           store.EventReader
	LedgerReader          store.LedgerReader
	Logger                *log.Entry
	PreflightGetter       methods.PreflightGetter
	Daemon                host.Daemon
	DataStoreLedgerReader rpcdatastore.LedgerReader
}

// NewJSONRPCHandler constructs a Handler instance: it maps the v1 config onto
// the shared method-spec builder and hands the result to the shared jsonrpc
// assembly.
func NewJSONRPCHandler(cfg *config.Config, params HandlerParams) Handler {
	specs := jsonrpc.BuildHandlerSpecs(
		jsonrpc.SpecDeps{
			Daemon:                params.Daemon,
			Logger:                params.Logger,
			PreflightGetter:       params.PreflightGetter,
			LedgerReader:          params.LedgerReader,
			TransactionReader:     params.TransactionReader,
			EventReader:           params.EventReader,
			FeeStats:              params.FeeStatWindows,
			DataStoreLedgerReader: params.DataStoreLedgerReader,

			NetworkPassphrase:       cfg.NetworkPassphrase,
			FriendbotURL:            cfg.FriendbotURL,
			RetentionWindow:         cfg.HistoryRetentionWindow,
			MaxHealthyLedgerLatency: cfg.MaxHealthyLedgerLatency,

			MaxEventsLimit:           cfg.MaxEventsLimit,
			DefaultEventsLimit:       cfg.DefaultEventsLimit,
			MaxLedgersLimit:          cfg.MaxLedgersLimit,
			DefaultLedgersLimit:      cfg.DefaultLedgersLimit,
			MaxTransactionsLimit:     cfg.MaxTransactionsLimit,
			DefaultTransactionsLimit: cfg.DefaultTransactionsLimit,
		})
	specs = jsonrpc.SpecLimits{
		protocol.GetHealthMethodName: {
			QueueLimit:           cfg.RequestBacklogGetHealthQueueLimit,
			RequestDurationLimit: cfg.MaxGetHealthExecutionDuration,
		},
		protocol.GetEventsMethodName: {
			QueueLimit:           cfg.RequestBacklogGetEventsQueueLimit,
			RequestDurationLimit: cfg.MaxGetEventsExecutionDuration,
		},
		protocol.GetNetworkMethodName: {
			QueueLimit:           cfg.RequestBacklogGetNetworkQueueLimit,
			RequestDurationLimit: cfg.MaxGetNetworkExecutionDuration,
		},
		protocol.GetVersionInfoMethodName: {
			QueueLimit:           cfg.RequestBacklogGetVersionInfoQueueLimit,
			RequestDurationLimit: cfg.MaxGetVersionInfoExecutionDuration,
		},
		protocol.GetLatestLedgerMethodName: {
			QueueLimit:           cfg.RequestBacklogGetLatestLedgerQueueLimit,
			RequestDurationLimit: cfg.MaxGetLatestLedgerExecutionDuration,
		},
		protocol.GetLedgersMethodName: {
			QueueLimit:           cfg.RequestBacklogGetLedgersQueueLimit,
			RequestDurationLimit: cfg.MaxGetLedgersExecutionDuration,
		},
		protocol.GetLedgerEntriesMethodName: {
			QueueLimit:           cfg.RequestBacklogGetLedgerEntriesQueueLimit,
			RequestDurationLimit: cfg.MaxGetLedgerEntriesExecutionDuration,
		},
		protocol.GetTransactionMethodName: {
			QueueLimit:           cfg.RequestBacklogGetTransactionQueueLimit,
			RequestDurationLimit: cfg.MaxGetTransactionExecutionDuration,
		},
		protocol.GetTransactionsMethodName: {
			QueueLimit:           cfg.RequestBacklogGetTransactionsQueueLimit,
			RequestDurationLimit: cfg.MaxGetTransactionsExecutionDuration,
		},
		protocol.SendTransactionMethodName: {
			QueueLimit:           cfg.RequestBacklogSendTransactionQueueLimit,
			RequestDurationLimit: cfg.MaxSendTransactionExecutionDuration,
		},
		protocol.SimulateTransactionMethodName: {
			QueueLimit:           cfg.RequestBacklogSimulateTransactionQueueLimit,
			RequestDurationLimit: cfg.MaxSimulateTransactionExecutionDuration,
		},
		protocol.GetFeeStatsMethodName: {
			QueueLimit:           cfg.RequestBacklogGetFeeStatsTransactionQueueLimit,
			RequestDurationLimit: cfg.MaxGetFeeStatsExecutionDuration,
		},
	}.Apply(specs)

	return jsonrpc.NewHandler(jsonrpc.Params{
		Daemon:                params.Daemon,
		Logger:                params.Logger,
		Metrics:               jsonrpc.NewHandlerMetrics(params.Daemon.MetricsNamespace(), params.Daemon.MetricsRegistry()),
		Specs:                 specs,
		GlobalQueueLimit:      cfg.RequestBacklogGlobalQueueLimit,
		GlobalDurationWarning: cfg.RequestExecutionWarningThreshold,
		GlobalDurationLimit:   cfg.MaxRequestExecutionDuration,
	})
}
