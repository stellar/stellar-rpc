package rpcv1

import (
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/jsonrpc"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/methods"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcdatastore"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/feewindow"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

const (
	// Decoded output size limits for XDR unmarshaling of user-supplied input.
	ledgerKeyDecodeMaxMemory   = 16 * 1024   // 16 KB
	transactionDecodeMaxMemory = 1024 * 1024 // 1 MB
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
// the plain method-spec list and hands that to the shared jsonrpc builder.
//
//nolint:funlen
func NewJSONRPCHandler(cfg *config.Config, params HandlerParams) Handler {
	retentionWindow := cfg.HistoryRetentionWindow

	specs := []jsonrpc.HandlerSpec{
		{
			MethodName: protocol.GetHealthMethodName,
			Handler: methods.NewHealthCheck(
				retentionWindow, params.LedgerReader, cfg.MaxHealthyLedgerLatency),
			QueueLimit:           cfg.RequestBacklogGetHealthQueueLimit,
			RequestDurationLimit: cfg.MaxGetHealthExecutionDuration,
		},
		{
			MethodName: protocol.GetEventsMethodName,
			Handler: methods.NewGetEventsHandler(
				params.Logger,
				params.EventReader,
				cfg.MaxEventsLimit,
				cfg.DefaultEventsLimit,
				params.LedgerReader,
			),
			QueueLimit:           cfg.RequestBacklogGetEventsQueueLimit,
			RequestDurationLimit: cfg.MaxGetEventsExecutionDuration,
		},
		{
			MethodName: protocol.GetNetworkMethodName,
			Handler: methods.NewGetNetworkHandler(
				cfg.NetworkPassphrase,
				cfg.FriendbotURL,
				params.LedgerReader,
			),
			QueueLimit:           cfg.RequestBacklogGetNetworkQueueLimit,
			RequestDurationLimit: cfg.MaxGetNetworkExecutionDuration,
		},
		{
			MethodName: protocol.GetVersionInfoMethodName,
			Handler: methods.NewGetVersionInfoHandler(params.Logger,
				params.LedgerReader, params.Daemon),
			QueueLimit:           cfg.RequestBacklogGetVersionInfoQueueLimit,
			RequestDurationLimit: cfg.MaxGetVersionInfoExecutionDuration,
		},
		{
			MethodName:           protocol.GetLatestLedgerMethodName,
			Handler:              methods.NewGetLatestLedgerHandler(params.LedgerReader),
			QueueLimit:           cfg.RequestBacklogGetLatestLedgerQueueLimit,
			RequestDurationLimit: cfg.MaxGetLatestLedgerExecutionDuration,
		},
		{
			MethodName: protocol.GetLedgersMethodName,
			Handler: methods.NewGetLedgersHandler(params.LedgerReader,
				cfg.MaxLedgersLimit, cfg.DefaultLedgersLimit, params.DataStoreLedgerReader, params.Logger),
			QueueLimit:           cfg.RequestBacklogGetLedgersQueueLimit,
			RequestDurationLimit: cfg.MaxGetLedgersExecutionDuration,
		},
		{
			MethodName: protocol.GetLedgerEntriesMethodName,
			Handler: methods.NewGetLedgerEntriesHandler(params.Logger,
				params.Daemon.FastCoreClient(), params.LedgerReader,
				xdr.DecodeOptions{MaxMemoryBytes: ledgerKeyDecodeMaxMemory}),
			QueueLimit:           cfg.RequestBacklogGetLedgerEntriesQueueLimit,
			RequestDurationLimit: cfg.MaxGetLedgerEntriesExecutionDuration,
		},
		{
			MethodName:           protocol.GetTransactionMethodName,
			Handler:              methods.NewGetTransactionHandler(params.Logger, params.TransactionReader, params.LedgerReader),
			QueueLimit:           cfg.RequestBacklogGetTransactionQueueLimit,
			RequestDurationLimit: cfg.MaxGetTransactionExecutionDuration,
		},
		{
			MethodName: protocol.GetTransactionsMethodName,
			Handler: methods.NewGetTransactionsHandler(params.Logger, params.LedgerReader,
				cfg.MaxTransactionsLimit, cfg.DefaultTransactionsLimit, cfg.NetworkPassphrase),
			QueueLimit:           cfg.RequestBacklogGetTransactionsQueueLimit,
			RequestDurationLimit: cfg.MaxGetTransactionsExecutionDuration,
		},
		{
			MethodName: protocol.SendTransactionMethodName,
			Handler: methods.NewSendTransactionHandler(
				params.Daemon, params.Logger, params.LedgerReader, cfg.NetworkPassphrase,
				xdr.DecodeOptions{MaxMemoryBytes: transactionDecodeMaxMemory}),
			QueueLimit:           cfg.RequestBacklogSendTransactionQueueLimit,
			RequestDurationLimit: cfg.MaxSendTransactionExecutionDuration,
		},
		{
			MethodName: protocol.SimulateTransactionMethodName,
			Handler: methods.NewSimulateTransactionHandler(
				params.Logger, params.LedgerReader,
				params.Daemon.FastCoreClient(), params.PreflightGetter,
				xdr.DecodeOptions{MaxMemoryBytes: transactionDecodeMaxMemory}),
			QueueLimit:           cfg.RequestBacklogSimulateTransactionQueueLimit,
			RequestDurationLimit: cfg.MaxSimulateTransactionExecutionDuration,
		},
		{
			MethodName:           protocol.GetFeeStatsMethodName,
			Handler:              methods.NewGetFeeStatsHandler(params.FeeStatWindows, params.LedgerReader, params.Logger),
			QueueLimit:           cfg.RequestBacklogGetFeeStatsTransactionQueueLimit,
			RequestDurationLimit: cfg.MaxGetFeeStatsExecutionDuration,
		},
	}

	return jsonrpc.NewHandler(jsonrpc.Params{
		Daemon:                params.Daemon,
		Logger:                params.Logger,
		Specs:                 specs,
		GlobalQueueLimit:      cfg.RequestBacklogGlobalQueueLimit,
		GlobalDurationWarning: cfg.RequestExecutionWarningThreshold,
		GlobalDurationLimit:   cfg.MaxRequestExecutionDuration,
	})
}
