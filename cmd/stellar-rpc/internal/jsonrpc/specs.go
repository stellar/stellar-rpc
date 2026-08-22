package jsonrpc

import (
	"time"

	"github.com/creachadair/jrpc2"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/methods"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcdatastore"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

// SpecDeps carries every input the shared internal/methods constructors take.
// Each daemon fills it from its own config and wiring and hands it to
// BuildHandlerSpecs, so the method table — which method runs which handler
// with which decode limits — is written once instead of once per daemon.
type SpecDeps struct {
	Daemon          host.Daemon
	Logger          *log.Entry
	PreflightGetter methods.PreflightGetter

	LedgerReader      store.LedgerReader
	TransactionReader store.TransactionReader
	FeeStats          store.FeeStats

	// GetEventsHandler serves getEvents. Required: events handlers are
	// per-daemon — the two daemons' event stores share no interface, so each
	// builds its own handler (v1 wraps its database reader via
	// methods.NewGetEventsHandler; the full-history daemon serves a stub until
	// #774 wires its event store) and the shared table only mounts it.
	GetEventsHandler jrpc2.Handler

	// DataStoreLedgerReader is getLedgers' bulk fallback for ledgers below
	// local retention. nil means no fallback: the handler serves everything
	// from LedgerReader.
	DataStoreLedgerReader rpcdatastore.LedgerReader

	NetworkPassphrase string
	// FriendbotURL is echoed by getNetwork; "" omits the field from the
	// response.
	FriendbotURL string

	RetentionWindow         uint32
	MaxHealthyLedgerLatency time.Duration

	MaxLedgersLimit          uint
	DefaultLedgersLimit      uint
	MaxTransactionsLimit     uint
	DefaultTransactionsLimit uint
}

// MethodLimits is one method's serving limits: the request backlog cap and the
// per-request execution budget.
type MethodLimits struct {
	QueueLimit           uint
	RequestDurationLimit time.Duration
}

// SpecLimits maps each served method's wire name to its limits. Key shared
// methods by the protocol.*MethodName constants. Apply panics on a table
// missing a served method or naming an unserved one, so a daemon's limits
// mapping cannot silently drift from its method list: adding a method fails
// that daemon's startup (and its handler tests) until it supplies the new
// method's limits.
type SpecLimits map[string]MethodLimits

// LongestDuration is the largest per-method execution budget in the table.
// The full-history daemon derives its deferred-deletion grace period from it.
func (l SpecLimits) LongestDuration() time.Duration {
	var longest time.Duration
	for _, limits := range l {
		longest = max(longest, limits.RequestDurationLimit)
	}
	return longest
}

// Apply pairs each spec with its limits from the table and returns the
// completed list — the step that makes a method list servable. Run it over a
// daemon's COMPLETE list (the shared BuildHandlerSpecs result plus any methods
// that daemon appended). A served method missing from the table, or a table
// entry no spec claims, panics at startup.
func (l SpecLimits) Apply(specs []HandlerSpec) []HandlerSpec {
	used := make(map[string]bool, len(l))
	for i := range specs {
		limits, ok := l[specs[i].MethodName]
		if !ok {
			panic("jsonrpc: no limits configured for method " + specs[i].MethodName)
		}
		used[specs[i].MethodName] = true
		specs[i].QueueLimit = limits.QueueLimit
		specs[i].RequestDurationLimit = limits.RequestDurationLimit
	}
	for name := range l {
		if !used[name] {
			panic("jsonrpc: limits configured for unserved method " + name)
		}
	}
	return specs
}

// BuildHandlerSpecs builds the method list both daemons share: the
// internal/methods handlers over deps, without limits. A daemon appends its
// own methods to the result, then fills every spec's limits with
// SpecLimits.Apply before serving.
func BuildHandlerSpecs(deps SpecDeps) []HandlerSpec {
	if deps.GetEventsHandler == nil {
		panic("jsonrpc: SpecDeps.GetEventsHandler is required — events handlers are per-daemon")
	}
	spec := func(name string, h jrpc2.Handler) HandlerSpec {
		return HandlerSpec{MethodName: name, Handler: h}
	}
	return []HandlerSpec{
		spec(protocol.GetHealthMethodName,
			methods.NewHealthCheck(deps.RetentionWindow, deps.LedgerReader, deps.MaxHealthyLedgerLatency)),
		spec(protocol.GetEventsMethodName, deps.GetEventsHandler),
		spec(protocol.GetNetworkMethodName,
			methods.NewGetNetworkHandler(deps.NetworkPassphrase, deps.FriendbotURL, deps.LedgerReader)),
		spec(protocol.GetVersionInfoMethodName,
			methods.NewGetVersionInfoHandler(deps.Logger, deps.LedgerReader, deps.Daemon)),
		spec(protocol.GetLatestLedgerMethodName,
			methods.NewGetLatestLedgerHandler(deps.LedgerReader)),
		spec(protocol.GetLedgersMethodName,
			methods.NewGetLedgersHandler(deps.LedgerReader, deps.MaxLedgersLimit, deps.DefaultLedgersLimit,
				deps.DataStoreLedgerReader, deps.Logger)),
		spec(protocol.GetLedgerEntriesMethodName,
			methods.NewGetLedgerEntriesHandler(deps.Logger, deps.Daemon.FastCoreClient(), deps.LedgerReader,
				xdr.DecodeOptions{MaxMemoryBytes: LedgerKeyDecodeMaxMemory})),
		spec(protocol.GetTransactionMethodName,
			methods.NewGetTransactionHandler(deps.Logger, deps.TransactionReader, deps.LedgerReader)),
		spec(protocol.GetTransactionsMethodName,
			methods.NewGetTransactionsHandler(deps.Logger, deps.LedgerReader, deps.MaxTransactionsLimit,
				deps.DefaultTransactionsLimit, deps.NetworkPassphrase)),
		spec(protocol.SendTransactionMethodName,
			methods.NewSendTransactionHandler(deps.Daemon, deps.Logger, deps.LedgerReader, deps.NetworkPassphrase,
				xdr.DecodeOptions{MaxMemoryBytes: TransactionDecodeMaxMemory})),
		spec(protocol.SimulateTransactionMethodName,
			methods.NewSimulateTransactionHandler(deps.Logger, deps.LedgerReader, deps.Daemon.FastCoreClient(),
				deps.PreflightGetter, xdr.DecodeOptions{MaxMemoryBytes: TransactionDecodeMaxMemory})),
		spec(protocol.GetFeeStatsMethodName,
			methods.NewGetFeeStatsHandler(deps.FeeStats, deps.LedgerReader, deps.Logger)),
	}
}
