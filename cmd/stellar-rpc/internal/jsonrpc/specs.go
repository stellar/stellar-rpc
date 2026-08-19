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
	EventReader       store.EventReader
	FeeStats          store.FeeStats

	// GetEventsHandler, when non-nil, is served for getEvents instead of the
	// handler built from EventReader and the events limits below. The
	// full-history daemon sets it to a not-implemented stub until #774 wires
	// its event store.
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

	MaxEventsLimit           uint
	DefaultEventsLimit       uint
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

// SpecLimits maps each served method's wire name to its limits. Key by the
// protocol.*MethodName constants. BuildHandlerSpecs panics on a table missing
// a served method or naming an unserved one, so a daemon's limits mapping
// cannot silently drift from the method table: adding a method to
// BuildHandlerSpecs fails both daemons' startup (and their handler tests)
// until each supplies the new method's limits.
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

// ExtraMethod is a method one daemon serves beyond the shared table (e.g.
// v2-only methods). Its limits come from the same SpecLimits map as the
// shared methods, so it participates in the missing/unserved validation and
// in LongestDuration.
type ExtraMethod struct {
	MethodName string
	Handler    jrpc2.Handler
}

// BuildHandlerSpecs builds the method table both daemons serve: the shared
// internal/methods handlers over deps, each paired with its limits, plus any
// daemon-specific extra methods. The result feeds Params.Specs, after any
// daemon-specific per-handler wrapping.
func BuildHandlerSpecs(deps SpecDeps, limits SpecLimits, extra ...ExtraMethod) []HandlerSpec {
	getEvents := deps.GetEventsHandler
	if getEvents == nil {
		getEvents = methods.NewGetEventsHandler(deps.Logger, deps.EventReader,
			deps.MaxEventsLimit, deps.DefaultEventsLimit, deps.LedgerReader)
	}
	used := make(map[string]bool, len(limits))
	spec := func(name string, h jrpc2.Handler) HandlerSpec {
		l, ok := limits[name]
		if !ok {
			panic("jsonrpc: no limits configured for method " + name)
		}
		used[name] = true
		return HandlerSpec{
			MethodName:           name,
			Handler:              h,
			QueueLimit:           l.QueueLimit,
			RequestDurationLimit: l.RequestDurationLimit,
		}
	}
	specs := []HandlerSpec{ //nolint:prealloc // built once per daemon startup; the append below is trivial
		spec(protocol.GetHealthMethodName,
			methods.NewHealthCheck(deps.RetentionWindow, deps.LedgerReader, deps.MaxHealthyLedgerLatency)),
		spec(protocol.GetEventsMethodName, getEvents),
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
	for _, e := range extra {
		specs = append(specs, spec(e.MethodName, e.Handler))
	}
	for name := range limits {
		if !used[name] {
			panic("jsonrpc: limits configured for unserved method " + name)
		}
	}
	return specs
}
