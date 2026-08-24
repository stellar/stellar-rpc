package rpcv2

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/config"
)

// wantNanosecondHint is the fragment every sub-millisecond-duration rejection
// carries, telling the operator to write the string form instead.
const wantNanosecondHint = "nanoseconds"

// validCfg builds a valid Config; callers mutate one field to drive a rejection.
// Defaults are applied, matching production (validateConfig's contract is a
// post-WithDefaults config — every [service] pointer non-nil).
func validCfg(workers, maxRetries int, earliest string) config.Config {
	return config.Config{
		Storage:   config.StorageConfig{DefaultDataDir: "/data"},
		Retention: config.RetentionConfig{EarliestLedger: earliest},
		Backfill:  config.BackfillConfig{Workers: &workers, MaxRetries: &maxRetries},
		Ingestion: config.IngestionConfig{CaptiveCoreConfig: "/cc"},
	}.WithDefaults()
}

// readyTip returns a tip backend that always reports the given ledger.
func readyTip(ledger uint32) *fakeTipBackend {
	return &fakeTipBackend{tips: []uint32{ledger}}
}

// downTip returns a tip backend that never becomes usable. Sub-genesis reads as
// a permanent "not ready", so validateConfig (which applies the production retry
// policy) fails fast instead of sleeping through the backoff.
func downTip() *fakeTipBackend {
	return &fakeTipBackend{tips: []uint32{0}}
}

// callValidate runs validateConfig, returning the earliest_ledger it pinned.
func callValidate(t *testing.T, cfg config.Config, cat *catalog.Catalog, tip *fakeTipBackend) (uint32, error) {
	t.Helper()
	return validateConfig(context.Background(), cfg, cat, tip.Tip)
}

// requireEarliestPin asserts the earliest_ledger pin reads back as wantEarliest;
// also the anchor for restart-mutates-nothing assertions.
func requireEarliestPin(t *testing.T, cat *catalog.Catalog, wantEarliest uint32) {
	t.Helper()
	el, ok, err := cat.EarliestLedger()
	require.NoError(t, err, "readback of earliest_ledger pin")
	require.True(t, ok, "earliest_ledger pin must be present after validateConfig")
	require.Equal(t, wantEarliest, el, "earliest_ledger pin readback")
}

// ---------------------------------------------------------------------------
// Accept the documented-valid forms.
// ---------------------------------------------------------------------------

func TestValidateConfig_AcceptsGenesisFirstStart(t *testing.T) {
	cat, _ := testCatalog(t)
	// Genesis needs no tip: a down backend is fine.
	earliest, err := callValidate(t, validCfg(4, 3, "genesis"), cat, downTip())
	require.NoError(t, err)
	assert.Equal(t, uint32(chunk.FirstLedgerSeq), earliest)

	// Pin committed.
	el, ok, err := cat.EarliestLedger()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, uint32(chunk.FirstLedgerSeq), el)
}

func TestValidateConfig_AcceptsNowFirstStart(t *testing.T) {
	cat, _ := testCatalog(t)
	// chunk 5 first ledger is 50002; a tip mid-chunk-5 resolves "now" to 50002.
	tipLedger := chunk.ID(5).FirstLedger() + 1234
	earliest, err := callValidate(t, validCfg(4, 3, "now"), cat, readyTip(tipLedger))
	require.NoError(t, err)
	assert.Equal(t, chunk.ID(5).FirstLedger(), earliest)

	el, _, _ := cat.EarliestLedger()
	assert.Equal(t, chunk.ID(5).FirstLedger(), el)
}

func TestValidateConfig_AcceptsNumericFirstStart(t *testing.T) {
	cat, _ := testCatalog(t)
	floor := chunk.ID(3).FirstLedger() // 30002, chunk-aligned
	tipLedger := chunk.ID(10).FirstLedger()
	earliest, err := callValidate(t, validCfg(4, 3, itoa(floor)), cat, readyTip(tipLedger))
	require.NoError(t, err)
	assert.Equal(t, floor, earliest)
}

func TestValidateConfig_AcceptsMinWorkersAndZeroRetries(t *testing.T) {
	cat, _ := testCatalog(t)
	_, err := callValidate(t, validCfg(1, 0, "genesis"), cat, downTip())
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// Reject the malformed forms (stateless).
// ---------------------------------------------------------------------------

func TestValidateConfig_RejectsMalformed(t *testing.T) {
	tests := []struct {
		name string
		cfg  config.Config
		want string
	}{
		{"zero workers", validCfg(0, 3, "genesis"), "workers"},
		{"negative workers", validCfg(-1, 3, "genesis"), "workers"},
		{"negative max_retries", validCfg(4, -1, "genesis"), "max_retries"},
		{"bogus earliest string", validCfg(4, 3, "yesterday"), "earliest_ledger"},
		{"sub-genesis numeric floor", validCfg(4, 3, "1"), "earliest_ledger"},
		{"misaligned numeric floor", validCfg(4, 3, "12345"), "earliest_ledger"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cat, _ := testCatalog(t)
			_, err := callValidate(t, tc.cfg, cat, readyTip(chunk.ID(10).FirstLedger()))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)

			// A rejected config pins nothing.
			_, ok, _ := cat.EarliestLedger()
			assert.False(t, ok, "no earliest pin on a rejected config")
		})
	}
}

func TestValidateConfig_RejectsMalformedService(t *testing.T) {
	uintPtr := func(v uint) *uint { return &v }
	uint32Ptr := func(v uint32) *uint32 { return &v }
	durPtr := func(v time.Duration) *time.Duration { return &v }

	tests := []struct {
		name   string
		mutate func(*config.Config)
		want   string
	}{
		{
			"zero max_concurrent_requests",
			func(c *config.Config) { c.Service.MaxConcurrentRequests = uintPtr(0) },
			"max_concurrent_requests",
		},
		{
			"zero per-method queue_limit",
			func(c *config.Config) { c.Service.Methods.GetLedgers.QueueLimit = uintPtr(0) },
			"[service.methods.getLedgers].queue_limit",
		},
		{
			"zero wide-tier queue_limit",
			func(c *config.Config) { c.Service.Methods.QueueLimit = uintPtr(0) },
			"queue_limit",
		},
		{
			// The nanosecond trap: a bare TOML integer 10 decodes as 10ns.
			"sub-millisecond duration",
			func(c *config.Config) { c.Service.Methods.GetEvents.MaxExecutionDuration = durPtr(10) },
			wantNanosecondHint,
		},
		{
			"zero global execution duration",
			func(c *config.Config) { c.Service.MaxRequestExecutionDuration = durPtr(0) },
			"max_request_execution_duration",
		},
		{
			"default items above max",
			func(c *config.Config) {
				c.Service.Methods.GetLedgers.MaxItemsPerResponse = uintPtr(10)
				c.Service.Methods.GetLedgers.DefaultItemsPerResponse = uintPtr(11)
			},
			"default_items_per_response",
		},
		{
			"zero default items",
			func(c *config.Config) { c.Service.Methods.GetEvents.DefaultItemsPerResponse = uintPtr(0) },
			"default_items_per_response",
		},
		{
			"zero term_budget",
			func(c *config.Config) { c.Service.Methods.GetEvents.TermBudget = uintPtr(0) },
			"[service.methods.getEvents].term_budget",
		},
		{
			"fee window above the cap",
			func(c *config.Config) { c.Service.FeeStats.ClassicFeeWindowLedgers = uint32Ptr(1001) },
			"classic_fee_window_ledgers",
		},
		{
			"zero fee window",
			func(c *config.Config) { c.Service.FeeStats.SorobanInclusionFeeWindowLedgers = uint32Ptr(0) },
			"soroban_inclusion_fee_window_ledgers",
		},
		{
			"zero sendTransaction queue_limit",
			func(c *config.Config) { c.Service.Methods.SendTransaction.QueueLimit = uintPtr(0) },
			"[service.methods.sendTransaction].queue_limit",
		},
		{
			"sub-millisecond simulateTransaction duration",
			func(c *config.Config) { c.Service.Methods.SimulateTransaction.MaxExecutionDuration = durPtr(15) },
			wantNanosecondHint,
		},
		{
			"zero getLedgerEntries queue_limit",
			func(c *config.Config) { c.Service.Methods.GetLedgerEntries.QueueLimit = uintPtr(0) },
			"[service.methods.getLedgerEntries].queue_limit",
		},
		{
			"zero getEventsV2 queue_limit",
			func(c *config.Config) { c.Service.Methods.GetEventsV2.QueueLimit = uintPtr(0) },
			"[service.methods.getEventsV2].queue_limit",
		},
		{
			"zero getEventsV2 max_items_per_response",
			func(c *config.Config) { c.Service.Methods.GetEventsV2.MaxItemsPerResponse = uintPtr(0) },
			"[service.methods.getEventsV2].max_items_per_response",
		},
		{
			"zero preflight worker_count",
			func(c *config.Config) { c.Service.Preflight.WorkerCount = uintPtr(0) },
			"[service.preflight].worker_count",
		},
		{
			"zero preflight worker_queue_size",
			func(c *config.Config) { c.Service.Preflight.WorkerQueueSize = uintPtr(0) },
			"[service.preflight].worker_queue_size",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cat, _ := testCatalog(t)
			cfg := validCfg(4, 3, "genesis")
			tc.mutate(&cfg)
			_, err := callValidate(t, cfg, cat, downTip())
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestValidateConfig_RejectsMalformedBSB(t *testing.T) {
	uint32Ptr := func(v uint32) *uint32 { return &v }
	int64Ptr := func(v int64) *int64 { return &v }
	durPtr := func(v time.Duration) *time.Duration { return &v }

	tests := []struct {
		name   string
		mutate func(*config.Config)
		want   string
	}{
		{
			"zero buffer_size",
			func(c *config.Config) { c.Backfill.BSB.BufferSize = uint32Ptr(0) },
			"[backfill.bsb].buffer_size",
		},
		{
			"zero num_workers",
			func(c *config.Config) { c.Backfill.BSB.NumWorkers = uint32Ptr(0) },
			"[backfill.bsb].num_workers",
		},
		{
			// The nanosecond trap again: retry_wait = 10 decodes as 10ns.
			"sub-millisecond retry_wait",
			func(c *config.Config) { c.Backfill.BSB.RetryWait = durPtr(10) },
			wantNanosecondHint,
		},
		{
			"num_workers above buffer_size",
			func(c *config.Config) {
				c.Backfill.BSB.BufferSize = uint32Ptr(10)
				c.Backfill.BSB.NumWorkers = uint32Ptr(50)
			},
			"num_workers",
		},
		{
			"negative buffer_bytes",
			func(c *config.Config) { c.Backfill.BSB.BufferBytes = int64Ptr(-1) },
			"[backfill.bsb].buffer_bytes",
		},
		{
			// There is no off switch; zero would silently mean "cap off" at the
			// SDK, which is the configuration that caused #895.
			"zero buffer_bytes",
			func(c *config.Config) { c.Backfill.BSB.BufferBytes = int64Ptr(0) },
			"[backfill.bsb].buffer_bytes",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cat, _ := testCatalog(t)
			cfg := validCfg(4, 3, "genesis")
			tc.mutate(&cfg)
			_, err := callValidate(t, cfg, cat, downTip())
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestValidateConfig_RejectsMalformedCoreHTTP(t *testing.T) {
	uintPtr := func(v uint) *uint { return &v }
	durPtr := func(v time.Duration) *time.Duration { return &v }

	tests := []struct {
		name   string
		mutate func(*config.Config)
		want   string
	}{
		{
			// v1 read 0 as "don't run core's HTTP server".
			"zero core_http_port",
			func(c *config.Config) { c.Ingestion.CoreHTTPPort = uintPtr(0) },
			"[ingestion]." + keyCoreHTTPPort,
		},
		{
			// The captive-core toml carries these as uint16.
			"core_http_query_port above 65535",
			func(c *config.Config) { c.Ingestion.CoreHTTPQueryPort = uintPtr(70000) },
			"[ingestion]." + keyCoreHTTPQueryPort,
		},
		{
			"thread pool above 65535",
			func(c *config.Config) { c.Ingestion.CoreHTTPQueryThreadPoolSize = uintPtr(65536) },
			keyCoreQueryThreadPoolSize,
		},
		{
			"zero snapshot ledgers",
			func(c *config.Config) { c.Ingestion.CoreHTTPQuerySnapshotLedgers = uintPtr(0) },
			keyCoreQuerySnapshotLedgers,
		},
		{
			"both servers on one port",
			func(c *config.Config) {
				c.Ingestion.CoreHTTPPort = uintPtr(11626)
				c.Ingestion.CoreHTTPQueryPort = uintPtr(11626)
			},
			"cannot bind one port twice",
		},
		{
			// The nanosecond trap once more: core_request_timeout = 2 is 2ns.
			"sub-millisecond core_request_timeout",
			func(c *config.Config) { c.Ingestion.CoreRequestTimeout = durPtr(2) },
			wantNanosecondHint,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cat, _ := testCatalog(t)
			cfg := validCfg(4, 3, "genesis")
			tc.mutate(&cfg)
			_, err := callValidate(t, cfg, cat, downTip())
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

// A bad core_url otherwise fails per request, not at startup.
func TestValidateConfig_RejectsMisroutableCoreURLs(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want string
	}{
		{
			"without a scheme",
			"core.internal:11626",
			"must start with http:// or https://",
		},
		{
			"no host",
			"http://",
			"names no host",
		},
		{
			"not a URL at all",
			"http://[::1",
			"core_url",
		},
		{
			"this host but not core_http_port",
			"http://localhost:21626",
			"points at this host but not at " + keyCoreHTTPPort,
		},
		{
			"a loopback address but not core_http_port",
			"http://127.0.0.1:8080",
			"points at this host but not at " + keyCoreHTTPPort,
		},
		{
			"this host with no port",
			"http://localhost",
			"points at this host but not at " + keyCoreHTTPPort,
		},
		{
			"this host in uppercase but not core_http_port",
			"http://LOCALHOST:21626",
			"points at this host but not at " + keyCoreHTTPPort,
		},
		{
			"this host with a trailing dot but not core_http_port",
			"http://localhost.:21626",
			"points at this host but not at " + keyCoreHTTPPort,
		},
		{
			// The local captive core serves plain HTTP, so https can never work.
			"https to this host",
			"https://localhost:11626",
			"TLS handshake",
		},
		{
			"a port past 65535",
			"http://core.internal:70000",
			"ports run 1 to",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cat, _ := testCatalog(t)
			cfg := validCfg(4, 3, "genesis")
			cfg.Ingestion.CoreURL = tc.url
			_, err := callValidate(t, cfg, cat, downTip())
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestValidateConfig_AcceptsCoreURLsThatCannotMisroute(t *testing.T) {
	corePort := strconv.FormatUint(uint64(config.DefaultCoreHTTPPort), 10)
	urls := []string{
		"http://core.internal:8080",
		"https://core.internal",
		"http://localhost:" + corePort,
		"http://LOCALHOST:" + corePort,
		"http://localhost.:" + corePort,
		"http://127.0.0.1:" + corePort,
		"http://[::1]:" + corePort,
	}
	for _, u := range urls {
		t.Run(u, func(t *testing.T) {
			cat, _ := testCatalog(t)
			cfg := validCfg(4, 3, "genesis")
			cfg.Ingestion.CoreURL = u
			_, err := callValidate(t, cfg, cat, downTip())
			require.NoError(t, err)
		})
	}
}

func TestValidateConfig_AcceptsZeroBSBMaxRetries(t *testing.T) {
	uint32Ptr := func(v uint32) *uint32 { return &v }
	cat, _ := testCatalog(t)
	cfg := validCfg(4, 3, "genesis")
	cfg.Backfill.BSB.MaxRetries = uint32Ptr(0)
	_, err := callValidate(t, cfg, cat, downTip())
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// First start pins earliest_ledger.
// ---------------------------------------------------------------------------

func TestValidateConfig_FirstStartPinsEarliest(t *testing.T) {
	cat, _ := testCatalog(t)
	// Before: not pinned.
	_, ok, _ := cat.EarliestLedger()
	require.False(t, ok)

	_, err := callValidate(t, validCfg(4, 3, "genesis"), cat, downTip())
	require.NoError(t, err)

	// After: present.
	el, ok, _ := cat.EarliestLedger()
	require.True(t, ok)
	assert.Equal(t, uint32(chunk.FirstLedgerSeq), el)
}

// ---------------------------------------------------------------------------
// First start with "now" / numeric requires a reachable, ready tip.
// ---------------------------------------------------------------------------

func TestValidateConfig_NowFirstStartNeedsTip(t *testing.T) {
	cat, _ := testCatalog(t)
	_, err := callValidate(t, validCfg(4, 3, "now"), cat, downTip())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "now")
	_, ok, _ := cat.EarliestLedger()
	assert.False(t, ok, "nothing pinned when the tip is unavailable")
}

func TestValidateConfig_NumericFirstStartNeedsTip(t *testing.T) {
	cat, _ := testCatalog(t)
	floor := chunk.ID(3).FirstLedger()
	_, err := callValidate(t, validCfg(4, 3, itoa(floor)), cat, downTip())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "network tip")
}

func TestValidateConfig_NumericFloorPastTipRejected(t *testing.T) {
	cat, _ := testCatalog(t)
	floor := chunk.ID(100).FirstLedger()       // way ahead
	tipLedger := chunk.ID(5).FirstLedger() + 1 // tip far below the floor
	_, err := callValidate(t, validCfg(4, 3, itoa(floor)), cat, readyTip(tipLedger))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "past the current network tip")
	_, ok, _ := cat.EarliestLedger()
	assert.False(t, ok, "a future floor is never pinned")
}

func TestValidateConfig_SubGenesisTipRejectedAsNotReady(t *testing.T) {
	cat, _ := testCatalog(t)
	_, err := callValidate(t, validCfg(4, 3, "now"), cat, readyTip(chunk.FirstLedgerSeq-1))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "now")
}

// ---------------------------------------------------------------------------
// Restart immutability (earliest_ledger).
// ---------------------------------------------------------------------------

func TestValidateConfig_RestartAcceptsUnchanged(t *testing.T) {
	cat, _ := testCatalog(t)
	// First start pins earliest=genesis.
	_, err := callValidate(t, validCfg(4, 3, "genesis"), cat, downTip())
	require.NoError(t, err)
	requireEarliestPin(t, cat, uint32(chunk.FirstLedgerSeq))

	// Restart with the identical earliest: no error.
	earliest, err := callValidate(t, validCfg(8, 1, "genesis"), cat, downTip())
	require.NoError(t, err)
	assert.Equal(t, uint32(chunk.FirstLedgerSeq), earliest)

	// A successful restart mutates nothing.
	requireEarliestPin(t, cat, uint32(chunk.FirstLedgerSeq))
}

func TestValidateConfig_RestartAbortsOnChangedEarliest(t *testing.T) {
	cat, _ := testCatalog(t)
	// First start pins a numeric floor.
	floor := chunk.ID(3).FirstLedger()
	_, err := callValidate(t, validCfg(4, 3, itoa(floor)), cat, readyTip(chunk.ID(50).FirstLedger()))
	require.NoError(t, err)
	requireEarliestPin(t, cat, floor)

	// Restart with a different numeric floor aborts.
	other := chunk.ID(7).FirstLedger()
	_, err = callValidate(t, validCfg(4, 3, itoa(other)), cat, readyTip(chunk.ID(50).FirstLedger()))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "earliest_ledger changed")

	// The aborted restart left the original pin untouched.
	requireEarliestPin(t, cat, floor)
}

func TestValidateConfig_RestartGenesisVsNumericAborts(t *testing.T) {
	cat, _ := testCatalog(t)
	// First start: genesis (earliest pinned = 2).
	_, err := callValidate(t, validCfg(4, 3, "genesis"), cat, downTip())
	require.NoError(t, err)
	requireEarliestPin(t, cat, uint32(chunk.FirstLedgerSeq))

	// Restart edited to a numeric floor != genesis: abort.
	_, err = callValidate(t, validCfg(4, 3, itoa(chunk.ID(3).FirstLedger())), cat,
		readyTip(chunk.ID(50).FirstLedger()))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "earliest_ledger changed")

	// The aborted restart left the genesis pin untouched.
	requireEarliestPin(t, cat, uint32(chunk.FirstLedgerSeq))
}

// "now" on restart is a deliberate no-op: it keeps the pinned floor and never
// aborts, even when a backend would resolve it differently.
func TestValidateConfig_RestartNowIsNoOp(t *testing.T) {
	cat, _ := testCatalog(t)
	// First start: "now" resolves against a tip in chunk 5 -> pin 50002.
	_, err := callValidate(t, validCfg(4, 3, "now"), cat, readyTip(chunk.ID(5).FirstLedger()+10))
	require.NoError(t, err)
	requireEarliestPin(t, cat, chunk.ID(5).FirstLedger())

	// Restart with "now" and a down backend: original pin kept, no re-resolve.
	earliest, err := callValidate(t, validCfg(4, 3, "now"), cat, downTip())
	require.NoError(t, err)
	assert.Equal(t, chunk.ID(5).FirstLedger(), earliest, "restart with now keeps the original pin")

	// A "now" restart mutates nothing.
	requireEarliestPin(t, cat, chunk.ID(5).FirstLedger())
}

// itoa is the test-local uint32 -> decimal-string helper.
func itoa(n uint32) string { return strconv.FormatUint(uint64(n), 10) }
