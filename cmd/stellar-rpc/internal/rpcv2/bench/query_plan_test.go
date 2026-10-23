package bench

import (
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/network"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/config"
)

func validQueryFlags() queryFlags {
	return queryFlags{
		types:            queryTypeLedgers,
		targetRPS:        "1",
		duration:         time.Second,
		ledgersLimit:     defaultLedgersLimit,
		txPageLimit:      defaultTxPageLimit,
		eventsLimit:      defaultEventsLimit,
		notFoundFraction: defaultNotFoundFraction,
		txHashPoolSize:   defaultTxHashPoolSize,
		format:           protocol.FormatBase64,
		passphrase:       network.PublicNetworkPassphrase,
		seed:             defaultSeed,
	}
}

// plan bounds --txhash-pool-size, --not-found-fraction and --events-limit,
// the last at the daemon's queryEvents cap.
func TestQueryPlanPoolAndEventsFlags(t *testing.T) {
	for _, size := range []int{-1, 0, 1, defaultTxHashPoolSize, maxTxHashPoolSize, maxTxHashPoolSize + 1} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			f := validQueryFlags()
			f.txHashPoolSize = size
			p, err := f.plan()
			if size < 1 || size > maxTxHashPoolSize {
				require.ErrorContains(t, err, "--txhash-pool-size")
			} else {
				require.NoError(t, err)
				assert.Equal(t, size, p.TxHashPoolSize)
			}
		})
	}
	for _, fraction := range []float64{math.NaN(), math.Inf(1), math.Inf(-1), -0.1, 1.1} {
		f := validQueryFlags()
		f.notFoundFraction = fraction
		_, err := f.plan()
		require.ErrorContains(t, err, "--not-found-fraction")
	}
	for _, fraction := range []float64{0, 1} {
		f := validQueryFlags()
		f.notFoundFraction = fraction
		p, err := f.plan()
		require.NoError(t, err)
		assert.InDelta(t, fraction, p.NotFoundFraction, 0)
	}
	assert.Equal(t, config.DefaultQueryEventsMaxItemsPerResponse, maxEventsLimit())
	for limit, want := range map[uint32]string{
		0: "--events-limit must be in [1, 1000], got 0", 1000: "", 1001: "--events-limit must be in [1, 1000], got 1001",
	} {
		f := validQueryFlags()
		f.eventsLimit = limit
		_, err := f.plan()
		if want == "" {
			require.NoError(t, err)
			continue
		}
		require.ErrorContains(t, err, want)
	}
}

func TestParseTargetRPSRateBounds(t *testing.T) {
	got, err := parseTargetRPS("0.0001, 2," + strconv.Itoa(maxTargetRPS))
	require.NoError(t, err)
	assert.Equal(t, []float64{0.0001, 2, maxTargetRPS}, got)
	for _, bad := range []string{"0", "-1", "NaN", "Inf"} {
		_, err = parseTargetRPS("1," + bad)
		require.ErrorContains(t, err, "--target-rps rates must be positive and finite", bad)
	}
	_, err = parseTargetRPS("1," + strconv.Itoa(maxTargetRPS+1))
	require.ErrorContains(t, err, "--target-rps rates must be <= 100000")
	_, err = parseTargetRPS("1,x")
	require.ErrorContains(t, err, `--target-rps: "x" is not a number`)
	_, err = parseTargetRPS("1,")
	require.ErrorContains(t, err, "empty entry")
}

func TestParseRejectsRepeats(t *testing.T) {
	_, err := parseTargetRPS("1,2,1")
	require.ErrorContains(t, err, "--target-rps repeats 1, which would duplicate its scenario rows")
	_, err = parseQueryTypes("ledgers,txpage,ledgers")
	require.ErrorContains(t, err, `--types repeats "ledgers", which would duplicate its scenario rows`)
	_, err = parseQueryTypes("ledgers,nope")
	require.ErrorContains(t, err, `unknown query type "nope"`)
	got, err := parseQueryTypes("txpage, ledgers")
	require.NoError(t, err)
	assert.Equal(t, []string{queryTypeTxPage, queryTypeLedgers}, got)
}

// --ledgers-limit and --txpage-limit accept the caps of the daemon's default
// config, and both formats pass.
func TestPlanAcceptsLimitCaps(t *testing.T) {
	assert.Equal(t, config.DefaultGetLedgersMaxItemsPerResponse, maxLedgersLimit())
	assert.Equal(t, config.DefaultGetTransactionsMaxItemsPerResponse, maxTxPageLimit())
	for _, format := range queryFormats {
		f := validQueryFlags()
		f.ledgersLimit = uint32(maxLedgersLimit())
		f.txPageLimit = uint32(maxTxPageLimit())
		f.format = format
		p, err := f.plan()
		require.NoError(t, err)
		assert.Equal(t, format, p.Format)
	}
}

// plan rejects a --target-rps, --duration and --warmup combination that no
// scenario can run.
func TestPlanRejectsUnrunnableScenarios(t *testing.T) {
	for _, tc := range []struct {
		name      string
		targetRPS string
		duration  time.Duration
		warmup    int
		want      string
	}{
		{
			"no measured iteration", "10,0.001", time.Minute, 0,
			"--target-rps, --duration and --warmup: scenario at 0.001 rps for 1m0s plans no measured iteration",
		},
		{
			"over the iteration cap", "1,100000", 20 * time.Minute, 0,
			"--target-rps, --duration and --warmup: scenario at 100000 rps for 20m0s plans more than",
		},
		{
			"warmup plus measured over the cap", "1", time.Second, maxIterations,
			"--target-rps, --duration and --warmup: scenario plans more than 100000000 iterations: " +
				"100000000 warmup plus 1 measured",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := validQueryFlags()
			f.targetRPS = tc.targetRPS
			f.duration = tc.duration
			f.warmup = tc.warmup
			_, err := f.plan()
			require.ErrorContains(t, err, tc.want)
		})
	}
}

// --warmup defaults to 0 and --evict-page-cache to true, and cacheScenario
// labels each combination of warmup and eviction.
func TestQueryCacheControls(t *testing.T) {
	cmd := newQueryCommand()
	assert.Equal(t, "0", cmd.Flags().Lookup("warmup").DefValue)
	assert.Equal(t, "true", cmd.Flags().Lookup("evict-page-cache").DefValue)

	f := validQueryFlags()
	f.evict = true
	p, err := f.plan()
	require.NoError(t, err)
	assert.True(t, p.Evict)

	for _, tc := range []struct {
		warmup int
		evict  bool
		want   string
	}{
		{0, false, "existing-cache"},
		{0, true, "cold-start"},
		{1, false, "warm-run"},
		{1, true, "warm-run"},
	} {
		assert.Equal(t, tc.want, cacheScenario(tc.warmup, tc.evict), "warmup %d, evict %v", tc.warmup, tc.evict)
	}
}

// cacheControls runs eviction only when it is requested on a dataset with
// frozen files and the platform supports it, and warns when the platform
// cannot evict or a warmup follows the eviction.
func TestCacheControls(t *testing.T) {
	const (
		unsupported = "cannot evict the page cache"
		refill      = "refills the page cache"
	)
	onLinux := isLinux()
	coldEviction, coldScenario, coldWarn, warmupWarn := evictionUnsupported, "existing-cache", unsupported, unsupported
	if onLinux {
		coldEviction, coldScenario, coldWarn, warmupWarn = evictionRequested, "cold-start", "", refill
	}
	for _, tc := range []struct {
		name     string
		warmup   int
		evict    bool
		paths    []string
		eviction string
		scenario string
		runs     bool
		warn     string
	}{
		{"not requested", 0, false, []string{"a.pack"}, evictionOff, "existing-cache", false, ""},
		{"hot dataset", 0, true, nil, evictionOff, "existing-cache", false, ""},
		{"hot dataset with warmup", 3, true, nil, evictionOff, "warm-run", false, ""},
		{"cold dataset", 0, true, []string{"a.pack"}, coldEviction, coldScenario, onLinux, coldWarn},
		{"cold dataset with warmup", 3, true, []string{"a.pack"}, coldEviction, "warm-run", onLinux, warmupWarn},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logger, output := capturingLogger()
			settings := map[string]string{}
			evict := cacheControls(logger, queryPlan{Warmup: tc.warmup, Evict: tc.evict},
				&queryDataset{EvictPaths: tc.paths}, settings)
			assert.Equal(t, map[string]string{"pageCacheEviction": tc.eviction, "cacheScenario": tc.scenario}, settings)
			assert.Equal(t, tc.runs, evict != nil)
			for _, warning := range []string{unsupported, refill} {
				assert.Equal(t, warning == tc.warn, strings.Contains(output.String(), warning), warning)
			}
		})
	}
}
