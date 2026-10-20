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
			p, err := f.plan(true)
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
		_, err := f.plan(true)
		require.ErrorContains(t, err, "--not-found-fraction")
	}
	for _, fraction := range []float64{0, 1} {
		f := validQueryFlags()
		f.notFoundFraction = fraction
		p, err := f.plan(true)
		require.NoError(t, err)
		assert.InDelta(t, fraction, p.NotFoundFraction, 0)
	}
	assert.Equal(t, config.DefaultQueryEventsMaxItemsPerResponse, maxEventsLimit())
	for limit, want := range map[uint32]string{
		0: "--events-limit must be in [1, 1000], got 0", 1000: "", 1001: "--events-limit must be in [1, 1000], got 1001",
	} {
		f := validQueryFlags()
		f.eventsLimit = limit
		_, err := f.plan(true)
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
		p, err := f.plan(true)
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
			_, err := f.plan(true)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

// Without --warmup, plan checks each rate against the hot warmup default, so a
// scenario that only a cold dataset can run fails before the dataset opens.
func TestPlanChecksHotWarmupDefault(t *testing.T) {
	for _, tc := range []struct {
		measured    int
		warmupGiven bool
		want        string
	}{
		{maxIterations - defaultHotWarmup, false, ""},
		{
			maxIterations - defaultHotWarmup + 1, false,
			"--target-rps, --duration and the hot --warmup default: scenario plans more than 100000000 " +
				"iterations: 20 warmup plus 99999981 measured",
		},
		{maxIterations - defaultHotWarmup + 1, true, ""},
		{maxIterations, true, ""},
	} {
		f := validQueryFlags()
		f.duration = time.Duration(tc.measured) * time.Second
		_, err := f.plan(tc.warmupGiven)
		if tc.want == "" {
			require.NoError(t, err, "%d measured, --warmup given: %v", tc.measured, tc.warmupGiven)
		} else {
			require.EqualError(t, err, tc.want)
		}
	}
}

// --warmup defaults to 0 and --evict-page-cache to true. Without --warmup a hot
// dataset warms up defaultHotWarmup iterations and a cold one none; a given
// --warmup, or a plan built without the flags, keeps its warmup on both tiers.
func TestQueryCacheControls(t *testing.T) {
	cmd := newQueryCommand()
	assert.Equal(t, "0", cmd.Flags().Lookup("warmup").DefValue)
	assert.Equal(t, "true", cmd.Flags().Lookup("evict-page-cache").DefValue)

	f := validQueryFlags()
	f.evict = true
	byTier, err := f.plan(false)
	require.NoError(t, err)
	assert.True(t, byTier.Evict)
	assert.Equal(t, 0, byTier.forTier(tierCold).Warmup)
	assert.Equal(t, defaultHotWarmup, byTier.forTier(tierHot).Warmup)

	f.warmup = 3
	given, err := f.plan(true)
	require.NoError(t, err)
	assert.Equal(t, 3, given.forTier(tierCold).Warmup)
	assert.Equal(t, 3, given.forTier(tierHot).Warmup)
	built := queryPlan{Warmup: 5}
	assert.Equal(t, 5, built.forTier(tierHot).Warmup)

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
// frozen files and the platform supports it, and warns when it does not.
func TestCacheControls(t *testing.T) {
	onLinux := isLinux()
	coldEviction, coldScenario := "unsupported-on-this-platform", "existing-cache"
	if onLinux {
		coldEviction, coldScenario = "requested", "cold-start"
	}
	for _, tc := range []struct {
		name       string
		warmup     int
		evict      bool
		paths      []string
		eviction   string
		scenario   string
		runs, warn bool
	}{
		{"not requested", 0, false, []string{"a.pack"}, "off", "existing-cache", false, false},
		{"hot dataset", 0, true, nil, "off", "existing-cache", false, false},
		{"cold dataset", 0, true, []string{"a.pack"}, coldEviction, coldScenario, onLinux, !onLinux},
		{"cold dataset with warmup", 3, true, []string{"a.pack"}, coldEviction, "warm-run", onLinux, !onLinux},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logger, output := capturingLogger()
			settings := map[string]string{}
			evict := cacheControls(logger, queryPlan{Warmup: tc.warmup, Evict: tc.evict},
				&queryDataset{EvictPaths: tc.paths}, settings)
			assert.Equal(t, map[string]string{
				"warmup": strconv.Itoa(tc.warmup), "pageCacheEviction": tc.eviction, "cacheScenario": tc.scenario,
			}, settings)
			assert.Equal(t, tc.runs, evict != nil)
			assert.Equal(t, tc.warn, strings.Contains(output.String(), "cannot evict the page cache"))
		})
	}
}
