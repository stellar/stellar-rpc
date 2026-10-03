package bench

import (
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/network"
)

func validQueryFlags() queryFlags {
	return queryFlags{
		types:            queryTypeLedgers,
		targetRPS:        "1",
		duration:         time.Second,
		ledgersSpan:      defaultLedgersSpan,
		txPageSpan:       defaultTxPageSpan,
		txPageLimit:      defaultTxPageLimit,
		eventsLimit:      defaultEventsLimit,
		notFoundFraction: defaultNotFoundFraction,
		txHashPoolSize:   defaultTxHashPoolSize,
		passphrase:       network.PublicNetworkPassphrase,
		seed:             defaultSeed,
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

func TestQueryPlanPoolAndNotFoundFraction(t *testing.T) {
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
}

func TestQueryCacheControls(t *testing.T) {
	for _, cmd := range NewQueryCommand().Commands() {
		t.Run(cmd.Name(), func(t *testing.T) {
			warmup := cmd.Flags().Lookup("warmup")
			require.NotNil(t, warmup)
			want := "0"
			if cmd.Name() == "hot" {
				want = "20"
				assert.Nil(t, cmd.Flags().Lookup("evict-page-cache"))
			} else {
				eviction := cmd.Flags().Lookup("evict-page-cache")
				require.NotNil(t, eviction)
				assert.Equal(t, "true", eviction.DefValue)
			}
			assert.Equal(t, want, warmup.DefValue)
		})
	}
	for _, tc := range []struct {
		warmup int
		evict  bool
		want   string
	}{
		{0, false, "existing-cache"},
		{0, true, "cold-start"},
		{20, false, "warm-run"},
		{20, true, "warm-run"},
	} {
		p := queryPlan{Warmup: tc.warmup, Evict: tc.evict}
		assert.Equal(t, tc.want, p.cacheScenario())
	}
	assert.Equal(t, "off", evictionState(false))
	want := "unsupported-on-this-platform"
	if evictSupported {
		want = "requested"
	}
	assert.Equal(t, want, evictionState(true))
}

// TestPlanBoundsReadSpans: --ledgers-span and --txpage-span accept maxReadSpan
// and reject maxReadSpan+1.
func TestPlanBoundsReadSpans(t *testing.T) {
	f := validQueryFlags()
	f.ledgersSpan = maxReadSpan
	f.txPageSpan = maxReadSpan
	_, err := f.plan()
	require.NoError(t, err)

	f = validQueryFlags()
	f.ledgersSpan = maxReadSpan + 1
	_, err = f.plan()
	require.ErrorContains(t, err, "--ledgers-span")

	f = validQueryFlags()
	f.txPageSpan = maxReadSpan + 1
	_, err = f.plan()
	require.ErrorContains(t, err, "--txpage-span")
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

func TestColdQueryOptionsValidate(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts coldQueryOptions
		want string
	}{
		{"range ends below maxChunkID", coldQueryOptions{ColdRoot: "x", StartChunk: maxChunkID - 1, NumChunks: 1}, ""},
		{
			"range ends at maxChunkID",
			coldQueryOptions{ColdRoot: "x", StartChunk: maxChunkID, NumChunks: 1},
			"at or past the last valid chunk ID",
		},
		{
			"range end past uint32",
			coldQueryOptions{ColdRoot: "x", StartChunk: maxChunkID - 1, NumChunks: math.MaxUint32},
			"at or past the last valid chunk ID",
		},
		{"no --cold-dir", coldQueryOptions{NumChunks: 1}, "--cold-dir is required"},
		{"no chunk", coldQueryOptions{ColdRoot: "x"}, "--num-chunks must be >= 1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.opts.validate()
			if tc.want == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestHotQueryOptionsValidate(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts hotQueryOptions
		want string
	}{
		{"chunk at maxChunkID", hotQueryOptions{HotRoot: "x", Chunk: maxChunkID}, ""},
		{"chunk past maxChunkID", hotQueryOptions{HotRoot: "x", Chunk: maxChunkID + 1}, "past the last valid chunk ID"},
		{"no --hot-dir", hotQueryOptions{}, "--hot-dir is required"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.opts.validate()
			if tc.want == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.want)
		})
	}
}
