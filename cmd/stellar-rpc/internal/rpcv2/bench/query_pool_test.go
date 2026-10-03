package bench

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/network"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
)

// The events scan counts only the events of the dataset's ledger range: a hot
// dataset narrowed by --sample-ledgers leaves out the chunk's later events.
func TestScanEventTermsStaysInRange(t *testing.T) {
	hotRoot := ingestHotChunk(t)
	for _, tc := range []struct {
		sampleLedgers uint32
		want          int
	}{
		{0, 2},
		{eventEvery, 1},
	} {
		t.Run(strconv.Itoa(int(tc.sampleLedgers)), func(t *testing.T) {
			ds, release, err := openHotDataset(testLogger(), hotQueryOptions{
				HotRoot: hotRoot, Chunk: 0, SampleLedgers: tc.sampleLedgers,
				Plan: queryPlan{Types: []string{queryTypeEvents}},
			})
			require.NoError(t, err)
			defer release()

			counts := &eventTermCounts{
				contracts: map[string]int{},
				pairs:     map[string]int{},
				terms:     map[string]eventTermPair{},
			}
			require.NoError(t, counts.scanChunk(context.Background(), ds, 0, eventScanCap))
			assert.Equal(t, tc.want, counts.scanned)
		})
	}
}

// A canceled context stops the txhash pool build.
func TestBuildTxHashPoolStopsOnCancel(t *testing.T) {
	ds, release, err := openHotDataset(testLogger(), hotQueryOptions{
		HotRoot: ingestHotChunk(t), Chunk: 0,
		Plan: queryPlan{Types: []string{queryTypeTxHash}, Passphrase: network.PublicNetworkPassphrase},
	})
	require.NoError(t, err)
	defer release()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = buildTxHashPool(ctx, testLogger(), ds, 0, defaultSeed, defaultTxHashPoolSize)
	require.ErrorIs(t, err, context.Canceled)
}

// A txhash pool build fails under the wrong passphrase.
func TestBuildTxHashPoolWrongPassphrase(t *testing.T) {
	ds, release, err := openHotDataset(testLogger(), hotQueryOptions{
		HotRoot: ingestHotChunk(t), Chunk: 0,
		Plan: queryPlan{Types: []string{queryTypeTxHash}, Passphrase: network.TestNetworkPassphrase},
	})
	require.NoError(t, err)
	defer release()

	_, err = buildTxHashPool(context.Background(), testLogger(), ds, 0, defaultSeed, defaultTxHashPoolSize)
	require.ErrorContains(t, err, "does not pair")
}

// A canceled context stops the events pool build.
func TestBuildEventFilterPoolStopsOnCancel(t *testing.T) {
	ds, release, err := openHotDataset(testLogger(), hotQueryOptions{
		HotRoot: ingestHotChunk(t), Chunk: 0,
		Plan: queryPlan{Types: []string{queryTypeEvents}},
	})
	require.NoError(t, err)
	defer release()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = buildEventFilterPool(ctx, testLogger(), ds)
	require.ErrorIs(t, err, context.Canceled)
}

// Each chunk's tx-hash stop point is its cumulative share of size, rounded up,
// and the last chunk's is size.
func TestTxHashStopAt(t *testing.T) {
	for _, tc := range []struct {
		size, n int
		want    []int
	}{
		{10, 1, []int{10}},
		{10, 3, []int{4, 7, 10}},
		{2, 4, []int{1, 1, 2, 2}},
	} {
		got := make([]int, tc.n)
		for i := range tc.n {
			got[i] = txHashStopAt(tc.size, i, tc.n)
		}
		assert.Equal(t, tc.want, got, "size %d, n %d", tc.size, tc.n)
	}
}

// The events scan stays within eventScanCap, gives each scanned chunk at least
// minEventScanPerChunk, and strides by the fewest chunks that keep that share,
// so the scan spans the range.
func TestEventScanPlan(t *testing.T) {
	maxFull := eventScanCap / minEventScanPerChunk
	for _, n := range []int{1, maxFull, maxFull + 1, maxFull + 2, 1000} {
		stride, perChunk := eventScanPlan(n)
		require.GreaterOrEqual(t, stride, 1, "n %d", n)
		assert.GreaterOrEqual(t, perChunk, minEventScanPerChunk, "n %d", n)
		scanned := 0
		for i := 0; i < n; i += stride {
			scanned++
		}
		assert.LessOrEqual(t, scanned*perChunk, eventScanCap, "n %d", n)
		assert.Equal(t, (n+maxFull-1)/maxFull, stride, "n %d", n)
	}
	stride, perChunk := eventScanPlan(1)
	assert.Equal(t, 1, stride)
	assert.Equal(t, eventScanCap, perChunk)
}

// The events pool derives the unfiltered set, a contract set and a (contract,
// first topic) set from the stored events, and each set matches the fixture's
// events.
func TestBuildEventFilterPool(t *testing.T) {
	ds, release, err := openHotDataset(testLogger(), hotQueryOptions{
		HotRoot: ingestHotChunk(t), Chunk: 0,
		Plan: queryPlan{Types: []string{queryTypeEvents}},
	})
	require.NoError(t, err)
	defer release()

	pool, err := buildEventFilterPool(context.Background(), testLogger(), ds)
	require.NoError(t, err)
	require.Len(t, pool.sets, 3)
	assert.Equal(t, "derived", pool.kind())
	assert.Nil(t, pool.sets[0])
	require.Len(t, pool.sets[1], 1)
	assert.NotEmpty(t, pool.sets[1][0].ContractID)
	assert.Nil(t, pool.sets[1][0].Topics[0])
	require.Len(t, pool.sets[2], 1)
	assert.Equal(t, pool.sets[1][0].ContractID, pool.sets[2][0].ContractID)
	assert.NotEmpty(t, pool.sets[2][0].Topics[0])

	for i, filters := range pool.sets {
		view, err := ds.view()
		require.NoError(t, err)
		hi := ds.LastLedger
		page, err := view.QueryEvents(context.Background(), query.EventCursor{Scope: query.EventScope{
			MinLedger: ds.FirstLedger, MaxLedger: &hi, Dir: query.Ascending, Filters: filters,
		}}, defaultEventsLimit)
		view.Release()
		require.NoError(t, err)
		assert.Len(t, page.Events, 2, "set %d", i)
	}

	assert.Equal(t, "unfiltered", (&eventFilterPool{sets: pool.sets[:1]}).kind())
}

// byDescendingCount orders keys by count, most frequent first, and ties by key
// bytes.
func TestByDescendingCount(t *testing.T) {
	got := byDescendingCount(map[string]int{"b": 2, "a": 2, "c": 5})
	assert.Equal(t, [][]byte{[]byte("c"), []byte("a"), []byte("b")}, got)
}
