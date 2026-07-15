package latencytrack

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBucketGrid_SpansTenMicrosToTenMinutes(t *testing.T) {
	assert.Equal(t, (10 * time.Microsecond).Nanoseconds(), bucketBounds[0])
	for i := 1; i < numBuckets; i++ {
		require.Greater(t, bucketBounds[i], bucketBounds[i-1], "bounds must be strictly increasing")
	}
	top := time.Duration(bucketBounds[numBuckets-1])
	assert.Greater(t, top, 10*time.Minute)
	assert.Less(t, top, 15*time.Minute)
}

func TestTracker_KnownDistribution(t *testing.T) {
	tr := &Tracker{}
	for ms := 1; ms <= 1000; ms++ {
		tr.Record(time.Duration(ms) * time.Millisecond)
	}
	s := tr.Snapshot()

	assert.Equal(t, uint64(1000), s.Count)
	assert.Equal(t, 1000*time.Millisecond, s.Max, "max is exact")
	assert.Equal(t, 500500*time.Microsecond, s.Avg, "avg is exact (sum/count)")

	assert.InEpsilon(t, float64(500*time.Millisecond), float64(s.P50), 0.05)
	assert.InEpsilon(t, float64(750*time.Millisecond), float64(s.P75), 0.05)
	assert.InEpsilon(t, float64(900*time.Millisecond), float64(s.P90), 0.05)
	assert.InEpsilon(t, float64(990*time.Millisecond), float64(s.P99), 0.05)
}

func TestTracker_OverflowClampsButMaxExact(t *testing.T) {
	tr := &Tracker{}
	tr.Record(20 * time.Minute) // past the top bucket bound
	s := tr.Snapshot()

	assert.Equal(t, uint64(1), s.Count)
	assert.Equal(t, 20*time.Minute, s.Max, "max is exact even for clamped overflows")
	assert.LessOrEqual(t, s.P99, s.Max)
	assert.Greater(t, s.P99, 10*time.Minute)
}

func TestTracker_EmptyAndNil(t *testing.T) {
	assert.Equal(t, Stats{}, (&Tracker{}).Snapshot())

	var nilTracker *Tracker
	nilTracker.Record(time.Second) // must not panic
	assert.Equal(t, Stats{}, nilTracker.Snapshot())
}

func TestTracker_NegativeCountsAsZero(t *testing.T) {
	tr := &Tracker{}
	tr.Record(-time.Second)
	s := tr.Snapshot()
	assert.Equal(t, uint64(1), s.Count)
	assert.Equal(t, time.Duration(0), s.Max)
}

func TestTracker_ConcurrentRecords(t *testing.T) {
	tr := &Tracker{}
	const goroutines, perG = 8, 10_000
	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perG {
				tr.Record(time.Millisecond)
			}
		}()
	}
	wg.Wait()
	s := tr.Snapshot()
	assert.Equal(t, uint64(goroutines*perG), s.Count)
	assert.Equal(t, time.Millisecond, s.Max)
	assert.Equal(t, time.Millisecond, s.Avg)
}

func TestSet_TrackerIdentityAndSnapshotAll(t *testing.T) {
	set := &Set{}
	a := set.Tracker("ingest.read")
	require.Same(t, a, set.Tracker("ingest.read"), "same name resolves to the same tracker")

	a.Record(2 * time.Second)
	set.Tracker("ingest.write").Record(3 * time.Second)

	all := set.SnapshotAll()
	require.Len(t, all, 2)
	assert.Equal(t, uint64(1), all["ingest.read"].Count)
	assert.Equal(t, 2*time.Second, all["ingest.read"].Max)
	assert.Equal(t, 3*time.Second, all["ingest.write"].Max)
}

func TestSet_NilIsInert(t *testing.T) {
	var set *Set
	set.Tracker("x").Record(time.Second) // must not panic
	assert.Nil(t, set.SnapshotAll())
}

func TestStats_MarshalJSONSecondsFloat(t *testing.T) {
	set := &Set{}
	set.Tracker("ingest.e2e").Record(1500 * time.Millisecond)

	raw, err := json.Marshal(set.SnapshotAll())
	require.NoError(t, err)

	var decoded map[string]struct {
		Count uint64  `json:"count"`
		Avg   float64 `json:"avg"`
		Max   float64 `json:"max"`
		P50   float64 `json:"p50"`
		P99   float64 `json:"p99"`
	}
	require.NoError(t, json.Unmarshal(raw, &decoded))
	e2e, ok := decoded["ingest.e2e"]
	require.True(t, ok)
	assert.Equal(t, uint64(1), e2e.Count)
	assert.InDelta(t, 1.5, e2e.Max, 1e-9, "durations serialize as seconds")
	assert.InDelta(t, 1.5, e2e.Avg, 1e-9)
	assert.InEpsilon(t, 1.5, e2e.P50, 0.13)
}
