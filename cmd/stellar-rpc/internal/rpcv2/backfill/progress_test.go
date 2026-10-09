package backfill

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/observability"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rpcv2test"
)

// gaugeMetrics records the gauges in the order they were set, so a test can
// see a value go backwards.
type gaugeMetrics struct {
	observability.NopMetrics

	mu        sync.Mutex
	planned   []int
	completed []int
	retries   int
}

func (g *gaugeMetrics) BackfillPlanned(n int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.planned = append(g.planned, n)
}

func (g *gaugeMetrics) BackfillCompleted(n int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.completed = append(g.completed, n)
}

func (g *gaugeMetrics) BackfillRetry() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.retries++
}

// snapshot copies the records, so an assertion never reads a slice a worker
// may still be appending to.
func (g *gaugeMetrics) snapshot() ([]int, []int, int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return slices.Clone(g.planned), slices.Clone(g.completed), g.retries
}

func newTestProgress(t *testing.T, m observability.Metrics, chunks int) *progress {
	t.Helper()
	plan := Plan{ChunkBuilds: make([]ChunkBuild, chunks)}
	cat, _ := testCatalog(t)
	cfg := ExecConfig{Logger: rpcv2test.SilentLogger(), Metrics: m, Catalog: cat}
	return newProgress(cfg, plan, time.Now())
}

// chunkEnded must pair with chunkStarted whether the build succeeded or not,
// or in_flight stays inflated for the rest of the pass.
func TestProgress_InFlightPairsOnEveryExit(t *testing.T) {
	p := newTestProgress(t, &gaugeMetrics{}, 2)
	p.chunkStarted()
	p.chunkStarted()
	require.Equal(t, 2, p.inFlight)
	p.chunkEnded() // the failed build
	p.chunkEnded() // the successful one
	assert.Zero(t, p.inFlight)
}

// A steady-state tick resolves an empty plan; publishing 0/0 for one would
// erase the finished backfill's numbers.
func TestProgress_EmptyPlanLeavesTheGaugesAlone(t *testing.T) {
	m := &gaugeMetrics{}
	newTestProgress(t, m, 3)
	planned, completed, _ := m.snapshot()
	assert.Equal(t, []int{3}, planned)
	assert.Equal(t, []int{0}, completed)

	newTestProgress(t, m, 0)
	planned, completed, _ = m.snapshot()
	assert.Equal(t, []int{3}, planned, "a tick with nothing to do must not republish")
	assert.Equal(t, []int{0}, completed)
}

// The gauge is Set under the lock that produced the count, so concurrent
// finishes cannot leave the pass one short.
func TestProgress_CompletedGaugeNeverGoesBackwards(t *testing.T) {
	const n = 200
	m := &gaugeMetrics{}
	p := newTestProgress(t, m, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() { p.chunkFrozen(ChunkBuild{Chunk: chunk.ID(i)}) })
	}
	wg.Wait()
	_, completed, _ := m.snapshot()
	// The construction publishes 0; the n freezes then publish 1..n in order.
	require.Equal(t, 0, completed[0])
	require.Len(t, completed[1:], n)
	for i, got := range completed[1:] {
		require.Equal(t, i+1, got, "the gauge must be set in the order the count was taken")
	}
}

// The retry notification is what makes a quietly-retried task visible.
func TestNotifyRetry_CountsAndWarns(t *testing.T) {
	m := &gaugeMetrics{}
	cfg := ExecConfig{Logger: rpcv2test.SilentLogger(), Metrics: m, MaxRetries: 2, retryBackoff: time.Nanosecond}
	err := withRetries(context.Background(), cfg, cfg.notifyRetry("chunk", "00000007"), func() error {
		return errors.New("always fails")
	})
	require.Error(t, err)
	// backoff notifies before each wait, so one fewer than the attempts.
	_, _, retries := m.snapshot()
	assert.Equal(t, 2, retries, "every retried attempt is counted")
}
