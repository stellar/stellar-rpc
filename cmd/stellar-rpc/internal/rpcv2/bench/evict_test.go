package bench

import (
	"context"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A missing artifact is skipped, not an error.
func TestEvictColdArtifactsSkipsMissingFile(t *testing.T) {
	if !evictSupported {
		t.Skip("page-cache eviction is Linux only")
	}
	dir := t.TempDir()
	present := filepath.Join(dir, "present.pack")
	require.NoError(t, os.WriteFile(present, []byte("ledgers"), 0o600))
	missing := filepath.Join(dir, "missing.pack")

	ds := &queryDataset{EvictPaths: []string{missing, present}}
	evicted, err := ds.evictColdArtifacts(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, evicted, "the present file is advised, the missing one skipped")
}

// A cancel before eviction advises no file, and the scenario adds no row and
// returns the context error.
func TestEvictColdArtifactsCanceled(t *testing.T) {
	if !evictSupported {
		t.Skip("page-cache eviction is Linux only")
	}
	present := filepath.Join(t.TempDir(), "present.pack")
	require.NoError(t, os.WriteFile(present, []byte("ledgers"), 0o600))
	ds := &queryDataset{EvictPaths: []string{present}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	evicted, err := ds.evictColdArtifacts(ctx)
	require.ErrorIs(t, err, context.Canceled)
	assert.Zero(t, evicted)

	req := func(context.Context, *rand.Rand) (requestTiming, error) {
		t.Error("a canceled scenario sends no request")
		return requestTiming{}, nil
	}
	logger, output := capturingLogger()
	p := queryPlan{Duration: time.Second, Evict: true}
	run := testQueryRun(logger, ds, p, &fakeScenarioClock{})
	require.ErrorIs(t, run.scenario(ctx, queryTypeLedgers, 100, req), context.Canceled)
	assert.Empty(t, run.report.scenarios)
	assert.Contains(t, output.String(), "canceled during page-cache eviction")
}
