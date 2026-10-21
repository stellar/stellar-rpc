package bench

import (
	"context"
	"io/fs"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// isLinux reports whether the tests run on the one platform that evicts.
func isLinux() bool { return runtime.GOOS == "linux" }

// A file that cannot be opened fails eviction.
func TestEvictColdArtifactsFailsOnMissingFile(t *testing.T) {
	if !evictSupported {
		t.Skip("page-cache eviction is Linux only")
	}
	dir := t.TempDir()
	present := filepath.Join(dir, "present.pack")
	require.NoError(t, os.WriteFile(present, []byte("ledgers"), 0o600))
	missing := filepath.Join(dir, "missing.pack")

	ds := &queryDataset{EvictPaths: []string{missing, present}}
	require.ErrorIs(t, ds.evictColdArtifacts(context.Background()), fs.ErrNotExist)
}

// A cancel before eviction advises no file, and the scenario adds no row and
// returns the context error.
func TestEvictColdArtifactsCanceled(t *testing.T) {
	ds := &queryDataset{EvictPaths: []string{filepath.Join(t.TempDir(), "missing.pack")}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, ds.evictColdArtifacts(ctx), context.Canceled)

	req := func(context.Context, *rand.Rand) (requestTiming, error) {
		t.Error("a canceled scenario sends no request")
		return requestTiming{}, nil
	}
	logger, output := capturingLogger()
	run := testQueryRun(logger, queryPlan{Duration: time.Second}, &fakeScenarioClock{})
	run.evict = ds.evictColdArtifacts
	require.ErrorIs(t, run.scenario(ctx, queryTypeLedgers, 100, req), context.Canceled)
	assert.Empty(t, run.report.scenarios)
	assert.Contains(t, output.String(), "canceled during page-cache eviction")
}
