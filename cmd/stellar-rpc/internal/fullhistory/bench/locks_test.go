package bench

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/ingest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
)

// TestRunColdRefusesLockedOutputRoot: the cold bench takes the daemon's
// exclusive root flocks on EVERY write root under --cold-out-dir, regardless
// of the --types subset — a root a live daemon holds must fail fast with
// ErrRootLocked, never be truncated over. The held root here (events) is one
// the ledgers-only run would not even write.
func TestRunColdRefusesLockedOutputRoot(t *testing.T) {
	outRoot := t.TempDir()
	held, err := config.LockRoots(geometry.NewLayout(outRoot).EventsRoot())
	require.NoError(t, err)
	defer held.Release()

	err = runCold(context.Background(), testLogger(), coldOptions{
		Source:       sourceConfig{Kind: sourcePack, PackDir: t.TempDir()},
		Types:        ingest.Config{Ledgers: true},
		StartChunk:   chunk.ID(0),
		NumChunks:    1,
		ChunkWorkers: 1,
		ArtifactRoot: outRoot,
		OutDir:       t.TempDir(),
	})
	require.ErrorIs(t, err, config.ErrRootLocked)
}

// TestRunHotRefusesLockedHotRoot: the hot bench flocks its layout's hot root —
// the exact path a daemon whose data dir is --hot-dir holds — before creating
// any DB under it.
func TestRunHotRefusesLockedHotRoot(t *testing.T) {
	hotRoot := t.TempDir()
	held, err := config.LockRoots(geometry.NewLayout(hotRoot).HotRoot())
	require.NoError(t, err)
	defer held.Release()

	err = runHot(context.Background(), testLogger(), hotOptions{
		Source:  sourceConfig{Kind: sourcePack, PackDir: t.TempDir()},
		Chunk:   chunk.ID(0),
		HotRoot: hotRoot,
		OutDir:  t.TempDir(),
	})
	require.ErrorIs(t, err, config.ErrRootLocked)
}

// TestBenchReleasesLocksOnError: a run that fails AFTER acquiring the flocks
// (here at source resolution) must leave every root re-lockable — the
// daemon's release-on-every-exit-path semantics.
func TestBenchReleasesLocksOnError(t *testing.T) {
	outRoot := t.TempDir()
	err := runCold(context.Background(), testLogger(), coldOptions{
		Source:       sourceConfig{Kind: "bogus"},
		Types:        ingest.Config{Ledgers: true},
		StartChunk:   chunk.ID(0),
		NumChunks:    1,
		ChunkWorkers: 1,
		ArtifactRoot: outRoot,
		OutDir:       t.TempDir(),
	})
	require.Error(t, err)
	require.NotErrorIs(t, err, config.ErrRootLocked)
	layout := geometry.NewLayout(outRoot)
	relock, err := config.LockRoots(layout.LedgersRoot(), layout.EventsRoot(), layout.TxHashRawRoot())
	require.NoError(t, err)
	relock.Release()

	hotRoot := t.TempDir()
	err = runHot(context.Background(), testLogger(), hotOptions{
		Source:  sourceConfig{Kind: "bogus"},
		Chunk:   chunk.ID(0),
		HotRoot: hotRoot,
		OutDir:  t.TempDir(),
	})
	require.Error(t, err)
	require.NotErrorIs(t, err, config.ErrRootLocked)
	relock, err = config.LockRoots(geometry.NewLayout(hotRoot).HotRoot())
	require.NoError(t, err)
	relock.Release()
}
