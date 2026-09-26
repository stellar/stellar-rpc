package runspill

import (
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSpiller_EndToEnd drives many records through a tiny slab (forcing many
// rotations and background spills) and checks the merged result against a
// reference union — the full spill→merge pipeline under -race.
func TestSpiller_EndToEnd(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "scratch")
	sp, err := NewSpiller(dir, 64*RecordSize) // tiny: ~forces dozens of spills
	require.NoError(t, err)

	rng := rand.New(rand.NewSource(3))
	want := map[[16]byte][]uint32{}
	id := uint32(0)
	for range 5_000 {
		k := key(byte(rng.Intn(40)))
		require.NoError(t, sp.Add(k, id))
		want[k] = append(want[k], id)
		id += uint32(rng.Intn(2)) + 1
	}
	runs, err := sp.Finish()
	require.NoError(t, err)
	require.Greater(t, len(runs), 10, "tiny slab must have produced many runs")

	got := collectMerge(t, runs)
	assert.Equal(t, want, got)
	require.NoError(t, sp.Cleanup())
}

// TestSpiller_EmptyFinish: no records → no runs, no files.
func TestSpiller_EmptyFinish(t *testing.T) {
	sp, err := NewSpiller(filepath.Join(t.TempDir(), "s"), 1<<16)
	require.NoError(t, err)
	runs, err := sp.Finish()
	require.NoError(t, err)
	assert.Empty(t, runs)
}

// TestSpiller_WipesLeftoverScratch: a prior attempt's files must not survive
// NewSpiller.
func TestSpiller_WipesLeftoverScratch(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s")
	sp1, err := NewSpiller(dir, 1<<16)
	require.NoError(t, err)
	require.NoError(t, sp1.Add(key(1), 1))
	_, err = sp1.Finish()
	require.NoError(t, err)

	sp2, err := NewSpiller(dir, 1<<16)
	require.NoError(t, err)
	runs, err := sp2.Finish()
	require.NoError(t, err)
	assert.Empty(t, runs, "leftover runs from a previous attempt must be wiped")
}

// TestSpiller_SurfacesWriteError: an unwritable scratch dir surfaces at the
// next rotation or Finish, never silently.
func TestSpiller_SurfacesWriteError(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s")
	sp, err := NewSpiller(dir, 2*RecordSize)
	require.NoError(t, err)
	// Yank the scratch dir out from under the spiller.
	require.NoError(t, sp.Cleanup())
	for i := range uint32(100) {
		if err = sp.Add(key(1), i); err != nil {
			break
		}
	}
	if err == nil {
		_, err = sp.Finish()
	}
	require.Error(t, err, "write failure must surface")
}
