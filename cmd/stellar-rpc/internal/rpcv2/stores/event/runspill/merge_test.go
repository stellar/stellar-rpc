package runspill

import (
	"bytes"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// spillRun writes one run from (term -> ids) pairs appended in map order.
func spillRun(t *testing.T, dir, name string, recs map[[16]byte][]uint32) string {
	t.Helper()
	slab := NewSlab(4 << 20)
	for k, ids := range recs {
		for _, id := range ids {
			require.True(t, slab.Append(k, id))
		}
	}
	path := filepath.Join(dir, name)
	spillSlab(t, slab, path)
	return path
}

func collectMerge(t *testing.T, paths []string) map[[16]byte][]uint32 {
	t.Helper()
	out := map[[16]byte][]uint32{}
	var order [][16]byte
	require.NoError(t, MergeRuns(paths, func(term [16]byte, ids []uint32) error {
		_, dup := out[term]
		require.False(t, dup, "term emitted twice")
		out[term] = append([]uint32(nil), ids...)
		order = append(order, term)
		return nil
	}))
	for i := 1; i < len(order); i++ {
		require.Equal(t, -1, cmpTermCompat(order[i-1], order[i]), "terms must emit ascending")
	}
	return out
}

// TestMergeRuns_UnionsAcrossRuns: overlapping and disjoint terms across three
// runs, including a term present in all runs with interleaved and duplicate
// IDs — the union must be ascending and deduped.
func TestMergeRuns_UnionsAcrossRuns(t *testing.T) {
	dir := t.TempDir()
	shared := key(1)
	paths := []string{
		spillRun(t, dir, "a.run", map[[16]byte][]uint32{
			shared: {0, 3, 6}, key(2): {10, 11},
		}),
		spillRun(t, dir, "b.run", map[[16]byte][]uint32{
			shared: {1, 3, 7}, key(3): {20},
		}),
		spillRun(t, dir, "c.run", map[[16]byte][]uint32{
			shared: {2, 8}, key(2): {12},
		}),
	}
	got := collectMerge(t, paths)
	assert.Equal(t, map[[16]byte][]uint32{
		shared: {0, 1, 2, 3, 6, 7, 8},
		key(2): {10, 11, 12},
		key(3): {20},
	}, got)
}

// TestMergeRuns_MatchesReference: randomized many-run property test against
// a reference union.
func TestMergeRuns_MatchesReference(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	dir := t.TempDir()
	want := map[[16]byte][]uint32{}
	paths := make([]string, 0, 12)
	nextID := uint32(0)
	for run := range 12 {
		recs := map[[16]byte][]uint32{}
		for range 500 {
			k := key(byte(rng.Intn(60)))
			id := nextID
			nextID += uint32(rng.Intn(2)) // duplicates across appends allowed
			recs[k] = append(recs[k], id)
		}
		paths = append(paths, spillRun(t, dir, filepath.Base(t.Name())+string(rune('a'+run))+".run", recs))
		for k, ids := range recs {
			want[k] = unionAscending(want[k], dedupAscending(ids))
		}
	}
	assert.Equal(t, want, collectMerge(t, paths))
}

// dedupAscending mirrors the slab's dedup for the reference model (input is
// ascending with possible repeats).
func dedupAscending(ids []uint32) []uint32 {
	out := ids[:0:0]
	for _, id := range ids {
		if n := len(out); n == 0 || out[n-1] != id {
			out = append(out, id)
		}
	}
	return out
}

// TestMergeRuns_SingleRunPassThrough: one run streams through unchanged.
func TestMergeRuns_SingleRunPassThrough(t *testing.T) {
	dir := t.TempDir()
	recs := map[[16]byte][]uint32{key(5): {1, 2, 3}, key(9): {7}}
	got := collectMerge(t, []string{spillRun(t, dir, "one.run", recs)})
	assert.Equal(t, recs, got)
}

// TestMergeRuns_EmptyInput: no runs, no emits, no error.
func TestMergeRuns_EmptyInput(t *testing.T) {
	require.NoError(t, MergeRuns(nil, func([16]byte, []uint32) error {
		t.Fatal("emit must not be called")
		return nil
	}))
}

func TestUnionAscending(t *testing.T) {
	assert.Equal(t, []uint32{1, 2, 3}, unionAscending(nil, []uint32{1, 2, 3}))
	assert.Equal(t, []uint32{1, 2, 3, 4}, unionAscending([]uint32{1, 2}, []uint32{3, 4}))
	assert.Equal(t, []uint32{1, 2, 3, 5}, unionAscending([]uint32{1, 3}, []uint32{2, 3, 5}))
	assert.Equal(t, []uint32{1, 2}, unionAscending([]uint32{1, 2}, nil))
}

func cmpTermCompat(a, b [16]byte) int { return bytes.Compare(a[:], b[:]) }
