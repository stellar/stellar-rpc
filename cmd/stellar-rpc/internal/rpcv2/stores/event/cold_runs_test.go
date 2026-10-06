package event

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// Three spilled slabs: a term on every event, terms shared by some slabs,
// and one unique to each event. The merge yields each term once, in key
// order, with the union of its slabs.
func TestColdRuns_MergeUnitesEachTermAcrossSlabs(t *testing.T) {
	rng := rand.New(rand.NewSource(5))
	runs := coldRuns{dir: filepath.Join(t.TempDir(), "runs")}
	want := NewBitmaps()
	every := randomTermKey(rng)
	shared := []TermKey{randomTermKey(rng), randomTermKey(rng), randomTermKey(rng)}
	s := newHotSlab(0)
	for slab := range uint32(3) {
		s.reset(slab)
		for i := range uint32(5000) {
			id := slab<<indexSlabShift | i*13
			keys := []TermKey{every, shared[(slab+i)%3], randomTermKey(rng)}
			for _, k := range keys {
				s.add(k, id)
				want.AddTo(k, id)
			}
		}
		require.NoError(t, runs.spill(s))
		if slab == 1 {
			require.NoError(t, runs.spill(newHotSlab(9)), "an empty run")
		}
	}
	require.Len(t, runs.paths, 4)

	var prev TermKey
	terms := 0
	for term, err := range runs.terms() {
		require.NoError(t, err)
		if terms > 0 {
			require.Negative(t, bytes.Compare(prev[:], term.key[:]), "keys ascend")
		}
		require.True(t, want[term.key].Equals(term.bitmap))
		prev = term.key
		terms++
	}
	require.Equal(t, len(want), terms)
	keys := 0
	for key, err := range runs.keys() {
		require.NoError(t, err)
		require.NotNil(t, want[key])
		keys++
	}
	require.Equal(t, len(want), keys)

	require.NoError(t, runs.remove())
	require.NoDirExists(t, runs.dir)
}

// A run cut anywhere but at an entry boundary is an error, in both passes.
func TestColdRuns_ReportTruncation(t *testing.T) {
	runs := coldRuns{dir: filepath.Join(t.TempDir(), "runs")}
	s := newHotSlab(0)
	s.add(TermKey{1}, 1)
	s.add(TermKey{2}, 2)
	require.NoError(t, runs.spill(s))
	data, err := os.ReadFile(runs.paths[0])
	require.NoError(t, err)
	boundary := len(data) / 2 // two entries of the same size

	for cut := len(data) - 1; cut > 0; cut-- {
		require.NoError(t, os.WriteFile(runs.paths[0], data[:cut], 0o600))
		n, err := runs.count()
		terms := 0
		var terr error
		for _, e := range runs.terms() {
			if e != nil {
				terr = e
				break
			}
			terms++
		}
		if cut == boundary {
			require.NoError(t, err)
			require.NoError(t, terr)
			require.Equal(t, uint64(1), n)
			require.Equal(t, 1, terms)
			continue
		}
		require.ErrorContains(t, err, "unexpected EOF", "cut at %d", cut)
		require.ErrorContains(t, terr, "unexpected EOF", "cut at %d", cut)
	}
}

func TestColdRuns_RejectOversizedEntries(t *testing.T) {
	runs := coldRuns{dir: filepath.Join(t.TempDir(), "runs")}
	require.NoError(t, os.MkdirAll(runs.dir, 0o755))
	path := filepath.Join(runs.dir, "00000")
	entry := binary.AppendUvarint(make([]byte, len(TermKey{})), runValueMax+1)
	require.NoError(t, os.WriteFile(path, entry, 0o600))
	runs.paths = []string{path}
	_, err := runs.count()
	require.ErrorContains(t, err, "entry of")
}
