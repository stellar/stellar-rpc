package event

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/stretchr/testify/require"
)

// Three spilled slabs: a term on every event, terms shared by some slabs,
// and one unique to each event. The merge yields each term once, in key
// order, with its bitmap from each run holding it, in slab order.
func TestColdRuns_MergeYieldsEachTermWithItsSlabs(t *testing.T) {
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
		spillSlab(t, &runs, s)
		if slab == 1 {
			spillSlab(t, &runs, newHotSlab(9)) // an empty run
		}
	}
	require.Len(t, runs.paths, 4)

	var prev TermKey
	terms := 0
	for term, err := range runs.merge() {
		require.NoError(t, err)
		if terms > 0 {
			require.Negative(t, bytes.Compare(prev[:], term.key[:]), "keys ascend")
		}
		got := roaring.New()
		for i, p := range term.parts {
			if i > 0 {
				require.Less(t, term.parts[i-1].slab, p.slab, "slabs ascend")
			}
			part := roaring.New()
			require.NoError(t, part.UnmarshalBinary(p.bitmap))
			require.Equal(t, p.slab, part.Minimum()>>indexSlabShift)
			require.Equal(t, p.slab, part.Maximum()>>indexSlabShift)
			got.Or(part)
		}
		require.True(t, want[term.key].Equals(got))
		prev = term.key
		terms++
	}
	require.Equal(t, len(want), terms)

	require.NoError(t, runs.removeRuns())
	require.DirExists(t, runs.dir)
	require.NoError(t, runs.remove())
	require.NoDirExists(t, runs.dir)
}

// A run cut anywhere but at an entry boundary is an error.
func TestColdRuns_ReportTruncation(t *testing.T) {
	runs := coldRuns{dir: filepath.Join(t.TempDir(), "runs")}
	s := newHotSlab(0)
	s.add(TermKey{1}, 1)
	s.add(TermKey{2}, 2)
	spillSlab(t, &runs, s)
	data, err := os.ReadFile(runs.paths[0])
	require.NoError(t, err)
	boundary := len(data) / 2 // two entries of the same size

	for cut := len(data) - 1; cut > 0; cut-- {
		require.NoError(t, os.WriteFile(runs.paths[0], data[:cut], 0o600))
		terms, err := mergedTerms(&runs)
		if cut == boundary {
			require.NoError(t, err)
			require.Equal(t, 1, terms)
			continue
		}
		require.ErrorContains(t, err, "unexpected EOF", "cut at %d", cut)
	}
}

func TestColdRuns_RejectOversizedEntries(t *testing.T) {
	runs := coldRuns{dir: filepath.Join(t.TempDir(), "runs")}
	require.NoError(t, os.MkdirAll(runs.dir, 0o755))
	path := filepath.Join(runs.dir, "00000")
	entry := binary.AppendUvarint(make([]byte, len(TermKey{})), runValueMax+1)
	require.NoError(t, os.WriteFile(path, entry, 0o600))
	runs.paths, runs.slabs = []string{path}, []uint32{0}
	_, err := mergedTerms(&runs)
	require.ErrorContains(t, err, "entry of")
}

func spillSlab(t *testing.T, runs *coldRuns, s *hotSlab) {
	t.Helper()
	_, err := runs.spill(s.slab, s.terms())
	require.NoError(t, err)
}

// mergedTerms counts the merged terms up to the first error.
func mergedTerms(runs *coldRuns) (int, error) {
	n := 0
	for _, err := range runs.merge() {
		if err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}
