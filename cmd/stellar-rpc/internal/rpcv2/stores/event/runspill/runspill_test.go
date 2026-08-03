package runspill

import (
	"bytes"
	"errors"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func key(b byte) [16]byte {
	var k [16]byte
	k[0] = b
	return k
}

// drain reads a run file fully, asserting clean EOF (which verifies CRC).
func drain(t *testing.T, path string) map[[16]byte][]uint32 {
	t.Helper()
	r, err := OpenRun(path)
	require.NoError(t, err)
	defer r.Close()
	out := map[[16]byte][]uint32{}
	for {
		term, ids, err := r.Next()
		if errors.Is(err, io.EOF) {
			return out
		}
		require.NoError(t, err)
		out[term] = append(out[term], ids...)
	}
}

func TestSlab_SpillRoundTrip(t *testing.T) {
	// Random records across a small term space, appended unsorted, with
	// per-term ascending IDs (the ingest contract).
	rng := rand.New(rand.NewSource(7))
	want := map[[16]byte][]uint32{}
	slab := NewSlab(1 << 20)
	nextID := map[byte]uint32{}
	for range 20_000 {
		b := byte(rng.Intn(50))
		id := nextID[b]
		nextID[b] = id + uint32(rng.Intn(3)) + 1
		require.True(t, slab.Append(key(b), id))
		k := key(b)
		if n := len(want[k]); n == 0 || want[k][n-1] != id {
			want[k] = append(want[k], id)
		}
	}
	path := filepath.Join(t.TempDir(), "00.run")
	require.NoError(t, WriteRun(path, slab.SortEncode(nil)))
	assert.Equal(t, want, drain(t, path))
}

func TestSlab_AppendRejectsWhenFull(t *testing.T) {
	slab := NewSlab(RecordSize * 2)
	require.True(t, slab.Append(key(1), 1))
	require.True(t, slab.Append(key(1), 2))
	require.False(t, slab.Append(key(1), 3), "full slab must reject, not grow")
	assert.Equal(t, 2, slab.Records())
	slab.Reset()
	require.True(t, slab.Append(key(1), 4))
}

func TestSlab_DedupsExactDuplicates(t *testing.T) {
	slab := NewSlab(1 << 12)
	require.True(t, slab.Append(key(9), 5))
	require.True(t, slab.Append(key(9), 5))
	require.True(t, slab.Append(key(9), 6))
	path := filepath.Join(t.TempDir(), "d.run")
	require.NoError(t, WriteRun(path, slab.SortEncode(nil)))
	assert.Equal(t, map[[16]byte][]uint32{key(9): {5, 6}}, drain(t, path))
}

func TestRunReader_DetectsCorruption(t *testing.T) {
	slab := NewSlab(1 << 16)
	for i := range uint32(1000) {
		require.True(t, slab.Append(key(byte(i%7)), i))
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "c.run")
	require.NoError(t, WriteRun(path, slab.SortEncode(nil)))

	raw, err := os.ReadFile(path)
	require.NoError(t, err)

	// Flip one payload byte: reader must fail (structurally or via CRC at
	// drain end), never silently return wrong postings as a clean EOF.
	for _, pos := range []int{20, len(raw) / 2, len(raw) - 6} {
		mut := append([]byte(nil), raw...)
		mut[pos] ^= 0xff
		p := filepath.Join(dir, "mut.run")
		require.NoError(t, os.WriteFile(p, mut, 0o644))
		r, err := OpenRun(p)
		if err != nil {
			continue // header corruption — rejected at open, fine
		}
		sawErr := false
		for {
			_, _, nerr := r.Next()
			if errors.Is(nerr, io.EOF) {
				break
			}
			if nerr != nil {
				sawErr = true
				break
			}
		}
		_ = r.Close()
		assert.True(t, sawErr, "corruption at byte %d must surface an error", pos)
	}

	// Truncation must also fail.
	p := filepath.Join(dir, "trunc.run")
	require.NoError(t, os.WriteFile(p, raw[:len(raw)-9], 0o644))
	r, err := OpenRun(p)
	require.NoError(t, err)
	sawErr := false
	for {
		_, _, nerr := r.Next()
		if errors.Is(nerr, io.EOF) {
			break
		}
		if nerr != nil {
			sawErr = true
			break
		}
	}
	_ = r.Close()
	assert.True(t, sawErr, "truncated run must surface an error")
}

func TestSlab_OutputIsTermSorted(t *testing.T) {
	slab := NewSlab(1 << 16)
	for i := 200; i > 0; i-- { // reverse insertion order
		require.True(t, slab.Append(key(byte(i)), uint32(i)))
	}
	path := filepath.Join(t.TempDir(), "s.run")
	require.NoError(t, WriteRun(path, slab.SortEncode(nil)))
	r, err := OpenRun(path)
	require.NoError(t, err)
	defer r.Close()
	var prev [16]byte
	first := true
	for {
		term, _, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		if !first {
			assert.Equal(t, 1, bytes.Compare(term[:], prev[:]), "terms must stream in ascending order")
		}
		prev = term
		first = false
	}
}
