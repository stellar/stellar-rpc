package runspill

import (
	"bytes"
	"encoding/binary"
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
	payload, records := slab.SortEncode(nil)
	require.NoError(t, WriteRun(path, payload, records))
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
	payload, records := slab.SortEncode(nil)
	require.NoError(t, WriteRun(path, payload, records))
	assert.Equal(t, map[[16]byte][]uint32{key(9): {5, 6}}, drain(t, path))
}

func TestRunReader_DetectsCorruption(t *testing.T) {
	slab := NewSlab(1 << 16)
	for i := range uint32(1000) {
		require.True(t, slab.Append(key(byte(i%7)), i))
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "c.run")
	payload, records := slab.SortEncode(nil)
	require.NoError(t, WriteRun(path, payload, records))

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

	// Truncation must also fail — at open now that the header's payload
	// length is bounded by the file's actual capacity.
	p := filepath.Join(dir, "trunc.run")
	require.NoError(t, os.WriteFile(p, raw[:len(raw)-9], 0o644))
	_, err = OpenRun(p)
	require.ErrorIs(t, err, ErrCorruptRun, "truncated run must surface an error")

	// The magic is a version gate: the header relayout rode the EVR1→EVR2
	// bump, so the old tag must be rejected, not parsed with new offsets.
	prev := append([]byte(nil), raw...)
	prev[3] = '1'
	pPrev := filepath.Join(dir, "evr1.run")
	require.NoError(t, os.WriteFile(pPrev, prev, 0o644))
	_, err = OpenRun(pPrev)
	require.ErrorIs(t, err, ErrCorruptRun, "EVR1 magic must be rejected")
}

func TestSlab_OutputIsTermSorted(t *testing.T) {
	slab := NewSlab(1 << 16)
	for i := 200; i > 0; i-- { // reverse insertion order
		require.True(t, slab.Append(key(byte(i)), uint32(i)))
	}
	path := filepath.Join(t.TempDir(), "s.run")
	payload, records := slab.SortEncode(nil)
	require.NoError(t, WriteRun(path, payload, records))
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

// TestRunHeader_RecordCount pins the header's record-count field: written by
// both writer paths, exposed pre-drain, bounded at open, and cross-checked
// against the actual drain at EOF.
func TestRunHeader_RecordCount(t *testing.T) {
	path := filepath.Join(t.TempDir(), "h.run")
	rw, err := NewRunWriter(path)
	require.NoError(t, err)
	require.NoError(t, rw.Append(key(1), []uint32{1, 2}))
	require.NoError(t, rw.Append(key(2), []uint32{7}))
	require.NoError(t, rw.Commit())

	r, err := OpenRun(path)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), r.Records())
	require.NoError(t, r.Close())
	assert.Len(t, drain(t, path), 2)

	raw, err := os.ReadFile(path)
	require.NoError(t, err)

	// A count beyond the payload's structural capacity is rejected at open,
	// before it can size anything.
	big := append([]byte(nil), raw...)
	binary.BigEndian.PutUint64(big[12:], 1<<40)
	pBig := filepath.Join(t.TempDir(), "big.run")
	require.NoError(t, os.WriteFile(pBig, big, 0o644))
	_, err = OpenRun(pBig)
	require.ErrorIs(t, err, ErrCorruptRun)

	// A plausible-but-wrong count passes open and is caught by the drain
	// cross-check at EOF (the payload and CRC are intact).
	off := append([]byte(nil), raw...)
	binary.BigEndian.PutUint64(off[12:], 1)
	pOff := filepath.Join(t.TempDir(), "off.run")
	require.NoError(t, os.WriteFile(pOff, off, 0o644))
	r2, err := OpenRun(pOff)
	require.NoError(t, err)
	sawErr := false
	for {
		_, _, nerr := r2.Next()
		if errors.Is(nerr, io.EOF) {
			break
		}
		if nerr != nil {
			require.ErrorIs(t, nerr, ErrCorruptRun)
			sawErr = true
			break
		}
	}
	assert.True(t, sawErr, "count mismatch must fail the drain")
	_, _, again := r2.Next()
	require.ErrorIs(t, again, ErrCorruptRun, "the failure must be sticky, not a clean EOF")
	_ = r2.Close()

	// A payload length beyond the file's capacity is rejected at open.
	long := append([]byte(nil), raw...)
	binary.BigEndian.PutUint64(long[4:], uint64(len(raw)))
	pLong := filepath.Join(t.TempDir(), "long.run")
	require.NoError(t, os.WriteFile(pLong, long, 0o644))
	_, err = OpenRun(pLong)
	require.ErrorIs(t, err, ErrCorruptRun)

	// The capacity bound accounts for the trailer: one byte past the true
	// payload length must already be rejected.
	graze := append([]byte(nil), raw...)
	binary.BigEndian.PutUint64(graze[4:], uint64(len(raw)-HeaderLen-4+1))
	pGraze := filepath.Join(t.TempDir(), "graze.run")
	require.NoError(t, os.WriteFile(pGraze, graze, 0o644))
	_, err = OpenRun(pGraze)
	require.ErrorIs(t, err, ErrCorruptRun)
}

// TestRunWriter_DiscardLeavesNothing pins the two-phase lifecycle: a
// discarded writer publishes nothing and leaves no temp file, and Discard
// after Commit must not touch the committed file.
func TestRunWriter_DiscardLeavesNothing(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "aborted.run")
	rw, err := NewRunWriter(path)
	require.NoError(t, err)
	require.NoError(t, rw.Append(key(1), []uint32{1}))
	rw.Discard()
	_, err = os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist, "discarded run must not be published")
	_, err = os.Stat(path + ".tmp")
	require.ErrorIs(t, err, os.ErrNotExist, "discarded run must not leave its temp file")

	committed := filepath.Join(dir, "committed.run")
	rw2, err := NewRunWriter(committed)
	require.NoError(t, err)
	require.NoError(t, rw2.Append(key(2), []uint32{4, 9}))
	require.NoError(t, rw2.Commit())
	rw2.Discard() // deferred-Discard idiom: must be a no-op now
	assert.Equal(t, map[[16]byte][]uint32{key(2): {4, 9}}, drain(t, committed))
}

// TestRunWriter_CommitFailureRemovesTemp forces Commit's rename step to fail
// (the final name is occupied by a directory) and requires the failed Commit
// to surface the error without leaving its temp file behind.
func TestRunWriter_CommitFailureRemovesTemp(t *testing.T) {
	path := filepath.Join(t.TempDir(), "blocked.run")
	rw, err := NewRunWriter(path)
	require.NoError(t, err)
	require.NoError(t, rw.Append(key(1), []uint32{1}))
	require.NoError(t, os.Mkdir(path, 0o755)) // rename's destination is now a directory
	require.Error(t, rw.Commit())
	_, serr := os.Stat(path + ".tmp")
	require.ErrorIs(t, serr, os.ErrNotExist, "failed Commit must remove the temp file")
}
