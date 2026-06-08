package txhash

// cold_merge_order_test.go guards the assumption that justifies keying the
// cold merge on the 8-byte prefix only (no k1 tiebreak — see cold_merge.go):
//
//	The built index is byte-identical regardless of the order in which keys
//	sharing a routing prefix (same k0) reach the builder.
//
// Why this needs a guard rather than a comment: the property is a streamhash
// implementation detail, not a contract. streamhash's blockBuilder documents
// that within-block AddKey order doesn't affect *correctness*, and routing
// (FastRange32 over the big-endian prefix) plus the sole "block index
// non-decreasing" precondition are algorithm-independent — so a prefix-only
// (k0) sorted stream is always accepted and queries correctly, for any
// algorithm. But byte-identical *layout* under within-prefix reordering is
// only guaranteed analytically for AlgoBijection (slots are key+seed
// determined; the per-bucket seed is the first one that makes the bucket's
// key SET injective). For AlgoPTRHash it holds empirically (verified here);
// for a hypothetical future order-sensitive algorithm it might not. If that
// day comes, this test fails and forces a conscious choice (restore a full-key
// tiebreak, or accept order-dependent bytes) instead of silently shipping
// non-reproducible artifacts.
//
// The fixture forces same-k0 collisions split ACROSS files, then builds with
// the input files in forward vs reversed order — which flips the colliding
// twins' merge order — and diffs the bytes.

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/streamhash"
)

func TestBuildColdIndexOrderIndependent(t *testing.T) {
	const (
		nFiles = 8
		nBase  = 8000
		nTwin  = 300 // forced 8-byte-prefix collisions, each split across two files
	)
	r := rand.New(rand.NewPCG(0xBEEF, 0xF00D))
	type entry struct {
		k0, k1 uint64
		seq    uint32
	}
	files := make([][]entry, nFiles)
	var seq uint32 = 1
	k0s := make([]uint64, 0, nBase)
	for i := range nBase {
		k0 := r.Uint64()
		k0s = append(k0s, k0)
		files[i%nFiles] = append(files[i%nFiles], entry{k0, r.Uint64(), seq})
		seq++
	}
	for range nTwin {
		k0 := k0s[r.IntN(len(k0s))] // reuse an existing prefix -> a guaranteed k0 collision
		f := r.IntN(nFiles)
		files[(f+3)%nFiles] = append(files[(f+3)%nFiles], entry{k0, r.Uint64(), seq}) // different file from a typical base
		seq++
	}

	dir := t.TempDir()
	paths := make([]string, nFiles)
	for f := range files {
		es := files[f]
		sort.Slice(es, func(i, j int) bool { // each .bin must be sorted by the big-endian prefix
			if es[i].k0 != es[j].k0 {
				return es[i].k0 < es[j].k0
			}
			return es[i].k1 < es[j].k1
		})
		buf := make([]byte, binHeaderSize+len(es)*binEntrySize)
		binary.LittleEndian.PutUint64(buf[:binHeaderSize], uint64(len(es)))
		off := binHeaderSize
		for _, x := range es {
			binary.BigEndian.PutUint64(buf[off:off+8], x.k0)
			binary.BigEndian.PutUint64(buf[off+8:off+16], x.k1)
			binary.LittleEndian.PutUint32(buf[off+binKeySize:], x.seq)
			off += binEntrySize
		}
		paths[f] = filepath.Join(dir, fmt.Sprintf("%08d.bin", f))
		require.NoError(t, os.WriteFile(paths[f], buf, 0o600))
	}
	reversed := make([]string, nFiles)
	for i := range paths {
		reversed[i] = paths[nFiles-1-i]
	}

	build := func(t *testing.T, inputs []string, name string, opts ...streamhash.BuildOption) []byte {
		out := filepath.Join(dir, name)
		require.NoError(t, BuildColdIndex(context.Background(), inputs, out, 0, opts...))
		b, err := os.ReadFile(out)
		require.NoError(t, err)
		require.NoError(t, os.Remove(out))
		return b
	}

	// Each case: forward input order vs reversed input order must be byte-identical.
	// "default" exercises the exact production configuration (BuildColdIndex's
	// own algorithm choice); the explicit cases pin both shipped algorithms.
	cases := []struct {
		name string
		opts []streamhash.BuildOption
	}{
		{"default", nil},
		{"bijection", []streamhash.BuildOption{streamhash.WithAlgorithm(streamhash.AlgoBijection)}},
		{"ptrhash", []streamhash.BuildOption{streamhash.WithAlgorithm(streamhash.AlgoPTRHash)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fwd := build(t, paths, tc.name+"-fwd.idx", tc.opts...)
			rev := build(t, reversed, tc.name+"-rev.idx", tc.opts...)
			require.Equalf(t, fwd, rev,
				"%s: index bytes changed when same-prefix keys were merged in a different order "+
					"(%d keys, %d forced prefix collisions) — the k1-free merge is no longer "+
					"order-independent for this algorithm; see cold_merge.go", tc.name, nBase+nTwin, nTwin)
		})
	}
}
