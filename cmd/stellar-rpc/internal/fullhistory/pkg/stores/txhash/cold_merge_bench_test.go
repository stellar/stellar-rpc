package txhash

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/streamhash"
)

// benchRec is one synthetic (key, seq) pair used to lay out a .bin file.
type benchRec struct {
	key [binKeySize]byte
	seq uint32
}

// genBenchBins writes numFiles sorted .bin files of perFile entries each
// under dir and returns their paths. Keys are random (16-byte unique by
// birthday bound at these sizes); seqs are dense offsets from minLedger,
// staying within the 24-bit payload budget. Setup only — not timed.
//
//nolint:unparam // reusable bench fixture; current callers happen to share the same size
func genBenchBins(b *testing.B, dir string, numFiles, perFile int, minLedger uint32) []string {
	b.Helper()
	r := rand.New(rand.NewPCG(0xC0FFEE, 0x1234))
	recs := make([]benchRec, perFile)
	inputs := make([]string, 0, numFiles)

	for f := range numFiles {
		base := uint32(f * perFile)
		for k := range recs {
			binary.LittleEndian.PutUint64(recs[k].key[0:8], r.Uint64())
			binary.LittleEndian.PutUint64(recs[k].key[8:16], r.Uint64())
			recs[k].seq = minLedger + base + uint32(k)
		}
		sort.Slice(recs, func(i, j int) bool {
			a0 := binary.BigEndian.Uint64(recs[i].key[0:8])
			b0 := binary.BigEndian.Uint64(recs[j].key[0:8])
			if a0 != b0 {
				return a0 < b0
			}
			return binary.BigEndian.Uint64(recs[i].key[8:16]) < binary.BigEndian.Uint64(recs[j].key[8:16])
		})

		path := filepath.Join(dir, fmt.Sprintf("%08d.bin", f))
		buf := make([]byte, binHeaderSize+perFile*binEntrySize)
		binary.LittleEndian.PutUint64(buf[:binHeaderSize], uint64(perFile))
		off := binHeaderSize
		for k := range recs {
			copy(buf[off:], recs[k].key[:])
			binary.LittleEndian.PutUint32(buf[off+binKeySize:], recs[k].seq)
			off += binEntrySize
		}
		require.NoError(b, os.WriteFile(path, buf, 0o600))
		inputs = append(inputs, path)
	}
	return inputs
}

// BenchmarkBuildColdIndex measures end-to-end build throughput over a
// representative chunk group. NOTE: on non-Linux hosts O_DIRECT is a
// no-op, so reads are served from the page cache after the first
// iteration — this measures merge + MPHF-build CPU, not cold disk I/O.
func BenchmarkBuildColdIndex(b *testing.B) {
	const (
		numFiles = 50
		perFile  = 100_000
	)
	dir := b.TempDir()
	minLedger := fixtureBaseChunk.FirstLedger()
	inputs := genBenchBins(b, dir, numFiles, perFile, minLedger)
	totalKeys := numFiles * perFile

	// Compare streamhash block-build parallelism: serial (the lower
	// bound), the package default (no override → NumCPU/2), and NumCPU.
	// The merge feeds the same stream in every case, so this isolates how
	// much the consumer side gates throughput.
	cases := []struct {
		name    string
		workers int // <=0 means "no override; use BuildColdIndex's default"
	}{
		{"serial", 1},
		{"default", 0},
		{"numcpu", runtime.NumCPU()},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			var opts []streamhash.BuildOption
			if tc.workers > 0 {
				opts = append(opts, streamhash.WithWorkers(tc.workers))
			}
			b.SetBytes(int64(totalKeys) * binEntrySize)
			b.ResetTimer()
			for i := range b.N {
				out := filepath.Join(dir, fmt.Sprintf("out-%s-%d.idx", tc.name, i))
				if err := BuildColdIndex(context.Background(), inputs, out, minLedger, opts...); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				_ = os.Remove(out)
				b.StartTimer()
			}
			b.ReportMetric(float64(totalKeys)/b.Elapsed().Seconds()*float64(b.N), "keys/sec")
		})
	}
}

// benchSink absorbs the merged payloads so the compiler can't elide the
// drain loops in the merge-only benchmarks.
var benchSink uint64

// BenchmarkColdMerge isolates the merge component — it streams entries
// out of the .bin files in globally sorted order WITHOUT feeding
// streamhash, so it measures the merge's own ceiling (and whether the
// parallel fan-in pipeline actually beats a dead-simple single-threaded
// merge over the same files). NOTE: warm page cache on non-Linux, so I/O
// latency — the very thing the parallel pipeline hides — is mostly free
// here; the parallel/serial gap is therefore a lower bound on the real
// (cold-disk) win.
func BenchmarkColdMerge(b *testing.B) {
	const (
		numFiles = 50
		perFile  = 100_000
	)
	dir := b.TempDir()
	minLedger := fixtureBaseChunk.FirstLedger()
	inputs := genBenchBins(b, dir, numFiles, perFile, minLedger)
	totalKeys := numFiles * perFile

	b.Run("serial", func(b *testing.B) {
		b.SetBytes(int64(totalKeys) * binEntrySize)
		b.ResetTimer()
		for range b.N {
			benchSink ^= drainSerial(b, inputs, minLedger)
		}
		b.ReportMetric(float64(totalKeys)/b.Elapsed().Seconds()*float64(b.N), "keys/sec")
	})

	b.Run("parallel", func(b *testing.B) {
		b.SetBytes(int64(totalKeys) * binEntrySize)
		b.ResetTimer()
		for range b.N {
			benchSink ^= drainParallel(b, inputs, minLedger)
		}
		b.ReportMetric(float64(totalKeys)/b.Elapsed().Seconds()*float64(b.N), "keys/sec")
	})
}

// drainSerial k-way merges inputs in a single goroutine — no channels,
// no fan-in, just one heap over fileReaders — applying the same per-entry
// work feedMergedKeys does (seq extraction) but discarding the result.
// This is the baseline the parallel pipeline must beat to justify itself.
func drainSerial(b *testing.B, inputs []string, minLedger uint32) uint64 {
	b.Helper()
	readers := make([]*fileReader, 0, len(inputs))
	defer func() {
		for _, r := range readers {
			r.close()
		}
	}()
	h := make([]mergeEntry, 0, len(inputs))
	for _, path := range inputs {
		r, err := newFileReader(path, mergeFileBufBytes)
		require.NoError(b, err)
		readers = append(readers, r)
		if r.prepareFirst() {
			h = append(h, mergeEntry{k0: r.k0(), k1: r.k1(), idx: len(readers) - 1})
		}
	}
	n := len(h)
	for i := n/2 - 1; i >= 0; i-- {
		siftDown(h, i, n)
	}

	var sink uint64
	for n > 0 {
		r := readers[h[0].idx]
		sink ^= uint64(binary.LittleEndian.Uint32(r.entry()[binKeySize:]) - minLedger)
		if r.advance() {
			h[0] = mergeEntry{k0: r.k0(), k1: r.k1(), idx: h[0].idx}
			siftDown(h, 0, n)
		} else {
			require.NoError(b, r.err)
			n--
			if n > 0 {
				h[0] = h[n]
				siftDown(h, 0, n)
			}
		}
	}
	return sink
}

// drainParallel runs the real fan-in merge tree and drains the final
// stream with the same per-entry work, but no AddKey.
func drainParallel(b *testing.B, inputs []string, minLedger uint32) uint64 {
	b.Helper()
	m := newMerger(context.Background())
	defer m.stop()
	finalCh, finalPool := m.buildMergeTree(inputs, min(maxMergeLeaves(), len(inputs)), mergeFileBufBytes)

	var sink uint64
	for batch := range finalCh {
		data := batch.data[:batch.count*binEntrySize]
		for off := 0; off < len(data); off += binEntrySize {
			sink ^= uint64(binary.LittleEndian.Uint32(data[off+binKeySize:off+binEntrySize]) - minLedger)
		}
		if !m.send(finalPool, batch) {
			break
		}
	}
	require.NoError(b, m.firstErr())
	return sink
}
