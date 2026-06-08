package txhash

// cold_merge_sweep_bench_test.go holds ADDITIVE empirical benchmarks for
// the cold-merge investigation. None of these edit production code; the
// bufBytes/numLeaves sweeps drive the existing runtime parameters of
// buildMergeTree/newFileReader directly.
//
//   - BenchmarkSortedVsUnsorted:  full build, sorted k-way merge (current
//     BuildColdIndex path) vs streamhash NewUnsortedBuilder fed straight
//     from the .bin files (single AddKey and concurrent AddKeys variants).
//   - BenchmarkMergeBufBytes:      merge-only throughput sweep over the
//     per-file read buffer size (runtime param to buildMergeTree).
//   - BenchmarkMergeNumLeaves:     merge-only throughput sweep over the
//     leaf-goroutine count (runtime param to buildMergeTree).
//
// All warm-page-cache on the host that runs them; on non-Linux O_DIRECT is
// a no-op so reads come from cache after iteration 1.

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/streamhash"
)

const (
	sweepNumFiles = 50
	sweepPerFile  = 100_000
)

// buildColdIndexUnsorted builds the same index BuildColdIndex builds, but
// via streamhash.NewUnsortedBuilder: it reads each .bin file sequentially
// (NO k-way merge) and feeds every entry through a single AddKey. Same
// payload conversion and same ColdBuildOptions / WithWorkers default as
// the sorted path, so the only difference is sorted-merge vs unsorted.
func buildColdIndexUnsorted(
	b *testing.B, inputs []string, outputPath, tmpDir string, minLedger uint32, workers int,
) {
	b.Helper()
	total, err := scanAndValidate(inputs)
	require.NoError(b, err)

	opts := make([]streamhash.BuildOption, 0, 4)
	if workers > 0 {
		opts = append(opts, streamhash.WithWorkers(workers))
	} else {
		opts = append(opts, streamhash.WithWorkers(defaultBuildWorkers()))
	}
	opts = append(opts, ColdBuildOptions(minLedger)...)

	ub, err := streamhash.NewUnsortedBuilder(context.Background(), outputPath, total, tmpDir, opts...)
	require.NoError(b, err)

	var added uint64
	for _, path := range inputs {
		r, rerr := newFileReader(path, mergeFileBufBytes)
		require.NoError(b, rerr)
		if r.prepareFirst() {
			for {
				entry := r.entry()
				seq := binary.LittleEndian.Uint32(entry[binKeySize:])
				payload := uint64(seq - minLedger)
				require.NoError(b, ub.AddKey(entry[:binKeySize], payload))
				added++
				if !r.advance() {
					break
				}
			}
		}
		require.NoError(b, r.err)
		r.close()
	}
	require.Equal(b, total, added)
	require.NoError(b, ub.Finish())
	require.NoError(b, ub.Close())
}

// buildColdIndexUnsortedConcurrent builds the index via the unsorted
// builder's AddKeys path: numWriters lock-free writers, each reading a
// disjoint slice of the .bin files (NO merge). AddKeys calls Finish
// internally on success.
func buildColdIndexUnsortedConcurrent(
	b *testing.B, inputs []string, outputPath, tmpDir string, minLedger uint32, workers, numWriters int,
) {
	b.Helper()
	total, err := scanAndValidate(inputs)
	require.NoError(b, err)

	opts := make([]streamhash.BuildOption, 0, 4)
	if workers > 0 {
		opts = append(opts, streamhash.WithWorkers(workers))
	} else {
		opts = append(opts, streamhash.WithWorkers(defaultBuildWorkers()))
	}
	opts = append(opts, ColdBuildOptions(minLedger)...)

	ub, err := streamhash.NewUnsortedBuilder(context.Background(), outputPath, total, tmpDir, opts...)
	require.NoError(b, err)

	filesPerWriter := (len(inputs) + numWriters - 1) / numWriters
	var added atomic.Uint64
	aerr := ub.AddKeys(numWriters, func(writerID int, addKey func([]byte, uint64) error) error {
		start := writerID * filesPerWriter
		if start >= len(inputs) {
			return nil
		}
		end := min(start+filesPerWriter, len(inputs))
		var local uint64
		for _, path := range inputs[start:end] {
			r, rerr := newFileReader(path, mergeFileBufBytes)
			if rerr != nil {
				return rerr
			}
			if r.prepareFirst() {
				for {
					entry := r.entry()
					seq := binary.LittleEndian.Uint32(entry[binKeySize:])
					payload := uint64(seq - minLedger)
					if kerr := addKey(entry[:binKeySize], payload); kerr != nil {
						r.close()
						return kerr
					}
					local++
					if !r.advance() {
						break
					}
				}
			}
			if r.err != nil {
				r.close()
				return r.err
			}
			r.close()
		}
		added.Add(local)
		return nil
	})
	require.NoError(b, aerr)
	require.Equal(b, total, added.Load())
	require.NoError(b, ub.Close())
}

// BenchmarkSortedVsUnsorted is the priority-1 comparison: same 5M-entry
// dataset built via the current sorted k-way merge vs streamhash's
// unsorted builder. All cases use the NumCPU/2 worker default.
func BenchmarkSortedVsUnsorted(b *testing.B) {
	dir := b.TempDir()
	minLedger := fixtureBaseChunk.FirstLedger()
	inputs := genBenchBins(b, dir, sweepNumFiles, sweepPerFile, minLedger)
	totalKeys := sweepNumFiles * sweepPerFile

	b.Run("sorted/default", func(b *testing.B) {
		b.SetBytes(int64(totalKeys) * binEntrySize)
		b.ResetTimer()
		for i := range b.N {
			out := filepath.Join(dir, fmt.Sprintf("sorted-%d.idx", i))
			require.NoError(b, BuildColdIndex(context.Background(), inputs, out, minLedger))
			b.StopTimer()
			_ = os.Remove(out)
			b.StartTimer()
		}
		b.ReportMetric(float64(totalKeys)/b.Elapsed().Seconds()*float64(b.N), "keys/sec")
	})

	b.Run("unsorted/default/AddKey", func(b *testing.B) {
		b.SetBytes(int64(totalKeys) * binEntrySize)
		b.ResetTimer()
		for i := range b.N {
			out := filepath.Join(dir, fmt.Sprintf("unsorted-%d.idx", i))
			tmp := b.TempDir()
			buildColdIndexUnsorted(b, inputs, out, tmp, minLedger, 0)
			b.StopTimer()
			_ = os.Remove(out)
			b.StartTimer()
		}
		b.ReportMetric(float64(totalKeys)/b.Elapsed().Seconds()*float64(b.N), "keys/sec")
	})

	// Concurrent-writer unsorted variant: NumCPU/2 writers (mirrors the
	// merge's own leaf parallelism budget; AddKeys ingests lock-free).
	cw := max(1, runtime.NumCPU()/2)
	b.Run(fmt.Sprintf("unsorted/default/AddKeys-%dw", cw), func(b *testing.B) {
		b.SetBytes(int64(totalKeys) * binEntrySize)
		b.ResetTimer()
		for i := range b.N {
			out := filepath.Join(dir, fmt.Sprintf("unsorted-cw-%d.idx", i))
			tmp := b.TempDir()
			buildColdIndexUnsortedConcurrent(b, inputs, out, tmp, minLedger, 0, cw)
			b.StopTimer()
			_ = os.Remove(out)
			b.StartTimer()
		}
		b.ReportMetric(float64(totalKeys)/b.Elapsed().Seconds()*float64(b.N), "keys/sec")
	})
}

// drainParallelTuned is drainParallel with explicit numLeaves/bufBytes so
// the sweeps can drive buildMergeTree's runtime parameters without touching
// production constants.
func drainParallelTuned(b *testing.B, inputs []string, minLedger uint32, numLeaves, bufBytes int) uint64 {
	b.Helper()
	m := newMerger(context.Background())
	defer m.stop()
	finalCh, finalPool := m.buildMergeTree(inputs, min(numLeaves, len(inputs)), bufBytes)

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

// BenchmarkMergeBufBytes sweeps the per-file read buffer at the production
// leaf count (maxMergeLeaves). Merge-only (no streamhash).
func BenchmarkMergeBufBytes(b *testing.B) {
	dir := b.TempDir()
	minLedger := fixtureBaseChunk.FirstLedger()
	inputs := genBenchBins(b, dir, sweepNumFiles, sweepPerFile, minLedger)
	totalKeys := sweepNumFiles * sweepPerFile

	for _, kib := range []int{16, 32, 64, 128, 256} {
		bufBytes := kib << 10
		b.Run(fmt.Sprintf("%dKiB", kib), func(b *testing.B) {
			b.SetBytes(int64(totalKeys) * binEntrySize)
			b.ResetTimer()
			for range b.N {
				benchSink ^= drainParallelTuned(b, inputs, minLedger, maxMergeLeaves(), bufBytes)
			}
			b.ReportMetric(float64(totalKeys)/b.Elapsed().Seconds()*float64(b.N), "keys/sec")
		})
	}
}

// BenchmarkMergeNumLeaves sweeps the leaf-goroutine count at the production
// buffer size. On the 50-file fixture only 8/16/25/50 produce distinct
// topologies (32 collapses to the same 25-leaf shape as 25). Merge-only.
func BenchmarkMergeNumLeaves(b *testing.B) {
	dir := b.TempDir()
	minLedger := fixtureBaseChunk.FirstLedger()
	inputs := genBenchBins(b, dir, sweepNumFiles, sweepPerFile, minLedger)
	totalKeys := sweepNumFiles * sweepPerFile

	for _, leaves := range []int{6, 8, 10, 12, 16, 25, 32, 50} {
		b.Run(fmt.Sprintf("leaves%d", leaves), func(b *testing.B) {
			b.SetBytes(int64(totalKeys) * binEntrySize)
			b.ResetTimer()
			for range b.N {
				benchSink ^= drainParallelTuned(b, inputs, minLedger, leaves, mergeFileBufBytes)
			}
			b.ReportMetric(float64(totalKeys)/b.Elapsed().Seconds()*float64(b.N), "keys/sec")
		})
	}
}

// buildAtLeaves replicates BuildColdIndex's pipeline (scan → sorted
// builder → fan-in merge → AddKey → finish) but with an explicit leaf
// count, so the END-TO-END effect of the leaf count can be measured.
// BuildColdIndex itself derives the count from maxMergeLeaves(), so this
// is the only way to sweep it end-to-end from committed code.
func buildAtLeaves(b *testing.B, inputs []string, outputPath string, minLedger uint32, numLeaves int) {
	b.Helper()
	total, err := scanAndValidate(inputs)
	require.NoError(b, err)

	opts := make([]streamhash.BuildOption, 0, 4)
	opts = append(opts, streamhash.WithWorkers(defaultBuildWorkers()))
	opts = append(opts, ColdBuildOptions(minLedger)...)
	builder, err := streamhash.NewSortedBuilder(context.Background(), outputPath, total, opts...)
	require.NoError(b, err)

	m := newMerger(context.Background())
	finalCh, finalPool := m.buildMergeTree(inputs, min(numLeaves, len(inputs)), mergeFileBufBytes)
	added, err := feedMergedKeys(builder, finalCh, finalPool, m, minLedger)
	m.stop()
	require.NoError(b, err)
	require.Equal(b, total, added)
	require.NoError(b, builder.Finish())
	require.NoError(b, builder.Close())
}

// BenchmarkBuildColdIndexNumLeaves sweeps the leaf count for the FULL
// build (merge + streamhash), answering whether leaf count moves
// end-to-end throughput or only merge-only throughput. This is the
// committed counterpart that makes the maxMergeLeaves() choice
// reproducible (BenchmarkMergeNumLeaves is merge-only).
func BenchmarkBuildColdIndexNumLeaves(b *testing.B) {
	dir := b.TempDir()
	minLedger := fixtureBaseChunk.FirstLedger()
	inputs := genBenchBins(b, dir, sweepNumFiles, sweepPerFile, minLedger)
	totalKeys := sweepNumFiles * sweepPerFile

	for _, leaves := range []int{8, 10, 16, 32, 50} {
		b.Run(fmt.Sprintf("leaves%d", leaves), func(b *testing.B) {
			b.SetBytes(int64(totalKeys) * binEntrySize)
			b.ResetTimer()
			for i := range b.N {
				out := filepath.Join(dir, fmt.Sprintf("e2e-%d-%d.idx", leaves, i))
				buildAtLeaves(b, inputs, out, minLedger, leaves)
				b.StopTimer()
				_ = os.Remove(out)
				b.StartTimer()
			}
			b.ReportMetric(float64(totalKeys)/b.Elapsed().Seconds()*float64(b.N), "keys/sec")
		})
	}
}
