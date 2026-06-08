package txhash

// cold_merge_realdata_bench_test.go drives the SAME cold-merge / build
// paths as cold_merge_bench_test.go and cold_merge_sweep_bench_test.go,
// but over REAL on-disk .bin chunk files instead of genBenchBins
// synthetic fixtures. It exists to retune maxMergeLeaves and
// mergeFileBufBytes on a cold Linux NVMe — the regime the design
// targets (real O_DIRECT device reads, latency hidden by parallel
// readers) that the darwin/warm-cache benchmarks could never measure
// (O_DIRECT is a no-op there).
//
// Why real data sidesteps the cold-read problem: the genBenchBins
// benchmarks write their fixtures and immediately read them back, so the
// bytes sit in the page cache and (without root drop_caches) the read is
// served warm even on Linux. These .bin files were written long ago and
// are read with O_DIRECT, which bypasses the page cache on every read —
// so each iteration hits the device. No sudo / drop_caches needed.
//
// Pointed at a directory via TXHASH_REAL_BINS (skipped if unset). The
// files are the cold ingester's per-chunk output, conventionally named
// <chunkID:08d>.bin (see cold_index.go); the group's MinLedger anchor is
// derived from the lowest chunk via IndexBaseChunk, exactly as the
// production build locates it. Index outputs and the unsorted builder's
// spill tmpdir go under TXHASH_BENCH_OUT (or b.TempDir(), which honors
// $TMPDIR) — keep both on the same fast device as the inputs.
//
// Coverage: the cold-build tuning knobs — maxMergeLeaves (leaf-goroutine
// count / I/O concurrency: BenchmarkRealMergeNumLeaves and
// BenchmarkRealBuildColdIndexNumLeaves), the merge-leaves-vs-build-workers
// core split (BenchmarkRealBuildSplit, backing defaultBuildWorkers), and
// mergeFileBufBytes (per-file read buffer: BenchmarkRealMergeBufBytes) —
// plus the parallel-vs-serial merge premise (BenchmarkRealColdMerge).
// mergeBatchSize is deliberately NOT swept here: it is a compile-time array
// dimension and can only be compared across separate builds. Benchmarks
// reuse the sibling helpers (drainSerial, drainParallel, drainParallelTuned,
// buildAtLeaves) plus buildColdAt below; only the input set differs from the
// synthetic suite.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/streamhash"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// realColdInputs loads the real .bin chunk files named by TXHASH_REAL_BINS,
// sorted by name (i.e. by chunk ID), and returns them with the group's
// MinLedger anchor and the summed key count. Skips the benchmark if the
// env var is unset so the suite stays green on hosts without the data.
func realColdInputs(b *testing.B) ([]string, uint32, uint64) {
	b.Helper()
	dir := os.Getenv("TXHASH_REAL_BINS")
	if dir == "" {
		b.Skip("TXHASH_REAL_BINS unset; set it to a directory of real .bin chunk files")
	}
	// filepath.Clean(dir) sanitizes the env-derived directory so gosec's taint
	// analysis (G703) is satisfied before the matched names reach
	// scanAndValidate's os.Open; functionally a no-op (Join cleans too).
	matches, err := filepath.Glob(filepath.Join(filepath.Clean(dir), "*.bin"))
	require.NoError(b, err)
	require.NotEmpty(b, matches, "no .bin files under %s", dir)
	sort.Strings(matches) // Glob returns directory order; we need chunk-ID (name) order.

	// MinLedger anchors the whole set at the first ledger of the index GROUP
	// holding the lowest present chunk — the anchor BuildColdIndex is given in
	// production (chunk.ID.FirstLedger via IndexBaseChunk). Every file must
	// belong to that one group: a chunk at or beyond base+DefaultChunksPerIndex
	// would push its payload (seq - minLedger) past the 24-bit budget. The e2e
	// build (feedMergedKeys) would catch that, but the merge-only benchmarks do
	// not check the budget, so enforce the single-group invariant up front.
	firstChunk, err := chunkIDFromBinPath(matches[0])
	require.NoError(b, err)
	lastChunk, err := chunkIDFromBinPath(matches[len(matches)-1])
	require.NoError(b, err)
	base := IndexBaseChunk(firstChunk, DefaultChunksPerIndex)
	require.Lessf(b, uint32(lastChunk)-uint32(base), DefaultChunksPerIndex,
		"TXHASH_REAL_BINS spans more than one index group (chunks %d..%d, group base %d); "+
			"point it at a single %d-chunk group", firstChunk, lastChunk, base, DefaultChunksPerIndex)
	minLedger := base.FirstLedger()

	totalKeys, err := scanAndValidate(matches)
	require.NoError(b, err)
	require.NotZero(b, totalKeys)

	b.Logf("real cold inputs: %d files, %d keys (%.2f GiB), minLedger=%d, dir=%s",
		len(matches), totalKeys,
		float64(totalKeys*binEntrySize)/(1<<30), minLedger, dir)
	return matches, minLedger, totalKeys
}

// chunkIDFromBinPath parses the <chunkID:08d>.bin filename convention.
func chunkIDFromBinPath(path string) (chunk.ID, error) {
	name := strings.TrimSuffix(filepath.Base(path), ".bin")
	n, err := strconv.ParseUint(name, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("txhash: parse chunk id from %q: %w", path, err)
	}
	return chunk.ID(n), nil
}

// realBenchOutDir is where built indexes (and the unsorted builder's sort
// spill) are written. TXHASH_BENCH_OUT pins it to a chosen device;
// otherwise b.TempDir() honors $TMPDIR. Either way the caller is
// responsible for keeping it on the same fast storage as the inputs.
func realBenchOutDir(b *testing.B) string {
	b.Helper()
	if d := os.Getenv("TXHASH_BENCH_OUT"); d != "" {
		return d
	}
	return b.TempDir()
}

func reportKeysPerSec(b *testing.B, totalKeys uint64) {
	b.ReportMetric(float64(totalKeys)/b.Elapsed().Seconds()*float64(b.N), "keys/sec")
}

// BenchmarkRealColdMerge is the real-data counterpart of BenchmarkColdMerge:
// merge-only (no streamhash build), parallel fan-in vs single-threaded heap.
// On a real cold device this is where the parallel pipeline's latency-hiding
// should show its true margin over serial — the warm benchmark could only
// put a lower bound on it.
func BenchmarkRealColdMerge(b *testing.B) {
	inputs, minLedger, totalKeys := realColdInputs(b)

	b.Run("serial", func(b *testing.B) {
		b.SetBytes(int64(totalKeys) * binEntrySize)
		b.ResetTimer()
		for range b.N {
			benchSink ^= drainSerial(b, inputs, minLedger)
		}
		reportKeysPerSec(b, totalKeys)
	})

	b.Run("parallel", func(b *testing.B) {
		b.SetBytes(int64(totalKeys) * binEntrySize)
		b.ResetTimer()
		for range b.N {
			benchSink ^= drainParallel(b, inputs, minLedger)
		}
		reportKeysPerSec(b, totalKeys)
	})
}

// BenchmarkRealMergeNumLeaves sweeps the leaf-goroutine count (I/O
// concurrency) merge-only over real cold reads. The warm sweet spot was
// ~NumCPU; the open question is whether cold device latency rewards
// over-provisioning leaves to keep more reads in flight. Includes counts
// above NumCPU to catch a cold peak.
func BenchmarkRealMergeNumLeaves(b *testing.B) {
	inputs, minLedger, totalKeys := realColdInputs(b)
	for _, leaves := range []int{4, 8, 12, 16, 24, 32, 48, 64} {
		b.Run(fmt.Sprintf("leaves%d", leaves), func(b *testing.B) {
			b.SetBytes(int64(totalKeys) * binEntrySize)
			b.ResetTimer()
			for range b.N {
				benchSink ^= drainParallelTuned(b, inputs, minLedger, leaves, mergeFileBufBytes)
			}
			reportKeysPerSec(b, totalKeys)
		})
	}
}

// BenchmarkRealMergeBufBytes sweeps the per-file read buffer merge-only at
// the production leaf count. Warm throughput was flat 16–128 KiB; cold
// sequential reads may reward the larger 512 KiB–4 MiB buffers (fewer,
// bigger device reads), which is exactly what mergeFileBufBytes's comment
// flags as untested.
func BenchmarkRealMergeBufBytes(b *testing.B) {
	inputs, minLedger, totalKeys := realColdInputs(b)
	for _, kib := range []int{64, 128, 256, 512, 1024, 2048, 4096} {
		bufBytes := kib << 10
		b.Run(fmt.Sprintf("%dKiB", kib), func(b *testing.B) {
			b.SetBytes(int64(totalKeys) * binEntrySize)
			b.ResetTimer()
			for range b.N {
				benchSink ^= drainParallelTuned(b, inputs, minLedger, maxMergeLeaves(), bufBytes)
			}
			reportKeysPerSec(b, totalKeys)
		})
	}
}

// BenchmarkRealBuildColdIndexNumLeaves sweeps the leaf count for the FULL
// end-to-end build (merge + streamhash MPHF), over real cold reads. This is
// the committed driver for maxMergeLeaves(): merge-only and end-to-end can
// disagree because the leaves run concurrently with streamhash's own
// NumCPU/2 block-build workers, so over-provisioning leaves can starve the
// builder. Writes a full index per iteration and removes it untimed.
func BenchmarkRealBuildColdIndexNumLeaves(b *testing.B) {
	inputs, minLedger, totalKeys := realColdInputs(b)
	outDir := realBenchOutDir(b)
	for _, leaves := range []int{8, 12, 16, 24, 32, 48} {
		b.Run(fmt.Sprintf("leaves%d", leaves), func(b *testing.B) {
			b.SetBytes(int64(totalKeys) * binEntrySize)
			b.ResetTimer()
			for i := range b.N {
				out := filepath.Join(outDir, fmt.Sprintf("e2e-%d-%d.idx", leaves, i))
				// Safety net: buildAtLeaves may b.Fatal before the untimed
				// remove below, leaking a partial .idx into a pinned outDir.
				b.Cleanup(func() { _ = os.Remove(out) })
				buildAtLeaves(b, inputs, out, minLedger, leaves)
				b.StopTimer()
				_ = os.Remove(out)
				b.StartTimer()
			}
			reportKeysPerSec(b, totalKeys)
		})
	}
}

// buildColdAt replicates BuildColdIndex's pipeline with EXPLICIT leaf and
// worker counts — BuildColdIndex itself derives them from maxMergeLeaves()
// and defaultBuildWorkers() — so the split of cores between the merge's I/O
// goroutines and streamhash's MPHF-build workers can be swept.
func buildColdAt(b *testing.B, inputs []string, outputPath string, minLedger uint32, numLeaves, workers int) {
	b.Helper()
	total, err := scanAndValidate(inputs)
	require.NoError(b, err)
	opts := append([]streamhash.BuildOption{streamhash.WithWorkers(workers)}, ColdBuildOptions(minLedger)...)
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

// BenchmarkRealBuildSplit sweeps how the cores split between merge leaf
// goroutines (maxMergeLeaves) and streamhash block-build workers
// (defaultBuildWorkers). It backs defaultBuildWorkers's comment: on a cold
// build the builder is the e2e gate, so NumCPU/2 leaves + NumCPU/2 workers
// (= NumCPU, no oversubscription) is the joint optimum — doubling the
// workers saturates, and adding leaves only steals cores from the builder.
func BenchmarkRealBuildSplit(b *testing.B) {
	inputs, minLedger, totalKeys := realColdInputs(b)
	outDir := realBenchOutDir(b)
	half := defaultBuildWorkers() // NumCPU/2 — the production leaf + worker count
	cases := []struct {
		name            string
		leaves, workers int
	}{
		{"even", half, half},                        // production split (NumCPU/2, NumCPU/2)
		{"more-workers", half, 2 * half},            // does the builder want more cores? (saturates)
		{"builder-heavy", max(1, half/2), 2 * half}, // shift cores to the builder
		{"merge-heavy", 2 * half, half},             // more leaves steal from the builder
	}
	for _, tc := range cases {
		b.Run(fmt.Sprintf("%s_l%dw%d", tc.name, tc.leaves, tc.workers), func(b *testing.B) {
			b.SetBytes(int64(totalKeys) * binEntrySize)
			b.ResetTimer()
			for i := range b.N {
				out := filepath.Join(outDir, fmt.Sprintf("split-%d-%d-%d.idx", tc.leaves, tc.workers, i))
				b.Cleanup(func() { _ = os.Remove(out) })
				buildColdAt(b, inputs, out, minLedger, tc.leaves, tc.workers)
				b.StopTimer()
				_ = os.Remove(out)
				b.StartTimer()
			}
			reportKeysPerSec(b, totalKeys)
		})
	}
}
