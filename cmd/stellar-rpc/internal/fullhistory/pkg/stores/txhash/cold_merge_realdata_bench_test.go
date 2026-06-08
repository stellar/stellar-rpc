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
// Scope is the two merge tuning knobs only: maxMergeLeaves (leaf-goroutine
// count / I/O concurrency) and mergeFileBufBytes (per-file read buffer).
// All benchmarks here reuse the helpers already defined in the sibling
// _test.go files (drainSerial, drainParallel, drainParallelTuned,
// buildAtLeaves) so the only thing that changes versus the synthetic suite
// is the input set. No production code edits.

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

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
	matches, err := filepath.Glob(filepath.Join(filepath.Clean(dir), "*.bin"))
	require.NoError(b, err)
	require.NotEmpty(b, matches, "no .bin files under %s", dir)
	sort.Strings(matches)
	for i := range matches {
		matches[i] = filepath.Clean(matches[i])
	}

	// MinLedger is the first ledger of the index GROUP that holds the
	// lowest present chunk — the same anchor BuildColdIndex is given in
	// production (chunk.ID.FirstLedger via IndexBaseChunk). The present
	// files are a tail of that group, so every entry's seq is >= this
	// anchor and the payload offset stays within the 24-bit budget;
	// feedMergedKeys validates both, so a wrong anchor fails loudly.
	firstChunk, err := chunkIDFromBinPath(matches[0])
	require.NoError(b, err)
	minLedger := IndexBaseChunk(firstChunk, DefaultChunksPerIndex).FirstLedger()

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
				buildAtLeaves(b, inputs, out, minLedger, leaves)
				b.StopTimer()
				_ = os.Remove(out)
				b.StartTimer()
			}
			reportKeysPerSec(b, totalKeys)
		})
	}
}
