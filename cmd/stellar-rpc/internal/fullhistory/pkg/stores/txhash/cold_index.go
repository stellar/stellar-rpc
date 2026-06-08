package txhash

// cold_index.go is the build half of the cold txhash pipeline. It
// merges the per-chunk sorted tx-hash files for one chunk group into
// a single streamhash MPHF index — one file per group, per the
// geometry described in cold_format.go. The parallel merge that feeds
// the builder lives in cold_merge.go; the reader in cold_reader.go.
//
// Input format (.bin). The per-chunk files are produced by the cold
// txhash ingester (#765, not built yet); here the format is the
// build's input spec, and cold_merge.go's fileReader is the seam to
// swap in #765's real output. One file per chunk, conventionally named
// <chunkID:08d>.bin:
//
//	offset  size        field
//	0       8           entry count            (uint64 LE)
//	8       count × 20   entries, each:
//	          16          txhash[:16]            (key; streamhash routing)
//	          4           ledger seq             (uint32 LE, absolute)
//
// Entries within a file are sorted ascending by the big-endian uint64
// of their first 8 key bytes — the order streamhash's sorted builder
// expects (block index is monotonic in that prefix). Across files the
// build k-way merges them into one globally sorted stream (the parallel
// fan-in merge in cold_merge.go) and feeds it single-pass into the
// builder.

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"

	"github.com/stellar/streamhash"
)

const (
	// binKeySize is the txhash prefix width keyed on in the .bin file
	// and the streamhash index. streamhash routes and fingerprints on
	// the first 16 bytes, so the cold store keys on the 16-byte prefix
	// (the reader still accepts the full 32-byte hash).
	binKeySize = 16
	// binSeqSize is the absolute ledger-seq width in a .bin entry.
	binSeqSize = 4
	// binEntrySize is the fixed on-disk entry width.
	binEntrySize = binKeySize + binSeqSize
	// binHeaderSize is the leading uint64 LE entry-count header.
	binHeaderSize = 8
)

// ErrEmptyBuildSet is returned by BuildColdIndex when the chunk
// group's inputs hold zero entries. A streamhash index over zero keys
// has no slots and isn't useful, so the build refuses rather than
// emitting an unqueryable file.
var ErrEmptyBuildSet = errors.New("txhash: cannot build a cold index with zero keys")

// BuildColdIndex builds the cold txhash index for one chunk group and
// writes it to outputPath. inputs are the per-chunk .bin files (one
// per chunk in the group); minLedger is the group's MinLedger anchor
// (the first ledger of the group's lowest chunk — see
// chunk.ID.FirstLedger), embedded in the index so the reader recovers
// absolute seqs.
//
// Each input file is internally sorted; BuildColdIndex k-way merges
// them into one globally sorted stream — via the parallel, O_DIRECT
// fan-in merge in cold_merge.go — and feeds it single-pass into
// streamhash's sorted-mode builder, so I/O, merge CPU, and MPHF
// building all overlap. The total key count is summed from the file
// headers up front because streamhash needs it before the first AddKey.
//
// Returns ErrEmptyBuildSet when the group holds zero entries. By default
// the streamhash block build runs with runtime.NumCPU()/2 workers (the
// single biggest throughput lever — measured ~2.7× over single-threaded;
// see BenchmarkBuildColdIndex); callers can override the worker count, or
// anything else, by passing their own opts, which take precedence. On any
// error the partial output file is removed.
//
// ctx cancels the build: it propagates to the merge goroutines and to
// streamhash (polled during AddKey), so a long build over a large group
// surfaces cancellation promptly.
func BuildColdIndex(
	ctx context.Context,
	inputs []string,
	outputPath string,
	minLedger uint32,
	opts ...streamhash.BuildOption,
) (err error) {
	if len(inputs) == 0 {
		return ErrEmptyBuildSet
	}

	total, err := scanAndValidate(inputs)
	if err != nil {
		return err
	}
	if total == 0 {
		return ErrEmptyBuildSet
	}

	// Default to parallel block building; caller opts come last so an
	// explicit WithWorkers (or any other option) overrides the default.
	buildOpts := make([]streamhash.BuildOption, 0, len(opts)+4)
	buildOpts = append(buildOpts, streamhash.WithWorkers(defaultBuildWorkers()))
	buildOpts = append(buildOpts, ColdBuildOptions(minLedger)...)
	buildOpts = append(buildOpts, opts...)
	builder, berr := streamhash.NewSortedBuilder(ctx, outputPath, total, buildOpts...)
	if berr != nil {
		return fmt.Errorf("txhash: create cold index builder at %s: %w", outputPath, berr)
	}
	// Close is safe to call after Finish (it shuts down any parallel
	// workers) and removes the partial output on every error path before
	// Finish. Only overwrite a nil err so a cleanup failure surfaces
	// without masking the real one.
	defer func() {
		if cerr := builder.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("txhash: close cold index builder: %w", cerr)
		}
	}()

	numLeaves := min(maxMergeLeaves(), len(inputs))
	m := newMerger(ctx)
	defer m.stop()
	finalCh, finalPool := m.buildMergeTree(inputs, numLeaves, mergeFileBufBytes)

	added, err := feedMergedKeys(builder, finalCh, finalPool, m, minLedger)
	if err != nil {
		return err
	}
	if added != total {
		return fmt.Errorf("txhash: key count mismatch: headers declared %d, merged %d", total, added)
	}
	if ferr := builder.Finish(); ferr != nil {
		return fmt.Errorf("txhash: finalize cold index at %s: %w", outputPath, ferr)
	}
	return nil
}

// defaultBuildWorkers is the streamhash block-build parallelism used
// when the caller doesn't specify one. NumCPU/2 captures essentially all
// of the parallel-build speedup in measurement while leaving cores for
// the merge goroutines and GC (matches streamhash's own bench default).
//
// This pairs with maxMergeLeaves (also NumCPU/2): NumCPU/2 build workers +
// NumCPU/2 merge leaves = NumCPU cores, with no oversubscription. A cold
// (leaves, workers) split sweep confirmed this is the joint end-to-end
// optimum on a 16-core host — the builder saturates at NumCPU/2 workers
// (doubling them was neutral), and giving the merge more leaves only steals
// cores from the build workers, which the e2e profile shows are the gate
// (~62% of build CPU, dominated by the bijection MPHF solve). So shifting
// the split in either direction loses; the remaining cost is intrinsic
// solve work, not a tuning knob.
func defaultBuildWorkers() int {
	return max(1, runtime.NumCPU()/2)
}

// maxMergeLeaves caps the number of leaf merge goroutines (= peak number
// of concurrent O_DIRECT reads). NumCPU/2 is the measured sweet spot: the
// merge runs concurrently with streamhash's own NumCPU/2 block-build
// workers (defaultBuildWorkers), so NumCPU/2 leaves + NumCPU/2 builders
// fill the cores exactly. Going to NumCPU oversubscribes them — the
// scheduler churn starves the builder — and buys no I/O in return, since
// the device saturates well below NumCPU concurrent readers. The actual
// leaf count is further capped at the input file count.
//
// Measured cold: a Linux NVMe O_DIRECT build of real per-chunk data (382M
// keys, 16-core Graviton) ran ~18% faster end-to-end and ~34% faster
// merge-only at NumCPU/2 than at NumCPU; throughput was monotonic in
// 8 > 12 > 16 leaves. (The earlier NumCPU pick was warm-cache, where reads
// never block so leaf count tracked CPU; on fast NVMe reads don't block
// long enough for I/O to gate, so the builder does, and fewer leaves win.)
// See cold_merge_realdata_bench_test.go: BenchmarkRealBuildColdIndexNumLeaves.
func maxMergeLeaves() int {
	return max(1, runtime.NumCPU()/2)
}

// scanAndValidate reads each input's entry-count header and cross-checks
// it against the file length, returning the summed key count for the
// whole group. The header count is authoritative; validating it against
// the size here rejects a producer that miscounts — without it an
// *understated* count would silently drop a file's trailing entries
// (the merge reads to EOF) and corrupt the index with no error.
func scanAndValidate(inputs []string) (uint64, error) {
	var total uint64
	for _, path := range inputs {
		count, err := scanBinHeader(path)
		if err != nil {
			return 0, err
		}
		total += count
	}
	return total, nil
}

// scanBinHeader opens path, reads its declared entry count, and verifies
// the file is exactly binHeaderSize + count*binEntrySize bytes.
func scanBinHeader(path string) (uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("txhash: open %s: %w", path, err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("txhash: stat %s: %w", path, err)
	}
	var hdr [binHeaderSize]byte
	if _, err := io.ReadFull(f, hdr[:]); err != nil {
		return 0, fmt.Errorf("txhash: read header of %s: %w", path, err)
	}
	count := binary.LittleEndian.Uint64(hdr[:])

	want := uint64(binHeaderSize) + count*binEntrySize
	if size := fi.Size(); size < 0 || uint64(size) != want {
		return 0, fmt.Errorf("txhash: %s is %d bytes, want %d for declared count %d", path, fi.Size(), want, count)
	}
	return count, nil
}
