package txhash

// cold_index.go is the build half of the cold txhash pipeline:
// BuildColdIndex merges the per-chunk .bin files for one index — the
// DefaultChunksPerIndex consecutive chunks it covers — into a single
// streamhash MPHF. The merge is in cold_merge.go.
//
// .bin input format, one file per chunk named <chunkID:08d>.bin:
//
//	 8 bytes  entry count  (uint64 LE)
//	then, per 20-byte entry:
//	16 bytes  txhash[:16]  (the key; streamhash routes on this)
//	 4 bytes  ledger seq   (uint32 LE, absolute)
//
// Entries within a file must be sorted ascending by the big-endian uint64
// of their first 8 key bytes (the block order streamhash requires).

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
	// binKeySize is the key width: streamhash routes on the first 16 bytes,
	// so the store keys on the 16-byte prefix (Get still takes 32).
	binKeySize    = 16
	binSeqSize    = 4
	binEntrySize  = binKeySize + binSeqSize
	binHeaderSize = 8
)

// ErrEmptyBuildSet is returned by BuildColdIndex when inputs hold no
// entries (streamhash can't build an index over zero keys).
var ErrEmptyBuildSet = errors.New("txhash: cannot build a cold index with zero keys")

// BuildColdIndex builds one cold txhash index from inputs (the per-chunk
// .bin files for the index) into outputPath. [minLedger, maxLedger] is the
// index's ledger coverage: minLedger anchors the per-key payload (so the
// reader recovers absolute seqs) and every entry must fall within it
// (MinLedger/MaxLedger report it). The span must fit the 3-byte payload.
//
// The .bin files are k-way merged (cold_merge.go) and fed single-pass to
// streamhash. By default the block build uses runtime.NumCPU()/2 workers
// (~2.7x over single-threaded); caller opts override. Returns
// ErrEmptyBuildSet for empty inputs, removes the partial output on error,
// and honors ctx cancellation.
func BuildColdIndex(
	ctx context.Context,
	inputs []string,
	outputPath string,
	minLedger, maxLedger uint32,
	opts ...streamhash.BuildOption,
) (err error) {
	if len(inputs) == 0 {
		return ErrEmptyBuildSet
	}
	if maxLedger < minLedger {
		return fmt.Errorf("txhash: maxLedger %d < minLedger %d", maxLedger, minLedger)
	}
	if uint64(maxLedger-minLedger) > coldPayloadMax {
		return fmt.Errorf("txhash: coverage span %d exceeds %d-byte payload budget",
			maxLedger-minLedger, ColdPayloadSize)
	}

	total, err := scanAndValidate(inputs)
	if err != nil {
		return err
	}
	if total == 0 {
		return ErrEmptyBuildSet
	}

	// The cold format options go last so they win: a caller can override the
	// default WithWorkers (its opt precedes the format ones, which don't set
	// workers) but cannot change the pinned payload/fingerprint/metadata.
	buildOpts := make([]streamhash.BuildOption, 0, len(opts)+4)
	buildOpts = append(buildOpts, streamhash.WithWorkers(defaultBuildWorkers()))
	buildOpts = append(buildOpts, opts...)
	buildOpts = append(buildOpts, ColdBuildOptions(minLedger, maxLedger)...)
	builder, berr := streamhash.NewSortedBuilder(ctx, outputPath, total, buildOpts...)
	if berr != nil {
		return fmt.Errorf("txhash: create cold index builder at %s: %w", outputPath, berr)
	}
	// Close removes the partial output on error and is a no-op after Finish;
	// don't let its error mask a real one.
	defer func() {
		if cerr := builder.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("txhash: close cold index builder: %w", cerr)
		}
	}()

	numLeaves := min(maxMergeLeaves(), len(inputs))
	m := newMerger(ctx)
	defer m.stop()
	finalCh, finalPool := m.buildMergeTree(inputs, numLeaves, mergeFileBufBytes)

	added, err := feedMergedKeys(builder, finalCh, finalPool, m, minLedger, maxLedger)
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

// defaultBuildWorkers is the streamhash block-build parallelism used when
// the caller doesn't override it. NumCPU/2 — see maxMergeLeaves for the
// joint (leaves, workers) sweep that picked it.
func defaultBuildWorkers() int {
	return max(1, runtime.NumCPU()/2)
}

// maxMergeLeaves caps the leaf merge goroutines (= peak concurrent O_DIRECT
// reads). NumCPU/2 pairs with defaultBuildWorkers (also NumCPU/2): together
// they fill NumCPU cores without oversubscription, which a cold Linux NVMe
// sweep over 382M real keys found is the joint end-to-end optimum — the
// builder (the e2e gate) saturates at NumCPU/2 workers, and more leaves only
// steal its cores (~+18% e2e at NumCPU/2 vs NumCPU). Capped at the file count.
func maxMergeLeaves() int {
	return max(1, runtime.NumCPU()/2)
}

// scanAndValidate sums the per-file header counts, cross-checking each
// against the file length: an understated count would otherwise silently
// drop a file's trailing entries (the merge reads to EOF).
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
