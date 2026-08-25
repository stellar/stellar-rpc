package txhash

// cold_index.go is the build half of the cold txhash pipeline:
// BuildColdIndex merges the per-chunk .bin files for one index — the
// DefaultChunksPerIndex consecutive chunks it covers — into a single
// streamhash MPHF. The merge is in cold_merge.go; the .bin on-disk format
// (header, entry layout, constants) is owned by cold_bin.go.
//
// The merge requires each file's entries pre-sorted ascending by the
// big-endian uint64 of their first 8 key bytes — the block order streamhash
// routes on (for the first 8 bytes this is identical to the lex key order
// WriteColdBin guarantees).

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"

	"github.com/stellar/streamhash"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

// BuildColdIndex builds one cold txhash index from inputs (the per-chunk
// .bin files for the index) into outputPath. [minLedger, maxLedger] is the
// index's ledger coverage: minLedger anchors the per-key payload (so the
// reader recovers absolute seqs) and every entry must fall within it
// (MinLedger/MaxLedger report it). The span must fit the 3-byte payload.
//
// The .bin files are k-way merged (cold_merge.go) and fed single-pass to
// streamhash — the keys are already the blinded routing keys, so the build
// feeds them verbatim (no keying here). The index secret written into the
// metadata is ADOPTED from the .bin headers (the secret the producer keyed
// the inputs with), not re-derived — an index can never disagree with its
// inputs' keying, and scanAndValidate rejects mixed-secret inputs. The block
// build uses runtime.NumCPU()/2 workers (~2.7x over single-threaded).
// Removes the partial output on error, and honors ctx cancellation.
func BuildColdIndex(
	ctx context.Context,
	inputs []string,
	outputPath string,
	minLedger, maxLedger uint32,
) (err error) {
	if maxLedger < minLedger {
		return fmt.Errorf("txhash: maxLedger %d < minLedger %d", maxLedger, minLedger)
	}
	if uint64(maxLedger-minLedger) > coldPayloadMax {
		return fmt.Errorf("txhash: coverage span %d exceeds %d-byte payload budget",
			maxLedger-minLedger, ColdPayloadSize)
	}

	total, secret, err := scanAndValidate(inputs)
	if err != nil {
		return err
	}

	buildOpts := append(
		[]streamhash.BuildOption{streamhash.WithWorkers(defaultBuildWorkers())},
		ColdBuildOptions(minLedger, maxLedger, secret)...)
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

	// Skip the merge for a zero-key coverage: the empty index comes from
	// Finish alone, and the merge has nothing to feed.
	if total > 0 {
		numLeaves := min(maxMergeLeaves(), len(inputs))
		m := newMerger(ctx)
		defer m.stop()
		finalCh, finalPool := m.buildMergeTree(inputs, numLeaves, mergeFileBufBytes)

		added, ferr := feedMergedKeys(builder, finalCh, finalPool, m, minLedger, maxLedger)
		if ferr != nil {
			return ferr
		}
		if added != total {
			return fmt.Errorf("txhash: key count mismatch: headers declared %d, merged %d", total, added)
		}
	}
	if ferr := builder.Finish(); ferr != nil {
		return fmt.Errorf("txhash: finalize cold index at %s: %w", outputPath, ferr)
	}
	return nil
}

// defaultBuildWorkers is the streamhash block-build parallelism. NumCPU/2 —
// see maxMergeLeaves for the joint (leaves, workers) sweep that picked it.
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
// against the file length (an understated count would otherwise silently
// drop a file's trailing entries — the merge reads to EOF), and returns the
// index secret the inputs were blinded with. Every input in a window must
// carry the SAME secret; a mismatch means the .bin files were keyed under
// different secrets (a catalog remint or geometry drift between ingest
// passes), so the build stops rather than silently producing an index no
// query can hit.
func scanAndValidate(inputs []string) (uint64, [stores.SecretLen]byte, error) {
	var total uint64
	var secret [stores.SecretLen]byte
	if len(inputs) == 0 {
		return 0, secret, errors.New("txhash: cold index build has no .bin inputs")
	}
	for i, path := range inputs {
		count, s, err := scanBinHeader(path)
		if err != nil {
			return 0, secret, err
		}
		if i == 0 {
			secret = s
		} else if s != secret {
			return 0, secret, fmt.Errorf(
				"txhash: %s is keyed under a different index secret than %s — refusing to build", path, inputs[0])
		}
		total += count
	}
	return total, secret, nil
}

// scanBinHeader opens path, reads its declared entry count and the index
// secret its keys were blinded with, and verifies its byte size matches the
// count via coldBinCount (the shared, overflow-safe header check).
func scanBinHeader(path string) (uint64, [stores.SecretLen]byte, error) {
	var secret [stores.SecretLen]byte
	f, err := os.Open(path)
	if err != nil {
		return 0, secret, fmt.Errorf("txhash: open %s: %w", path, err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return 0, secret, fmt.Errorf("txhash: stat %s: %w", path, err)
	}
	var hdr [coldBinHeaderSize]byte
	if _, err := io.ReadFull(f, hdr[:]); err != nil {
		return 0, secret, fmt.Errorf("txhash: read header of %s: %w", path, err)
	}
	copy(secret[:], hdr[coldBinCountSize:])
	count, err := coldBinCount(path, fi.Size(), binary.LittleEndian.Uint64(hdr[:coldBinCountSize]))
	return count, secret, err
}
