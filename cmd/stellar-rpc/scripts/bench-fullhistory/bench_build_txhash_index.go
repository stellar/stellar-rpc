package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/tamirms/streamhash"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash"
)

// cmdBuildTxHashIndex is phase 2 of the cold txhash index build.
//
// Reads every *.bin file produced by ingest-raw-txhash in --in-dir,
// k-way merges them in sorted order via the streamhash bench merge
// primitives in streamhash_merge.go, and feeds the sorted stream
// into streamhash.NewSortedBuilder configured with the cold txhash
// option set (payload=3, fingerprint=1, user-metadata=MinLedger).
//
// MinLedger is auto-derived from the smallest chunk ID present in
// --in-dir: the per-chunk file at <id>.bin covers ledgers in
// [chunkFirstLedger(id), chunkLastLedger(id)], so the minimum
// ledger across the whole input is chunkFirstLedger(minChunkID).
// That value gets embedded in the .idx user metadata so the reader
// can recover absolute seqs without any sidecar metadata.
func cmdBuildTxHashIndex() {
	fs := flag.NewFlagSet("build-txhash-index", flag.ExitOnError)
	inDir := fs.String("in-dir", "", "directory with *.bin files from ingest-raw-txhash (required)")
	out := fs.String("out", "", "output .idx path (required)")
	workers := fs.Int("workers", max(1, runtime.NumCPU()/2), "streamhash parallel block-build workers")
	mergers := fs.Int("mergers", 32, "leaf merge goroutines")
	bufsize := fs.Int("bufsize", 32768, "per-file read buffer (bytes)")
	_ = fs.Parse(os.Args[1:])

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)

	validateBuildTxHashFlags(logger, *inDir, *out, *workers, *mergers, *bufsize)

	files, minLedger, totalKeys := discoverBuildInputs(logger, *inDir)
	if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
		fatal(logger, "mkdir output dir: %v", err)
	}

	logger.Infof("build-txhash-index in-dir=%s files=%d totalKeys=%d minLedger=%d workers=%d mergers=%d",
		*inDir, len(files), totalKeys, minLedger, *workers, *mergers)

	opts := append(txhash.ColdBuildOptions(minLedger), streamhash.WithWorkers(*workers))
	sb, err := streamhash.NewSortedBuilder(context.Background(), *out, totalKeys, opts...)
	if err != nil {
		fatal(logger, "NewSortedBuilder: %v", err)
	}

	feedStart := time.Now()
	added, err := feedSortedFromBinFiles(sb, files, *bufsize, *mergers, minLedger)
	feedElapsed := time.Since(feedStart)
	if err != nil {
		_ = sb.Close()
		fatal(logger, "build: %v", err)
	}
	if added != totalKeys {
		_ = sb.Close()
		fatal(logger, "key count mismatch: scanned %d, added %d", totalKeys, added)
	}
	finishStart := time.Now()
	if err := sb.Finish(); err != nil {
		_ = sb.Close()
		fatal(logger, "Finish: %v", err)
	}
	finishElapsed := time.Since(finishStart)
	total := feedElapsed + finishElapsed

	info, _ := os.Stat(*out)
	var size int64
	if info != nil {
		size = info.Size()
	}
	logger.Infof("built %d keys in %s total (feed=%s, finish=%s); %.0f keys/s",
		added,
		total.Round(time.Millisecond),
		feedElapsed.Round(time.Millisecond),
		finishElapsed.Round(time.Millisecond),
		float64(added)/total.Seconds(),
	)
	logger.Infof("index size %d bytes (%.2f bits/key)",
		size, float64(size*8)/float64(added),
	)
}

// validateBuildTxHashFlags enforces the required-flag invariants for
// cmdBuildTxHashIndex. Calls fatal on the first violation.
func validateBuildTxHashFlags(
	logger *supportlog.Entry,
	inDir, out string,
	workers, mergers, bufsize int,
) {
	if inDir == "" {
		fatal(logger, "--in-dir is required")
	}
	if out == "" {
		fatal(logger, "--out is required")
	}
	if workers < 1 {
		fatal(logger, "--workers must be >= 1")
	}
	if mergers < 1 {
		fatal(logger, "--mergers must be >= 1")
	}
	if bufsize < benchEntrySize {
		fatal(logger, "--bufsize must be >= %d (entry size)", benchEntrySize)
	}
}

// discoverBuildInputs globs the .bin files in inDir, derives the
// minimum ledger from the lowest-numbered chunk filename, and sums
// the file headers to compute the total key count. Calls fatal if
// any step fails or yields zero work.
func discoverBuildInputs(logger *supportlog.Entry, inDir string) ([]string, uint32, uint64) {
	files, err := filepath.Glob(filepath.Join(inDir, "*.bin"))
	if err != nil {
		fatal(logger, "glob %s: %v", inDir, err)
	}
	if len(files) == 0 {
		fatal(logger, "no .bin files under %s", inDir)
	}
	sort.Strings(files)

	minChunkID, err := chunkIDFromBinFilename(filepath.Base(files[0]))
	if err != nil {
		fatal(logger, "derive min chunk: %v", err)
	}
	minLedger := chunkFirstLedger(minChunkID)

	totalKeys, err := scanHeaders(files)
	if err != nil {
		fatal(logger, "scan headers: %v", err)
	}
	if totalKeys == 0 {
		fatal(logger, "no entries across %d files; refusing to build empty index", len(files))
	}
	return files, minLedger, totalKeys
}

// feedSortedFromBinFiles assembles the merge tree from
// streamhash_merge.go and feeds the sorted entry stream into the
// SortedBuilder. The tree construction is a near-verbatim port of
// streamhash cmd/bench/bench_files.go:buildSorted (commit
// ca41413750cb). The only deviation is the payload transform:
// each entry's 4-byte LE absolute seq is converted to a uint64
// offset (seq - minLedger), which streamhash packs into the
// 3-byte payload slot per ColdBuildOptions.
func feedSortedFromBinFiles(
	builder *streamhash.SortedBuilder,
	files []string,
	bufsize, numMergers int,
	minLedger uint32,
) (uint64, error) {
	G := max(numMergers, 1)
	filesPerGroup := (len(files) + G - 1) / G
	var streams []*streamReader
	for i := 0; i < len(files); i += filesPerGroup {
		end := min(i+filesPerGroup, len(files))
		streams = append(streams, launchMergeStream(files[i:end], bufsize))
	}

	for len(streams) > maxFanIn {
		var nextLevel []*streamReader
		for i := 0; i < len(streams); i += maxFanIn {
			end := min(i+maxFanIn, len(streams))
			group := streams[i:end]
			if len(group) == 1 {
				nextLevel = append(nextLevel, group[0])
			} else {
				nextLevel = append(nextLevel, launchFinalMerge(group))
			}
		}
		streams = nextLevel
	}

	finalCh := make(chan *mergeBatch, 2)
	finalPool := make(chan *mergeBatch, 3)
	for range 3 {
		finalPool <- &mergeBatch{}
	}
	go finalMerge(streams, finalCh, finalPool)

	var keysAdded uint64
	for batch := range finalCh {
		data := batch.data[:batch.count*benchEntrySize]
		for off := 0; off < len(data); off += benchEntrySize {
			entry := data[off : off+benchEntrySize]
			absSeq := binary.LittleEndian.Uint32(entry[keySize:])
			if absSeq < minLedger {
				return keysAdded, fmt.Errorf("entry seq %d below minLedger %d", absSeq, minLedger)
			}
			payload := uint64(absSeq - minLedger)
			if err := builder.AddKey(entry[:keySize], payload); err != nil {
				return keysAdded, fmt.Errorf("AddKey: %w", err)
			}
			keysAdded++
		}
		finalPool <- batch
	}
	return keysAdded, nil
}

// chunkIDFromBinFilename parses "00005900.bin" → 5900. The filename
// convention is set by ingest-raw-txhash.
func chunkIDFromBinFilename(name string) (uint32, error) {
	base := strings.TrimSuffix(name, ".bin")
	id, err := strconv.ParseUint(base, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("filename %q not <chunkID>.bin: %w", name, err)
	}
	return uint32(id), nil
}
