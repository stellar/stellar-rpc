package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"iter"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	goxdr "github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/zstd"
)

// cmdIngestRawTxHash is phase 1 of the cold txhash index build.
//
// For each selected cold packfile it decodes every LedgerCloseMeta,
// collects (txhash[:16], absoluteLedgerSeq) pairs, sorts them in
// memory by big-endian uint64 prefix (the order streamhash's
// SortedBuilder expects), and writes one .bin file per chunk:
//
//	<out-dir>/<chunkID:08d>.bin
//
// File format (matches streamhash bench's entry-file format so the
// downstream merge code in streamhash_merge.go reads it directly):
//
//	header  uint64 LE  entry count
//	entry   16 bytes    txhash[:16]
//	        uint32 LE   absolute ledger seq (build phase subtracts MinLedger)
//
// Chunks are processed in parallel — one chunk per goroutine, up to
// --workers concurrent.
func cmdIngestRawTxHash() {
	fs := flag.NewFlagSet("ingest-raw-txhash", flag.ExitOnError)
	coldDir := fs.String("cold-dir", "", "cold packfile root (required)")
	outDir := fs.String("out-dir", "", "directory for per-chunk .bin files (required)")
	chunk := fs.Int64("chunk", -1, "single chunk ID to extract; mutually exclusive with --all")
	all := fs.Bool("all", false, "extract every chunk in --cold-dir")
	workers := fs.Int("workers", runtime.NumCPU(), "parallel chunk-extraction goroutines")
	xdrViews := fs.Bool("xdr-views", false, "extract tx hashes via XDR views (zero-copy slicing) instead of full LedgerCloseMeta decode")
	cpuProfile := fs.String("cpuprofile", "", "write CPU profile to this path (overall run, not per-chunk)")
	_ = fs.Parse(os.Args[1:])

	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "cpuprofile: create: %v\n", err)
			os.Exit(1)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			fmt.Fprintf(os.Stderr, "cpuprofile: start: %v\n", err)
			os.Exit(1)
		}
		defer pprof.StopCPUProfile()
	}

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)

	if *coldDir == "" {
		fatal(logger, "--cold-dir is required")
	}
	if *outDir == "" {
		fatal(logger, "--out-dir is required")
	}
	if !*all && *chunk < 0 {
		fatal(logger, "either --chunk=N or --all is required")
	}
	if *all && *chunk >= 0 {
		fatal(logger, "--chunk and --all are mutually exclusive")
	}
	if *workers < 1 {
		fatal(logger, "--workers must be >= 1")
	}

	chunks, err := selectChunksForExtract(*coldDir, *chunk, *all)
	if err != nil {
		fatal(logger, "select chunks: %v", err)
	}
	if len(chunks) == 0 {
		fatal(logger, "no chunks found under %s", *coldDir)
	}

	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		fatal(logger, "mkdir %s: %v", *outDir, err)
	}

	logger.Infof("ingest-raw-txhash cold-dir=%s out-dir=%s chunks=%d workers=%d xdr-views=%v",
		*coldDir, *outDir, len(chunks), *workers, *xdrViews)

	work := make(chan uint32, len(chunks))
	for _, c := range chunks {
		work <- c
	}
	close(work)

	type chunkResult struct {
		entries int
		elapsed time.Duration
	}
	results := make(chan chunkResult, len(chunks))

	var (
		wg           sync.WaitGroup
		totalEntries atomic.Int64
		errOnce      sync.Once
		firstErr     error
	)
	start := time.Now()
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			dec := zstd.NewDecompressor()
			for chunkID := range work {
				t0 := time.Now()
				n, perr := extractChunk(*coldDir, *outDir, chunkID, dec, *xdrViews)
				d := time.Since(t0)
				if perr != nil {
					errOnce.Do(func() { firstErr = fmt.Errorf("chunk %d: %w", chunkID, perr) })
					return
				}
				totalEntries.Add(int64(n))
				results <- chunkResult{entries: n, elapsed: d}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect per-chunk timings as they complete so the final summary
	// can report distribution stats (p50/p99/etc) in addition to the
	// aggregate wall time.
	perChunkDurs := make([]time.Duration, 0, len(chunks))
	for r := range results {
		perChunkDurs = append(perChunkDurs, r.elapsed)
	}
	if firstErr != nil {
		fatal(logger, "%v", firstErr)
	}

	wall := time.Since(start)
	stats := computeStats(perChunkDurs)
	logger.Infof("extracted %d entries from %d chunks in %s wall (%.0f entries/s aggregate)",
		totalEntries.Load(), len(chunks),
		wall.Round(time.Millisecond),
		float64(totalEntries.Load())/wall.Seconds(),
	)
	logger.Infof("per-chunk: n=%d p50=%s p90=%s p99=%s max=%s mean=%s",
		stats.n,
		stats.p50.Round(time.Millisecond),
		stats.p90.Round(time.Millisecond),
		stats.p99.Round(time.Millisecond),
		stats.maxv.Round(time.Millisecond),
		(stats.total / time.Duration(stats.n)).Round(time.Millisecond),
	)
}

// selectChunksForExtract resolves the chunk IDs to process.
func selectChunksForExtract(coldDir string, single int64, all bool) ([]uint32, error) {
	if !all {
		return []uint32{uint32(single)}, nil
	}
	matches, err := filepath.Glob(filepath.Join(coldDir, "*", "*.pack"))
	if err != nil {
		return nil, fmt.Errorf("glob %s: %w", coldDir, err)
	}
	chunks := make([]uint32, 0, len(matches))
	for _, m := range matches {
		base := strings.TrimSuffix(filepath.Base(m), ".pack")
		id, perr := strconv.ParseUint(base, 10, 32)
		if perr != nil {
			continue
		}
		chunks = append(chunks, uint32(id))
	}
	sort.Slice(chunks, func(i, j int) bool { return chunks[i] < chunks[j] })
	return chunks, nil
}

// extractChunk decodes one cold pack, sorts (txhash[:16], absSeq)
// pairs, and writes them to <outDir>/<chunkID:08d>.bin. Returns the
// number of entries written. Overwrites any existing file at the
// output path.
//
// When useXDRViews is true, transaction hashes are extracted by
// walking the LedgerCloseMeta as a view (zero-copy byte-slice
// navigation in the SDK's xdr_views_generated.go) instead of a full
// XDR decode into a LedgerCloseMeta struct. View mode skips
// materialization of fee/apply processing fields the hash extraction
// doesn't need.
func extractChunk(coldDir, outDir string, chunkID uint32, dec *zstd.Decompressor, useXDRViews bool) (int, error) {
	packPathStr := packPath(coldDir, chunkID)
	binPath := filepath.Join(outDir, fmt.Sprintf("%08d.bin", chunkID))

	r, err := ledger.NewColdStoreReader(packPathStr, dec)
	if err != nil {
		return 0, fmt.Errorf("open %s: %w", packPathStr, err)
	}
	defer r.Close()

	first, err := r.FirstSeq()
	if err != nil {
		return 0, fmt.Errorf("FirstSeq: %w", err)
	}
	last, err := r.LastSeq()
	if err != nil {
		return 0, fmt.Errorf("LastSeq: %w", err)
	}

	// Collect all (key, seq) pairs into a flat slice so we can sort
	// in-memory once and stream out. At ~2M tx/chunk × 20 B per entry
	// ≈ 40 MB peak — fits trivially.
	type entry struct {
		key [keySize]byte
		seq uint32
	}
	var entries []entry
	appendHash := func(seq uint32, hashBytes []byte) {
		var e entry
		copy(e.key[:], hashBytes[:keySize])
		e.seq = seq
		entries = append(entries, e)
	}
	for ledgerEntry, iterErr := range r.IterateLedgers(first, last) {
		if iterErr != nil {
			return 0, fmt.Errorf("iterate seq %d: %w", ledgerEntry.Seq, iterErr)
		}
		if useXDRViews {
			if err := extractTxHashesView(ledgerEntry.Bytes, ledgerEntry.Seq, appendHash); err != nil {
				return 0, fmt.Errorf("view extract seq %d: %w", ledgerEntry.Seq, err)
			}
		} else {
			if err := extractTxHashesFull(ledgerEntry.Bytes, ledgerEntry.Seq, appendHash); err != nil {
				return 0, fmt.Errorf("full decode seq %d: %w", ledgerEntry.Seq, err)
			}
		}
	}

	// Sort by lex byte order on the full 16-byte key — equivalent to
	// streamhash's big-endian uint64 prefix comparison (which is just
	// the first 8 bytes byte-wise, with the remaining 8 as natural
	// tiebreak).
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].key[:], entries[j].key[:]) < 0
	})

	f, err := os.Create(binPath)
	if err != nil {
		return 0, fmt.Errorf("create %s: %w", binPath, err)
	}
	defer f.Close()

	bw := bufio.NewWriterSize(f, 1<<20)
	var header [8]byte
	binary.LittleEndian.PutUint64(header[:], uint64(len(entries)))
	if _, err := bw.Write(header[:]); err != nil {
		return 0, fmt.Errorf("write header: %w", err)
	}

	var entryBuf [benchEntrySize]byte
	for _, e := range entries {
		copy(entryBuf[:keySize], e.key[:])
		binary.LittleEndian.PutUint32(entryBuf[keySize:], e.seq)
		if _, err := bw.Write(entryBuf[:]); err != nil {
			return 0, fmt.Errorf("write entry: %w", err)
		}
	}
	if err := bw.Flush(); err != nil {
		return 0, fmt.Errorf("flush: %w", err)
	}
	return len(entries), nil
}

// extractTxHashesFull is the baseline strategy: XDR-decode the raw
// LedgerCloseMeta into a struct, then read the precomputed transaction
// hashes from its TxProcessing[i].Result.TransactionHash field. Easy
// to read; materializes the full ledger including fee/apply metadata
// we don't need.
func extractTxHashesFull(rawLCM []byte, seq uint32, emit func(seq uint32, hash []byte)) error {
	var lcm goxdr.LedgerCloseMeta
	if err := lcm.UnmarshalBinary(rawLCM); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}
	nTx := lcm.CountTransactions()
	for i := 0; i < nTx; i++ {
		h := lcm.TransactionHash(i)
		emit(seq, h[:])
	}
	return nil
}

// txResultMeta is satisfied by both TransactionResultMetaView (V0/V1
// LCM) and TransactionResultMetaV1View (V2 LCM) — the SDK gives them
// distinct types because their trailing fields differ, but both put
// Result first.
type txResultMeta interface {
	Result() (goxdr.TransactionResultPairView, error)
}

// extractTxHashesView walks the LedgerCloseMeta as an XDR view and
// emits each TransactionResultPair's TransactionHash. Same bytes as
// extractTxHashesFull, no full struct decode. Hash bytes alias rawLCM
// — caller's emit must copy before retaining.
func extractTxHashesView(rawLCM []byte, seq uint32, emit func(seq uint32, hash []byte)) error {
	v := goxdr.LedgerCloseMetaView(rawLCM)
	dv, err := v.V()
	if err != nil {
		return err
	}
	disc, err := dv.Value()
	if err != nil {
		return err
	}
	switch disc {
	case 0:
		v0, err := v.V0()
		if err != nil {
			return err
		}
		tp, err := v0.TxProcessing()
		if err != nil {
			return err
		}
		return emitHashesView(tp.Iter(), seq, emit)
	case 1:
		v1, err := v.V1()
		if err != nil {
			return err
		}
		tp, err := v1.TxProcessing()
		if err != nil {
			return err
		}
		return emitHashesView(tp.Iter(), seq, emit)
	case 2:
		v2, err := v.V2()
		if err != nil {
			return err
		}
		tp, err := v2.TxProcessing()
		if err != nil {
			return err
		}
		return emitHashesView(tp.Iter(), seq, emit)
	default:
		return fmt.Errorf("unknown LedgerCloseMeta V=%d", disc)
	}
}

// emitHashesView iterates one LCM version's TxProcessing array and
// emits each TransactionHash. Generic over T because the SDK names
// the per-tx element type differently for V0/V1 vs V2.
func emitHashesView[T txResultMeta](
	src iter.Seq2[T, error],
	seq uint32,
	emit func(seq uint32, hash []byte),
) error {
	for tx, iterErr := range src {
		if iterErr != nil {
			return iterErr
		}
		rp, err := tx.Result()
		if err != nil {
			return err
		}
		hv, err := rp.TransactionHash()
		if err != nil {
			return err
		}
		b, err := hv.Value()
		if err != nil {
			return err
		}
		emit(seq, b)
	}
	return nil
}

