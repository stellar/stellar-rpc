package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/sirupsen/logrus"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash"
)

// cmdHotTxHashIngest benches per-ledger txhash ingestion into a fresh
// txhash.HotStore. For every ledger in the source chunk it times the
// full extract-then-write pipeline: walk the LedgerCloseMeta to pull
// out (txhash, ledgerSeq) tuples, then commit them via one
// HotStore.AddEntries call. AddEntries goes through rocksdb.Store
// with WriteOptions.Sync=true, so each call WAL-fsyncs before
// returning — same single-call durability semantics as
// hot-ledgers-ingest's AddLedgers path.
//
// Why extract is inside the timer: unlike ledger ingestion (where the
// cold reader hands AddLedgers raw bytes and no transformation is
// needed), txhash ingestion is inherently a decode-then-write
// pipeline. A live ingester can't write tx hashes without first
// extracting them, so excluding the decode would understate the real
// per-ledger latency.
//
// Hash extraction reuses bench_ingest_raw_txhash.go's two strategies
// behind --xdr-views (full LedgerCloseMeta decode vs zero-copy XDR
// view walk). Toggling it shifts how much of each timed sample is
// decode vs fsync; see the bench notes.
//
// Output: one CSV row per non-empty ledger (seq, ntx, latency_ns).
// Empty ledgers are skipped so the latency distribution stays clean.
// Summary stats are printed to stdout. Single chunk, single
// goroutine — matches hot-ledgers-ingest exactly; no --all/--workers.
func cmdHotTxHashIngest() {
	fs := flag.NewFlagSet("hot-txhash-ingest", flag.ExitOnError)
	coldDir := fs.String("cold-dir", "", "source cold-store dir (required)")
	chunk := fs.Uint("chunk", 0, "source chunk; bench ingests every tx in its ledgers (required)")
	hotDir := fs.String("hot-dir", "", "fresh txhash HotStore destination dir (required; must be empty or absent)")
	xdrViews := fs.Bool("xdr-views", false,
		"extract tx hashes via XDR views (zero-copy) instead of full LedgerCloseMeta decode")
	outDir := fs.String("out", "bench-out", "CSV output dir")
	_ = fs.Parse(os.Args[1:])

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)

	if *coldDir == "" {
		fatal(logger, "--cold-dir is required")
	}
	if *chunk == 0 {
		fatal(logger, "--chunk is required")
	}
	if *hotDir == "" {
		fatal(logger, "--hot-dir is required")
	}
	chunkID := uint32(*chunk)

	// Refuse to write into a non-empty dir — preserves the "fresh
	// ingestion" premise. Missing dir is fine; NewHotStore creates it.
	if entries, err := os.ReadDir(*hotDir); err == nil && len(entries) > 0 {
		fatal(logger, "--hot-dir=%s is not empty; pick a fresh path or remove its contents", *hotDir)
	}

	first := chunkFirstLedger(chunkID)
	last := chunkLastLedger(chunkID)

	src := packPath(*coldDir, chunkID)
	if _, err := os.Stat(src); err != nil {
		fatal(logger, "cold pack missing: %s: %v", src, err)
	}

	cold, err := ledger.NewColdStoreReader(src)
	if err != nil {
		fatal(logger, "NewColdStoreReader %s: %v", src, err)
	}
	defer cold.Close()

	if err := os.MkdirAll(filepath.Dir(*hotDir), 0o755); err != nil {
		fatal(logger, "mkdir parent of %s: %v", *hotDir, err)
	}
	hot, err := txhash.NewHotStore(*hotDir, logger)
	if err != nil {
		fatal(logger, "NewHotStore %s: %v", *hotDir, err)
	}
	defer hot.Close()

	csvF, csvPath, err := createCSV(*outDir, "hot-txhash-ingest",
		"seq,ntx,extract_ns,write_ns,latency_ns")
	if err != nil {
		fatal(logger, "%v", err)
	}
	defer csvF.Close()

	logger.Infof("hot-txhash-ingest cold=%s chunk=%d hot=%s seqs=[%d,%d] xdr-views=%v",
		*coldDir, chunkID, *hotDir, first, last, *xdrViews)

	// Pre-allocate a per-ledger entry buffer. Typical pubnet ledger
	// carries hundreds to low thousands of tx; 4096 is a generous
	// initial cap that avoids growth on the common path.
	buf := make([]txhash.Entry, 0, 4096)
	appendHash := func(seq uint32, hashBytes []byte) {
		var h [32]byte
		copy(h[:], hashBytes)
		buf = append(buf, txhash.Entry{Hash: h, LedgerSeq: seq})
	}

	var (
		durs         []time.Duration // total = extract + write, per ledger
		extractDurs  []time.Duration // extract only
		writeDurs    []time.Duration // AddEntries only
		totalTx      int64
		emptyLedgers int
	)
	wallStart := time.Now()
	for entry, iterErr := range cold.IterateLedgers(first, last) {
		if iterErr != nil {
			fatal(logger, "cold iterate at seq %d: %v", entry.Seq, iterErr)
		}

		// Timer covers the full per-ledger ingest cost: extract hashes
		// from the LCM bytes + AddEntries. Unlike hot-ledgers-ingest
		// (where the cold reader hands AddLedgers raw bytes and no
		// transformation is needed), txhash ingestion is inherently a
		// decode-then-write pipeline — timing just the write half would
		// understate the real cost a live ingester pays per ledger.
		// extractStart/writeStart further split the timed window so the
		// CSV exposes the decode/fsync share without summary inference.
		extractStart := time.Now()
		buf = buf[:0]
		if *xdrViews {
			if err := extractTxHashesView(entry.Bytes, entry.Seq, appendHash); err != nil {
				fatal(logger, "view extract seq %d: %v", entry.Seq, err)
			}
		} else {
			if err := extractTxHashesFull(entry.Bytes, entry.Seq, appendHash); err != nil {
				fatal(logger, "full decode seq %d: %v", entry.Seq, err)
			}
		}
		extractDur := time.Since(extractStart)

		if len(buf) == 0 {
			emptyLedgers++
			continue
		}

		writeStart := time.Now()
		if err := hot.AddEntries(buf); err != nil {
			fatal(logger, "AddEntries(seq=%d, n=%d): %v", entry.Seq, len(buf), err)
		}
		writeDur := time.Since(writeStart)
		total := extractDur + writeDur

		durs = append(durs, total)
		extractDurs = append(extractDurs, extractDur)
		writeDurs = append(writeDurs, writeDur)
		totalTx += int64(len(buf))
		fmt.Fprintf(csvF, "%d,%d,%d,%d,%d\n",
			entry.Seq, len(buf),
			extractDur.Nanoseconds(), writeDur.Nanoseconds(), total.Nanoseconds())
	}
	wall := time.Since(wallStart)

	if len(durs) == 0 {
		fatal(logger, "no tx hashes ingested (source chunk empty?)")
	}

	stats := computeStats(durs)
	extractStats := computeStats(extractDurs)
	writeStats := computeStats(writeDurs)
	fmt.Println()
	fmt.Printf("hot-txhash-ingest n=%-5d p50=%-9s p90=%-9s p99=%-9s max=%-9s ops/s=%.0f\n",
		stats.n,
		stats.p50.Round(time.Microsecond),
		stats.p90.Round(time.Microsecond),
		stats.p99.Round(time.Microsecond),
		stats.maxv.Round(time.Microsecond),
		stats.opsPerSec,
	)
	fmt.Printf("  extract                  p50=%-9s p90=%-9s p99=%-9s max=%-9s\n",
		extractStats.p50.Round(time.Microsecond),
		extractStats.p90.Round(time.Microsecond),
		extractStats.p99.Round(time.Microsecond),
		extractStats.maxv.Round(time.Microsecond),
	)
	fmt.Printf("  write (RocksDB+fsync)    p50=%-9s p90=%-9s p99=%-9s max=%-9s\n",
		writeStats.p50.Round(time.Microsecond),
		writeStats.p90.Round(time.Microsecond),
		writeStats.p99.Round(time.Microsecond),
		writeStats.maxv.Round(time.Microsecond),
	)

	// Two throughput numbers because they answer different questions:
	//   stats.total / totalTx  → throughput if every ledger's extract +
	//     write ran back-to-back with no cold-side iteration latency
	//     between calls — call it the "in-pipeline" rate
	//   wall / totalTx         → realized end-to-end throughput including
	//     the cold reader's per-ledger seek/decompress between iterations
	logger.Infof("ingested %d tx hashes across %d ledgers (%d empty)",
		totalTx, len(durs), emptyLedgers)
	logger.Infof("wall=%s (%.0f tx/s end-to-end)",
		wall.Round(time.Millisecond),
		float64(totalTx)/wall.Seconds(),
	)
	logger.Infof("in-pipeline time=%s (%.0f tx/s extract+write)",
		stats.total.Round(time.Millisecond),
		float64(totalTx)/stats.total.Seconds(),
	)
	logger.Infof("extract total=%s; write total=%s",
		extractStats.total.Round(time.Millisecond),
		writeStats.total.Round(time.Millisecond),
	)
	logger.Infof("wrote %s", csvPath)
}
