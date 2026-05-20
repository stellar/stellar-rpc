package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/sirupsen/logrus"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	goxdr "github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/eventstore"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
)

// cmdHotEventsIngest benches per-ledger event ingestion into a fresh
// eventstore.HotStore. For every ledger in the source chunk it times
// the full extract-then-write pipeline: unmarshal the LedgerCloseMeta,
// run events.LCMToPayloads to produce payloads, then commit them via
// one HotStore.IngestLedgerEvents call. IngestLedgerEvents writes the
// per-event payloads + per-term index entries into RocksDB inside a
// single atomic batch (sync=true), then updates the in-memory bitmap
// mirror — same single-call durability semantics as
// hot-ledgers-ingest's AddLedgers and hot-txhash-ingest's AddEntries.
//
// Why extract is inside the timer: events ingestion is inherently a
// decode-then-write pipeline. A live ingester can't write events
// without first deriving them from the LedgerCloseMeta, so excluding
// the decode would understate the real per-ledger latency.
//
// Output: one CSV row per non-empty ledger (seq, n_events, extract_ns,
// write_ns, latency_ns). Empty ledgers (no events) are skipped so the
// latency distribution stays clean. Summary stats are printed to
// stdout. Single chunk, single goroutine — matches hot-ledgers-ingest
// / hot-txhash-ingest exactly.
func cmdHotEventsIngest() {
	fs := flag.NewFlagSet("hot-events-ingest", flag.ExitOnError)
	coldDir := fs.String("cold-dir", "", "source cold-store dir (required)")
	chunkFlag := fs.Int64("chunk", -1, "source chunk; bench ingests every event in its ledgers (required)")
	hotDir := fs.String("hot-events-dir", "",
		"fresh events HotStore destination dir (required; must be empty or absent)")
	outDir := fs.String("out", "bench-out", "CSV output dir")
	_ = fs.Parse(os.Args[1:])

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)

	if *coldDir == "" {
		fatal(logger, "--cold-dir is required")
	}
	if *chunkFlag < 0 {
		fatal(logger, "--chunk is required")
	}
	if *chunkFlag > math.MaxUint32 {
		fatal(logger, "--chunk=%d exceeds uint32", *chunkFlag)
	}
	if *hotDir == "" {
		fatal(logger, "--hot-events-dir is required")
	}
	chunkID := chunk.ID(uint32(*chunkFlag))

	// Refuse to write into a non-empty dir — preserves the "fresh
	// ingestion" premise. Missing dir is fine; OpenHotStore creates it.
	if entries, err := os.ReadDir(*hotDir); err == nil && len(entries) > 0 {
		fatal(logger, "--hot-events-dir=%s is not empty; pick a fresh path or remove its contents", *hotDir)
	}

	first := chunkID.FirstLedger()
	last := chunkID.LastLedger()

	src := packPath(*coldDir, uint32(chunkID))
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
	hot, err := eventstore.OpenHotStore(*hotDir, chunkID, logger)
	if err != nil {
		fatal(logger, "OpenHotStore %s: %v", *hotDir, err)
	}
	defer hot.Close()

	csvF, csvPath, err := createCSV(*outDir, "hot-events-ingest",
		"seq,n_events,extract_ns,write_ns,latency_ns")
	if err != nil {
		fatal(logger, "%v", err)
	}
	defer csvF.Close()

	logger.Infof("hot-events-ingest cold=%s chunk=%d hot=%s seqs=[%d,%d]",
		*coldDir, uint32(chunkID), *hotDir, first, last)

	var (
		durs         []time.Duration
		extractDurs  []time.Duration
		writeDurs    []time.Duration
		totalEvents  int64
		emptyLedgers int
	)
	wallStart := time.Now()
	for entry, iterErr := range cold.IterateLedgers(first, last) {
		if iterErr != nil {
			fatal(logger, "cold iterate at seq %d: %v", entry.Seq, iterErr)
		}

		// extractStart/writeStart split the timed window so the CSV
		// exposes the decode/fsync share without summary inference.
		// LCMToPayloads internally re-derives transaction hashes against
		// the passphrase, so the passphrase is required for events to
		// surface their TxHash field correctly.
		extractStart := time.Now()
		var lcm goxdr.LedgerCloseMeta
		if err := lcm.UnmarshalBinary(entry.Bytes); err != nil {
			fatal(logger, "unmarshal seq %d: %v", entry.Seq, err)
		}
		payloads, lerr := events.LCMToPayloads(PubnetPassphrase, lcm)
		if lerr != nil {
			fatal(logger, "LCMToPayloads seq %d: %v", entry.Seq, lerr)
		}
		extractDur := time.Since(extractStart)

		if len(payloads) == 0 {
			emptyLedgers++
			continue
		}

		writeStart := time.Now()
		if err := hot.IngestLedgerEvents(entry.Seq, payloads); err != nil {
			fatal(logger, "IngestLedgerEvents(seq=%d, n=%d): %v", entry.Seq, len(payloads), err)
		}
		writeDur := time.Since(writeStart)
		total := extractDur + writeDur

		durs = append(durs, total)
		extractDurs = append(extractDurs, extractDur)
		writeDurs = append(writeDurs, writeDur)
		totalEvents += int64(len(payloads))
		fmt.Fprintf(csvF, "%d,%d,%d,%d,%d\n",
			entry.Seq, len(payloads),
			extractDur.Nanoseconds(), writeDur.Nanoseconds(), total.Nanoseconds())
	}
	wall := time.Since(wallStart)

	if len(durs) == 0 {
		fatal(logger, "no events ingested (source chunk empty?)")
	}

	stats := computeStats(durs)
	extractStats := computeStats(extractDurs)
	writeStats := computeStats(writeDurs)
	fmt.Println()
	fmt.Printf("hot-events-ingest n=%-5d p50=%-9s p90=%-9s p99=%-9s max=%-9s ops/s=%.0f\n",
		stats.n,
		stats.p50.Round(time.Microsecond),
		stats.p90.Round(time.Microsecond),
		stats.p99.Round(time.Microsecond),
		stats.maxv.Round(time.Microsecond),
		stats.opsPerSec,
	)
	fmt.Printf("  extract (LCM + LCMToPayloads) p50=%-9s p90=%-9s p99=%-9s max=%-9s\n",
		extractStats.p50.Round(time.Microsecond),
		extractStats.p90.Round(time.Microsecond),
		extractStats.p99.Round(time.Microsecond),
		extractStats.maxv.Round(time.Microsecond),
	)
	fmt.Printf("  write (RocksDB batch+fsync)   p50=%-9s p90=%-9s p99=%-9s max=%-9s\n",
		writeStats.p50.Round(time.Microsecond),
		writeStats.p90.Round(time.Microsecond),
		writeStats.p99.Round(time.Microsecond),
		writeStats.maxv.Round(time.Microsecond),
	)

	// Two throughput numbers (same shape as hot-txhash-ingest):
	//   stats.total / totalEvents → in-pipeline rate (back-to-back work,
	//     excludes cold-reader iteration overhead between ledgers)
	//   wall / totalEvents        → end-to-end rate including cold-side
	//     decompress + seek time
	logger.Infof("ingested %d events across %d ledgers (%d empty)",
		totalEvents, len(durs), emptyLedgers)
	logger.Infof("wall=%s (%.0f events/s end-to-end)",
		wall.Round(time.Millisecond),
		float64(totalEvents)/wall.Seconds(),
	)
	logger.Infof("in-pipeline time=%s (%.0f events/s extract+write)",
		stats.total.Round(time.Millisecond),
		float64(totalEvents)/stats.total.Seconds(),
	)
	logger.Infof("extract total=%s; write total=%s",
		extractStats.total.Round(time.Millisecond),
		writeStats.total.Round(time.Millisecond),
	)
	logger.Infof("wrote %s", csvPath)
}
