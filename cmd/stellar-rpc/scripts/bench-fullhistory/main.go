// Benchmark harness for full-history reader performance.
//
// Read benches:
//
//	cold-ledgers   Cold-tier ledger reads. Random chunk + page-cache evict
//	               + fresh open per iter. --n and --workers are comma-lists
//	               for grid sweeps.
//	hot-ledgers    Hot-tier (RocksDB) ledger reads. One shared HotStore
//	               handle across workers; hardcoded 100-iter block-cache
//	               warmup. --n and --workers are comma-lists.
//	tx-page        Page of N transactions from a cursor; --tier, --page-size.
//	tx-hash        Tx-by-hash end-to-end: hash → seq lookup → ledger fetch →
//	               scan ledger for the hash. --tier=hot|cold-mphf. Hash pool
//	               is sampled from the cold packfile at startup; no external
//	               corpus file.
//	events         Events filter scenarios across hot/cold (needs seed-events
//	               + build-cold-events-index setup first).
//
// Ingest benches:
//
//	cold-ledgers-ingest  End-to-end packfile production from BSB. Reports
//	                     per-packfile total latency (with BSB) and
//	                     writer-only latency (excluding GetLedgerRaw waits).
//	hot-ledgers-ingest   Per-ledger ingestion into a fresh HotStore.
//	                     AddLedgers single-entry path = Store.Put with
//	                     SetSync=true, i.e. WAL-fsync per ledger.
//	hot-txhash-ingest    Per-ledger txhash ingestion into a fresh
//	                     txhash.HotStore. AddEntries fsyncs once per
//	                     ledger; --xdr-views toggles extraction strategy.
//	                     Also serves as the setup step for tx-hash
//	                     --tier=hot — ~60 s per chunk with --xdr-views.
//	ingest-raw-txhash    Phase 1 of cold txhash MPHF build: decode every
//	                     cold pack, write per-chunk sorted (txhash[:16],
//	                     ledgerSeq) .bin files in --out-dir.
//	build-txhash-index   Phase 2 of cold txhash MPHF build: k-way merge
//	                     the .bin files from phase 1 into a streamhash
//	                     sorted index with payload=3, fingerprint=1, and
//	                     MinLedger embedded as user metadata.
//
// Setup commands (non-trivial work that isn't a bench):
//
//	seed-events             Sample term corpus + populate event hot+cold
//	                        stores for the `events` bench.
//	build-cold-events-index Finalize cold event index (called after
//	                        seed-events).
//
// Per-iteration latencies are summarized to <out-dir>/<bench>.csv; the
// summary line is printed to stdout.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
)

// fatal logs the formatted message at error level and exits with status 1.
// All bench sub-commands use this for any unrecoverable error.
func fatal(logger *supportlog.Entry, format string, args ...interface{}) {
	logger.Errorf(format, args...)
	os.Exit(1)
}

const (
	ledgersPerChunk uint32 = 10_000
	chunksPerBucket uint32 = 1_000
)

func chunkIDForLedger(seq uint32) uint32 { return (seq - 2) / ledgersPerChunk }
func chunkFirstLedger(c uint32) uint32   { return c*ledgersPerChunk + 2 }
func chunkLastLedger(c uint32) uint32    { return (c+1)*ledgersPerChunk + 1 }

func packPath(coldDir string, c uint32) string {
	return filepath.Join(
		coldDir,
		fmt.Sprintf("%05d", c/chunksPerBucket),
		fmt.Sprintf("%08d.pack", c),
	)
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd := os.Args[1]
	os.Args = append([]string{os.Args[0]}, os.Args[2:]...)

	switch cmd {
	case "cold-ledgers":
		cmdColdLedgers()
	case "hot-ledgers":
		cmdHotLedgers()
	case "cold-ledgers-ingest":
		cmdColdLedgersIngest()
	case "hot-ledgers-ingest":
		cmdHotLedgersIngest()
	case "hot-txhash-ingest":
		cmdHotTxHashIngest()
	case "ingest-raw-txhash":
		cmdIngestRawTxHash()
	case "build-txhash-index":
		cmdBuildTxHashIndex()
	case "tx-page":
		cmdTxPage()
	case "tx-hash":
		cmdTxHash()
	case "seed-events":
		cmdSeedEvents()
	case "build-cold-events-index":
		cmdBuildColdEventsIndex()
	case "events":
		cmdEventsBench()
	default:
		fmt.Fprintln(os.Stderr, "unknown sub-command:", cmd)
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: bench-fullhistory <sub-command> [flags]

sub-commands:
  cold-ledgers                   cold-tier ledger reads with page-cache eviction
                                 + fresh open per iter; --n/--workers are
                                 comma-lists for grid sweeps
  hot-ledgers                    hot-tier (RocksDB) ledger reads; one shared
                                 HotStore handle across workers; --n/--workers
                                 are comma-lists
  cold-ledgers-ingest            produce packfiles from BSB; reports per-packfile
                                 total + writer-only latency (excluding BSB waits)
  hot-ledgers-ingest             ingest ledgers one-at-a-time into a fresh
                                 HotStore; reports per-ledger latency with
                                 WAL-fsync per call
  hot-txhash-ingest              ingest one ledger's tx hashes per AddEntries
                                 call into a fresh txhash.HotStore; reports
                                 per-ledger latency with WAL-fsync per call
  ingest-raw-txhash              phase 1 of cold txhash MPHF build: extract
                                 per-chunk sorted (txhash, ledgerSeq) .bin files
  build-txhash-index             phase 2 of cold txhash MPHF build: k-way merge
                                 .bin files into a streamhash sorted index
  tx-page                        bench page of N transactions
  tx-hash                        bench tx-by-hash end-to-end (--tier=hot|cold-mphf);
                                 samples hashes from the cold packfile at startup
  seed-events                    setup for events: populate hot+cold event
                                 stores + sample term corpus
  build-cold-events-index        finalize cold event index (run after seed-events)
  events                         bench events filter scenarios across hot/cold

run "<sub-command> -h" for per-command flags`)
}

// latencyStats holds percentile + throughput summary.
type latencyStats struct {
	n         int
	p50       time.Duration
	p90       time.Duration
	p95       time.Duration
	p99       time.Duration
	maxv      time.Duration
	total     time.Duration
	opsPerSec float64
}

func computeStats(durs []time.Duration) latencyStats {
	if len(durs) == 0 {
		return latencyStats{}
	}
	sort.Slice(durs, func(i, j int) bool { return durs[i] < durs[j] })
	var total time.Duration
	for _, d := range durs {
		total += d
	}
	pick := func(p float64) time.Duration {
		i := int(p * float64(len(durs)))
		if i >= len(durs) {
			i = len(durs) - 1
		}
		return durs[i]
	}
	return latencyStats{
		n:         len(durs),
		p50:       pick(0.50),
		p90:       pick(0.90),
		p95:       pick(0.95),
		p99:       pick(0.99),
		maxv:      durs[len(durs)-1],
		total:     total,
		opsPerSec: float64(len(durs)) / total.Seconds(),
	}
}

func (s latencyStats) line(label string) string {
	return fmt.Sprintf(
		"%-30s n=%-5d p50=%-9s p90=%-9s p95=%-9s p99=%-9s max=%-9s ops/s=%.0f",
		label, s.n,
		s.p50.Round(time.Microsecond),
		s.p90.Round(time.Microsecond),
		s.p95.Round(time.Microsecond),
		s.p99.Round(time.Microsecond),
		s.maxv.Round(time.Microsecond),
		s.opsPerSec,
	)
}

func writeCSV(path string, durs []time.Duration) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := fmt.Fprintln(f, "iteration_ns"); err != nil {
		return err
	}
	for _, d := range durs {
		if _, err := fmt.Fprintf(f, "%d\n", d.Nanoseconds()); err != nil {
			return err
		}
	}
	return nil
}
