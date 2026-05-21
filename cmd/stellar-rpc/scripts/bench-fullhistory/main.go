// Benchmark harness for full-history reader performance.
//
// # Naming convention
//
// Read/query benches are split per tier as `cold-X` / `hot-X` because
// each tier's methodology is baked into the loop: cold evicts the
// packfile from OS page cache and opens a fresh ColdStoreReader per
// iter (no warmup); hot keeps a shared HotStore handle and runs an
// N-iter RocksDB block-cache warmup before timed iters. The two
// shapes can't share one --tier-flagged loop body without lying about
// what each tier costs in production, so they're separate commands.
//
// Ingest commands also stay split (cold-X-ingest / hot-X-ingest)
// because the operations are genuinely different (BSB → packfile vs
// packfile → RocksDB; single-call fsync vs two-phase MPHF build),
// not "two tiers of one bench."
//
// Read benches:
//
//	cold-ledgers   Cold-tier ledger reads. Random chunk + page-cache evict
//	               + fresh ColdStoreReader open per iter. --n and --workers
//	               are comma-lists for grid sweeps.
//	hot-ledgers    Hot-tier (RocksDB) ledger reads. One shared HotStore
//	               handle across workers; 100-iter block-cache warmup.
//	               --n and --workers are comma-lists.
//	cold-tx-page   Page of N transactions starting from a random in-chunk
//	               cursor against the cold tier (evict + open per iter).
//	               Per-iter CSV: cursor_seq, cursor_tx, n_ledgers,
//	               open_ns, fetch_ns, decode_ns, scan_ns, total_ns.
//	hot-tx-page    Same workload against the hot tier (shared handle +
//	               warmup). CSV minus open_ns.
//	cold-tx-hash   getTransaction(hash) end-to-end against cold tier.
//	               Per-iter CSV: hash, seq, open_ns, lookup_ns, fetch_ns,
//	               scan_ns, materialize_ns, total_ns.
//	               --xdr-views (default true) toggles scan+materialize
//	               between view path (slice Result/Meta from raw via
//	               .Raw()) and round-trip (lcm.UnmarshalBinary +
//	               db.ParseTransaction).
//	hot-tx-hash    Same shape on hot tier. CSV minus open_ns.
//	cold-events    eventstore.Query against the cold tier (open fresh
//	               ColdReader + evict three pack files per iter).
//	               Default: auto-generated corpus — scan the chunk
//	               once at startup to pick 15 high-volume terms, then
//	               each iter shuffles them into a K-filter partition
//	               (round-robin with category-collision recovery; see
//	               corpus.go). Reproducible from (chunk, seed). The
//	               legacy --queries <file> JSON corpus overrides
//	               this; see query_corpus.go.
//	hot-events     Same workload against the hot tier (shared HotStore
//	               reader + warmup). CSV minus open_ns.
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
//	cold-events-ingest   End-to-end cold-events.pack production from a local
//	                     cold ledger pack. Per-chunk timings: total,
//	                     read_blocked, lcm_decode, term_index, cold_append,
//	                     cold_finalize, ledgers, events. Models the backfill
//	                     path (events.NewMemBitmaps + per-event TermsFor);
//	                     no HotStore writes are involved.
//	hot-events-ingest    Per-ledger event ingestion into a fresh
//	                     eventstore.HotStore. IngestLedgerEvents is one
//	                     atomic RocksDB batch per ledger with sync=true.
//	                     Mirrors hot-txhash-ingest.
//	ingest-raw-txhash    Phase 1 of cold txhash MPHF build: decode every
//	                     cold pack, write per-chunk sorted (txhash[:16],
//	                     ledgerSeq) .bin files.
//	build-txhash-index   Phase 2 of cold txhash MPHF build: k-way merge
//	                     the .bin files from phase 1 into a streamhash
//	                     sorted index with payload=3, fingerprint=1, and
//	                     MinLedger embedded as user metadata.
//
// Setup commands (non-trivial work that isn't a bench):
//
//	seed-events  Populate event hot+cold stores for the cold-events /
//	             hot-events query benches. The underlying hot+cold
//	             ingest steps are also available standalone as
//	             hot-events-ingest / cold-events-ingest with timing
//	             breakdowns. seed-events still writes a term corpus
//	             JSON as a reference for hand-authoring -queries
//	             files for the query benches, but that file is no
//	             longer consumed automatically.
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

func chunkFirstLedger(c uint32) uint32 { return c*ledgersPerChunk + 2 }
func chunkLastLedger(c uint32) uint32  { return (c+1)*ledgersPerChunk + 1 }

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
	case "cold-tx-page":
		cmdColdTxPage()
	case "hot-tx-page":
		cmdHotTxPage()
	case "cold-tx-hash":
		cmdColdTxHash()
	case "hot-tx-hash":
		cmdHotTxHash()
	case "cold-events":
		cmdColdEvents()
	case "hot-events":
		cmdHotEvents()
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
	case "seed-events":
		cmdSeedEvents()
	case "cold-events-ingest":
		cmdColdEventsIngest()
	case "hot-events-ingest":
		cmdHotEventsIngest()
	default:
		fmt.Fprintln(os.Stderr, "unknown sub-command:", cmd)
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: bench-fullhistory <sub-command> [flags]

read benches (split per tier — methodology baked in):
  cold-ledgers           cold-tier ledger reads with page-cache eviction
                         + fresh open per iter; grid sweep over --n / --workers
  hot-ledgers            hot-tier ledger reads with shared HotStore handle
                         + 100-iter block-cache warmup; grid sweep
  cold-tx-page           page of N transactions from a cursor (cold-cache:
                         evict + fresh open per iter)
  hot-tx-page            page of N transactions (hot: shared handle + warmup)
  cold-tx-hash           getTransaction(hash) end-to-end (cold-cache); sub-phase
                         CSV columns; --xdr-views toggles view vs round-trip
  hot-tx-hash            getTransaction(hash) (hot: shared handle + warmup)
  cold-events            eventstore.Query against cold reader (per-iter fresh
                         open + page-cache evict); auto-corpus from chunk by
                         default; --queries <file> for hand-authored JSON
  hot-events             eventstore.Query against hot reader (shared + warmup);
                         same source options as cold-events

ingest benches:
  cold-ledgers-ingest    produce packfiles from BSB; per-packfile total +
                         writer-only latency
  hot-ledgers-ingest     ingest ledgers into a fresh HotStore (WAL-fsync per call)
  hot-txhash-ingest      ingest one ledger's tx hashes per AddEntries call
                         into a fresh txhash.HotStore
  cold-events-ingest     produce N cold-events.pack/index artifacts from local
                         cold ledger packs (backfill-shape: no HotStore);
                         per-chunk total, read-blocked, lcm-decode, term-index,
                         cold-append, cold-finalize
  hot-events-ingest      ingest one ledger's events per IngestLedgerEvents call
                         into a fresh eventstore.HotStore (WAL-fsync per call)
  ingest-raw-txhash      phase 1 of cold txhash MPHF build: extract per-chunk
                         sorted (txhash, ledgerSeq) .bin files
  build-txhash-index     phase 2: k-way merge .bin files into a streamhash
                         sorted index

setup commands:
  seed-events            populate hot+cold event stores + sample term corpus

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
