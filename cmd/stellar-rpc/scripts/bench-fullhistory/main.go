// Benchmark harness for full-history reader performance.
//
// # Naming convention
//
// Read/query benches are split per tier as `cold-X` / `hot-X` because
// each tier's methodology is baked into the loop: cold evicts the
// packfile from OS page cache and opens a fresh ColdReader per
// iter (no warmup); hot keeps a shared HotStore handle and runs an
// N-iter RocksDB block-cache warmup before timed iters. The two
// shapes can't share one --tier-flagged loop body without lying about
// what each tier costs in production, so they're separate commands.
//
// Ingest benches are unified across data types: one hot-ingest and
// one cold-ingest command, each with --types= selecting any subset
// of {ledgers, txhash, events}. The driver streams from a
// ledgerbackend.LedgerStream (--source=pack reads a local cold
// packfile via packStream; --source=bsb reads from a GCS-backed
// buffered-storage stream) and fans the per-ledger bytes out to each
// enabled type's writer. build-txhash-index stays separate (phase 2
// of the cold txhash MPHF build doesn't fit the per-chunk loop).
//
// Read benches:
//
//	cold-ledgers   Cold-tier ledger reads. Random chunk + page-cache evict
//	               + fresh ColdReader open per iter. --n is single-valued
//	               (production page size); --query-concurrency is a comma-list
//	               concurrency sweep.
//	hot-ledgers    Hot-tier (RocksDB) ledger reads. One shared HotStore
//	               handle across workers; 100-iter block-cache warmup.
//	               --n single-valued; --query-concurrency comma-list sweep.
//	cold-txpage   Page of N transactions from a random cursor against
//	               the cold tier (evict + open per iter). Multi-chunk:
//	               picks a random chunk from --cold-dir per iter.
//	               Per-iter CSV: workers, chunk, cursor_seq, cursor_tx,
//	               n_ledgers, open_ns, fetch_ns, decode_ns, scan_ns,
//	               total_ns.
//	hot-txpage    Same workload against the hot tier (shared handle +
//	               warmup). CSV minus open_ns.
//	cold-txhash   getTransaction(hash) end-to-end against cold tier.
//	               Multi-chunk hash pool; MPHF opened once + shared
//	               across workers; per-iter evicts only the resolved
//	               chunk's pack. Per-iter CSV: workers, chunk, hash, seq,
//	               is_miss, lookup_ns, pack_open_ns, fetch_ns, scan_ns,
//	               materialize_ns, total_ns.
//	               --xdr-views toggles scan+materialize between view
//	               path (slice Result/Meta from raw via .Raw()) and
//	               round-trip (lcm.UnmarshalBinary + db.ParseTransaction).
//	hot-txhash    Same shape on hot tier (single-chunk, shared stores +
//	               warmup). CSV minus pack_open_ns.
//	cold-events    eventstore.Query against the cold tier. Multi-chunk:
//	               per-chunk corpora built at startup; per-iter pick a
//	               random chunk + evict its three pack files + open
//	               fresh ColdReader. Auto-generated corpus: one-shot
//	               scan picks 3 highest-volume contracts + top 12
//	               (position, value) topic terms over those contracts'
//	               4-topic events; each iter shuffles the 15-term
//	               universe into a K-filter partition (round-robin
//	               with category-collision recovery; see corpus.go).
//	               Reproducible from (chunk, seed).
//	hot-events     Same workload against the hot tier (shared HotStore
//	               reader + warmup). CSV minus open_ns.
//
// All read benches accept --query-concurrency=1,4,16,... as a comma-list and
// emit one summary CSV row per worker count plus per-iter detail
// rows (workers column included so cells can be filtered after the
// fact). Cold benches also accept optional --chunk-lo/--chunk-hi
// to constrain the chunk range; default is auto-discover from the
// cold-dir.
//
// Ingest benches:
//
//	hot-ingest           Single-chunk hot-store ingest. --types= picks any
//	                     subset of {ledgers,txhash,events}; --source=
//	                     pack|bsb selects local cold-pack vs BSB-from-GCS;
//	                     --xdr-views toggles view vs parsed extract;
//	                     --parallel runs ingesters concurrently per ledger.
//	                     Emits one aggregation CSV per data type + one
//	                     driver CSV (columns: stage, n, n_items, total_ns,
//	                     p50_ns, p90_ns, p99_ns, max_ns).
//	cold-ingest          Single-chunk cold-tier producer. --types= picks
//	                     any subset of {ledgers,events,txhash}; same
//	                     --source/--xdr-views/--parallel knobs as
//	                     hot-ingest. Per-type packfile tuning
//	                     (--ledgers-packfile-*, --events-packfile-*).
//	                     txhash emits phase-1 .bin files only;
//	                     build-txhash-index produces the .idx.
//	build-txhash-index   Phase 2 of cold txhash MPHF build: k-way merge
//	                     the .bin files from cold-ingest --types=txhash
//	                     into a streamhash sorted index with payload=3,
//	                     fingerprint=1, MinLedger embedded as user
//	                     metadata.
//
// Per-stage aggregates are summarized to <out-dir>/<bench>.csv; the
// summary block is printed to stdout.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
)

// fatal logs the formatted message at error level and exits with status 1.
// All bench sub-commands use this for any unrecoverable error.
//
//nolint:goprintffuncname // 180 callsites already use "fatal"; rename to fatalf would be churn for a stylistic nit
func fatal(logger *supportlog.Entry, format string, args ...any) {
	logger.Errorf(format, args...)
	os.Exit(1)
}

const (
	ledgersPerChunk uint32 = 10_000
	chunksPerBucket uint32 = 1_000
)

// pubnetPassphrase is the network passphrase the ingest benches use
// when decoding events from cold ledger packs. Stellar uses the
// passphrase as a hash domain for transaction IDs; the value must
// match the network the packs came from. Mainnet ("Public Global
// Stellar Network ; September 2015") covers every chunk the
// benches currently target.
const pubnetPassphrase = "Public Global Stellar Network ; September 2015"

// intListString formats a []int as "[1,2,3]" instead of the default
// `%v` slice formatting's space-separated form. Used in startup
// source labels (see bench_cold_events.go, bench_hot_events.go).
func intListString(xs []int) string {
	parts := make([]string, len(xs))
	for i, x := range xs {
		parts[i] = strconv.Itoa(x)
	}
	return "[" + strings.Join(parts, ",") + "]"
}

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
	case "cold-txpage":
		cmdColdTxPage()
	case "hot-txpage":
		cmdHotTxPage()
	case "cold-txhash":
		cmdColdTxHash()
	case "hot-txhash":
		cmdHotTxHash()
	case "cold-events":
		cmdColdEvents()
	case "hot-events":
		cmdHotEvents()
	case "hot-ingest":
		os.Exit(cmdHotIngest())
	case "cold-ingest":
		os.Exit(cmdColdIngest())
	case "build-txhash-index":
		cmdBuildTxHashIndex()
	default:
		fmt.Fprintln(os.Stderr, "unknown sub-command:", cmd)
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: bench-fullhistory <sub-command> [flags]

read benches (split per tier — methodology baked in; all do a 1D
--query-concurrency comma-list concurrency sweep):
  cold-ledgers           cold-tier ledger reads with page-cache eviction
                         + fresh open per iter; --n single-valued
  hot-ledgers            hot-tier ledger reads with shared HotStore handle
                         + 100-iter block-cache warmup; --n single-valued
  cold-txpage           multi-chunk: page of N tx from a random cursor
                         (cold-cache: evict + fresh open per iter)
  hot-txpage            page of N tx (hot: shared handle + warmup)
  cold-txhash           multi-chunk: getTransaction(hash) end-to-end
                         (cold-cache, MPHF opened once + shared); sub-phase
                         CSV; --xdr-views toggles view vs round-trip
  hot-txhash            getTransaction(hash) (hot: shared handle + warmup)
  cold-events            multi-chunk: eventstore.Query (per-iter fresh
                         open + 3-file evict on the picked chunk); auto-
                         corpus per chunk (one-shot scan + round-robin
                         K-filter partition per iter)
  hot-events             eventstore.Query against hot reader (shared + warmup);
                         same auto-corpus shape as cold-events

Cold benches accept optional --chunk-lo/--chunk-hi to constrain
which chunks to draw from; default auto-discovers from cold-dir.

ingest benches:
  hot-ingest             unified hot-store ingest. --types= picks any subset
                         of {ledgers,txhash,events}; --source=pack|bsb;
                         --xdr-views toggles view vs parsed extract;
                         --parallel runs ingesters concurrently per ledger.
                         Single chunk per run. Emits one aggregation CSV per
                         data type + one driver CSV (stage,n,n_items,
                         total_ns,p50/p90/p99/max).
  cold-ingest            unified cold-tier producer. --types= picks any subset
                         of {ledgers,events,txhash}; --source=pack|bsb;
                         same --xdr-views / --parallel knobs as hot-ingest.
                         Per-type packfile tuning (--ledgers-packfile-*,
                         --events-packfile-*). txhash writes phase-1 .bin
                         files atomically; feed to build-txhash-index for
                         phase 2.
  build-txhash-index     phase 2 of cold txhash MPHF build: k-way merge the
                         .bin files from cold-ingest --types=txhash into a
                         streamhash sorted index.

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
	slices.Sort(durs)
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

// line renders a sub-summary stdout line for a slice of latencies.
// Used for per-class, per-hit/miss splits inside one concurrent-sweep
// cell. Omits ops/s on purpose: computeStats fills opsPerSec from
// sum-of-latencies/total, which is meaningless for parallel runs
// (the durations overlap in wall time). The cell-level wall-clock
// ops/s is printed once by printSweepRow.
func (s latencyStats) line(label string) string {
	return fmt.Sprintf(
		"%-30s n=%-5d p50=%-9s p90=%-9s p95=%-9s p99=%-9s max=%-9s",
		label, s.n,
		s.p50.Round(time.Microsecond),
		s.p90.Round(time.Microsecond),
		s.p95.Round(time.Microsecond),
		s.p99.Round(time.Microsecond),
		s.maxv.Round(time.Microsecond),
	)
}
