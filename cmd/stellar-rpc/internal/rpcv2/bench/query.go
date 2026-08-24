package bench

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/stellar/go-stellar-sdk/network"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
)

// NewQueryCommand returns the `bench-query` command tree: `cold` benchmarks
// reads served from frozen artifacts, `hot` reads served from a hot chunk
// database. Both measure through query.ReadView — the stable read seam — so a
// store-reader refactor moves the numbers without moving the benchmark.
func NewQueryCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "bench-query",
		Short: "Benchmark full-history reads",
	}
	cmd.AddCommand(newQueryColdCommand(), newQueryHotCommand())
	return cmd
}

// The query types --types selects, in the order the report lists them. Each
// names one read path through query.ReadView, and each is also a report CSV
// basename (see querySpecs).
const (
	// queryTypeLedgers: point reads and fixed-length range scans over
	// ReadView.ScanLedgers — getLedgers' path.
	queryTypeLedgers = "ledgers"
	// queryTypeTxPage: the paged ledger walk getTransactions performs,
	// extracting each ledger's transactions in sequence.
	queryTypeTxPage = "txpage"
	// queryTypeTxHash: the full by-hash lookup getTransaction performs — hot
	// indexes first, then the frozen window indexes with the MPHF candidate
	// verified against the ledger. Never an index probe alone.
	queryTypeTxHash = "txhash"
	// queryTypeEvents: ReadView.QueryEvents over a filter set derived from the
	// benchmarked chunk.
	queryTypeEvents = "events"
)

// allQueryTypes is every type --types accepts, in report order. It is also the
// list the campaign runner passes verbatim.
//
//nolint:gochecknoglobals // fixed vocabulary, read-only
var allQueryTypes = []string{queryTypeLedgers, queryTypeTxPage, queryTypeTxHash, queryTypeEvents}

// Query report row labels. Each per-type CSV carries one total_c<W> row per
// swept concurrency level — that cell's per-query distribution — and driver.csv
// carries the matching <qtype>_c<W> cell wall-clock plus the fixture open.
const (
	queryRowTotalPrefix = "total_c"
	driverQueryOpen     = "open"  // fixture open: catalog, handles, first read view
	driverQueryEvict    = "evict" // one page-cache eviction pass before a cold cell
)

// queryCellRow is a per-type CSV's row label for concurrency level w.
func queryCellRow(w int) string { return queryRowTotalPrefix + strconv.Itoa(w) }

// queryDriverRow is driver.csv's cell wall-clock row label for one query type
// at concurrency level w.
func queryDriverRow(qtype string, w int) string {
	return qtype + "_c" + strconv.Itoa(w)
}

// Defaults for the read-shape flags. The two span defaults are the v2 page caps
// for their endpoints, so a default run reads what a client asks for; the miss
// fraction matches the share of by-hash lookups a production node sees asking
// for a hash that never landed.
const (
	defaultLedgersSpan  = 20
	defaultTxPageSpan   = 5
	defaultTxPageLimit  = 200
	defaultEventsLimit  = 100
	defaultMissFraction = 0.12
	defaultSeed         = 1
)

// queryFlags is the sweep flag set both bench-query subcommands share, beyond
// the --out and profiling flags newBenchCommand binds. The spellings and value
// formats of --types, --query-concurrency, --iters, and --warmup are the
// campaign runner's argv contract; the read-shape flags after them are
// bench-side only, so the runner's argv keeps working untouched.
type queryFlags struct {
	types       string
	concurrency string
	iters       int
	warmup      int
	warmupBound bool // bind --warmup (hot only; a cold cell evicts instead of warming)

	ledgersSpan  uint32
	txPageSpan   uint32
	txPageLimit  int
	eventsLimit  int
	missFraction float64
	passphrase   string
	seed         int64
}

func (f *queryFlags) bind(cmd *cobra.Command) {
	fs := cmd.Flags()
	fs.StringVar(&f.types, "types", strings.Join(allQueryTypes, ","),
		"comma-separated query types to sweep: "+strings.Join(allQueryTypes, " | "))
	fs.StringVar(&f.concurrency, "query-concurrency", "1",
		"comma-separated reader concurrency levels to sweep, e.g. 1,4,16")
	fs.IntVar(&f.iters, "iters", f.iters, "measured queries per type per concurrency level")
	if f.warmupBound {
		fs.IntVar(&f.warmup, "warmup", f.warmup,
			"unmeasured queries per cell before the measured ones, warming the store's caches")
	}
	fs.Uint32Var(&f.ledgersSpan, "ledgers-span", defaultLedgersSpan,
		"ledgers one ledgers query scans (1 = a point read)")
	fs.Uint32Var(&f.txPageSpan, "txpage-span", defaultTxPageSpan,
		"ledgers one txpage query walks")
	fs.IntVar(&f.txPageLimit, "txpage-limit", defaultTxPageLimit,
		"transactions one txpage query materializes before it stops, as a page cap")
	fs.IntVar(&f.eventsLimit, "events-limit", defaultEventsLimit,
		"events one events page may return")
	fs.Float64Var(&f.missFraction, "miss-fraction", defaultMissFraction,
		"share of txhash lookups asking for a hash that never landed, in [0, 1] "+
			"(a miss probes every index, so it is the path's worst case)")
	fs.StringVar(&f.passphrase, "network-passphrase", network.PublicNetworkPassphrase,
		"network passphrase the dataset's transactions were signed under; txhash and "+
			"txpage need it to pair envelopes, and a wrong one fails the corpus build")
	fs.Int64Var(&f.seed, "seed", defaultSeed,
		"seed for the work each query picks, so a re-run reads the same ledgers")
}

// queryPlan is the validated sweep: which types, at which concurrency levels,
// how many queries per cell, and the shape of each query.
type queryPlan struct {
	Types       []string
	Concurrency []int
	Iters       int
	Warmup      int

	LedgersSpan  uint32
	TxPageSpan   uint32
	TxPageLimit  int
	EventsLimit  int
	MissFraction float64
	Passphrase   string
	Seed         int64

	// Evict drops the cold artifacts from the OS page cache before each cell's
	// measured pass. Cold only: the hot tier's steady state is a warm cache, so
	// its cells warm up instead.
	Evict bool
}

// plan parses and validates the sweep flags.
func (f *queryFlags) plan() (queryPlan, error) {
	types, err := parseQueryTypes(f.types)
	if err != nil {
		return queryPlan{}, err
	}
	concurrency, err := parseConcurrency(f.concurrency)
	if err != nil {
		return queryPlan{}, err
	}
	switch {
	case f.iters < 1:
		return queryPlan{}, fmt.Errorf("--iters must be >= 1, got %d", f.iters)
	case f.warmup < 0:
		return queryPlan{}, fmt.Errorf("--warmup must be >= 0, got %d", f.warmup)
	case f.ledgersSpan < 1:
		return queryPlan{}, fmt.Errorf("--ledgers-span must be >= 1, got %d", f.ledgersSpan)
	case f.txPageSpan < 1:
		return queryPlan{}, fmt.Errorf("--txpage-span must be >= 1, got %d", f.txPageSpan)
	case f.txPageLimit < 1:
		return queryPlan{}, fmt.Errorf("--txpage-limit must be >= 1, got %d", f.txPageLimit)
	case f.eventsLimit < 1:
		return queryPlan{}, fmt.Errorf("--events-limit must be >= 1, got %d", f.eventsLimit)
	case f.missFraction < 0 || f.missFraction > 1:
		return queryPlan{}, fmt.Errorf("--miss-fraction must be in [0, 1], got %v", f.missFraction)
	case f.passphrase == "":
		return queryPlan{}, errors.New("--network-passphrase is required")
	}
	return queryPlan{
		Types:        types,
		Concurrency:  concurrency,
		Iters:        f.iters,
		Warmup:       f.warmup,
		LedgersSpan:  f.ledgersSpan,
		TxPageSpan:   f.txPageSpan,
		TxPageLimit:  f.txPageLimit,
		EventsLimit:  f.eventsLimit,
		MissFraction: f.missFraction,
		Passphrase:   f.passphrase,
		Seed:         f.seed,
	}, nil
}

// parseQueryTypes splits --types, keeping the caller's order and rejecting an
// empty list, an unknown type, or a repeat (a repeated type would collide on
// its CSV rows).
func parseQueryTypes(s string) ([]string, error) {
	fields := strings.Split(s, ",")
	types := make([]string, 0, len(fields))
	for _, f := range fields {
		qtype := strings.TrimSpace(f)
		if qtype == "" {
			return nil, fmt.Errorf("--types has an empty entry: %q", s)
		}
		if !slices.Contains(allQueryTypes, qtype) {
			return nil, fmt.Errorf("--types: unknown query type %q (want %s)",
				qtype, strings.Join(allQueryTypes, " | "))
		}
		if slices.Contains(types, qtype) {
			return nil, fmt.Errorf("--types repeats %q", qtype)
		}
		types = append(types, qtype)
	}
	return types, nil
}

// parseConcurrency splits --query-concurrency into the reader counts to sweep,
// rejecting an empty list, a non-integer, a level below 1, or a repeat.
func parseConcurrency(s string) ([]int, error) {
	fields := strings.Split(s, ",")
	levels := make([]int, 0, len(fields))
	for _, f := range fields {
		w, err := strconv.Atoi(strings.TrimSpace(f))
		if err != nil {
			return nil, fmt.Errorf("--query-concurrency: %q is not a list of integers", s)
		}
		if w < 1 {
			return nil, fmt.Errorf("--query-concurrency levels must be >= 1, got %d", w)
		}
		if slices.Contains(levels, w) {
			return nil, fmt.Errorf("--query-concurrency repeats %d", w)
		}
		levels = append(levels, w)
	}
	return levels, nil
}

// queryFixture is the read side of one bench-query run, assembled over a
// dataset an earlier bench-ingest run left on disk: the scratch catalog whose
// keys make that dataset servable, the registry holding the hot handles, and
// the ledger range the per-type corpora may sample from.
//
// Both tiers build one so the measured code path is the daemon's: every query
// takes a read view, and every read view resolves its tier through
// ReadView.resolveTier. Neither fixture can serve the other's tier — the cold
// one publishes no hot handle, the hot one freezes no artifact — so a resolve
// that succeeds proves the intended tier served it.
type queryFixture struct {
	registry *query.Registry

	// Passphrase is the network the dataset's transactions were signed under.
	// Materializing a transaction pairs its envelope by hash, so txpage and
	// txhash both need it and a wrong one makes every lookup a miss.
	Passphrase string

	// Chunks is the benchmarked chunk range, ascending.
	Chunks []chunk.ID

	// FirstLedger and LastLedger bound the ledgers the corpora may sample:
	// the chunk range's span for a cold fixture, and for a hot one what the hot
	// database actually holds, narrowed further by --sample-ledgers.
	FirstLedger, LastLedger uint32

	// EvictPaths are the on-disk artifacts a cold cell drops from the page cache
	// before it measures. Empty for a hot fixture, whose data lives in RocksDB's
	// own caches rather than in files the bench can advise on.
	EvictPaths []string
}

// view acquires one read view. Every measured query takes its own, as a served
// request does; the caller MUST Release it.
func (f *queryFixture) view() (*query.ReadView, error) {
	return f.registry.NewReadView()
}

// verifyServes acquires a read view and resolves each benchmarked chunk's
// ledger store, so a dataset that cannot be served fails at open with the
// chunk named — not per-query, deep inside the sweep. It runs the real routing
// (ReadView.Ledgers → resolveTier), which is also what makes it a check: the
// fixture publishes state for one tier only.
func (f *queryFixture) verifyServes() error {
	view, err := f.view()
	if err != nil {
		return fmt.Errorf("acquire read view: %w", err)
	}
	defer view.Release()
	for _, c := range f.Chunks {
		if _, err := view.Ledgers(c); err != nil {
			return fmt.Errorf("chunk %s has no servable ledger store: %w", c, err)
		}
	}
	return nil
}

// evictColdArtifacts drops the fixture's cold artifacts from the OS page cache
// and reports how many files it advised. Without it a cold cell after the first
// reads what the previous cell just paged in, and the sweep would show a warming
// curve rather than a concurrency curve.
//
// A file that cannot be opened is skipped rather than failing the run: the
// artifact set is derived from the layout, so a kind the dataset never produced
// is a legitimate absence.
func (f *queryFixture) evictColdArtifacts() (int, error) {
	evicted := 0
	for _, path := range f.EvictPaths {
		if err := evictFile(path); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return evicted, fmt.Errorf("evict %s from the page cache: %w", path, err)
		}
		evicted++
	}
	return evicted, nil
}

// runQuerySweep sweeps every requested type at every concurrency level against
// the fixture, recording each cell's per-query distribution in the sink.
//
// A type's corpus is sampled once, before its first cell, and shared by every
// concurrency level: re-sampling per level would make the levels read different
// work and turn the concurrency curve into noise.
func runQuerySweep(
	ctx context.Context, logger *supportlog.Entry, f *queryFixture, p queryPlan, sink *csvSink,
) error {
	for _, qtype := range p.Types {
		req, err := newQueryRequest(ctx, logger, f, p, qtype)
		if err != nil {
			return fmt.Errorf("prepare the %s benchmark: %w", qtype, err)
		}
		for _, w := range p.Concurrency {
			if err := runQueryCell(logger, f, p, sink, qtype, w, req); err != nil {
				return fmt.Errorf("query %s at concurrency %d: %w", qtype, w, err)
			}
			if err := ctx.Err(); err != nil {
				return err
			}
		}
	}
	return nil
}

// runQueryCell runs one (type, concurrency) cell: page-cache eviction for a
// cold run, then p.Warmup unmeasured requests and p.Iters measured ones on each
// of w workers.
//
// The cell's seed mixes in the type so two types do not read the same ledgers in
// the same order, which would let the second inherit the first's warm cache.
func runQueryCell(
	logger *supportlog.Entry, f *queryFixture, p queryPlan, sink *csvSink,
	qtype string, w int, req queryRequest,
) error {
	if p.Evict {
		start := time.Now()
		evicted, err := f.evictColdArtifacts()
		if err != nil {
			return err
		}
		sink.observe(fileDriver, driverQueryEvict, time.Since(start), evicted)
	}
	logger.Infof("query %s at concurrency %d: %d iters, %d warmup", qtype, w, p.Iters, p.Warmup)

	res := runSweep(w, p.Warmup, p.Iters, p.Seed+int64(len(qtype)), req)
	if res.errs > 0 {
		return fmt.Errorf("%d of %d requests failed", res.errs, w*p.Iters)
	}
	recordCell(sink, qtype, w, res)
	return nil
}

// recordCell files one cell's samples into the report.
//
// Every sample lands in the type's total_c<W> row — the blended cell the results
// converter reads — and a sample carrying a sub-stage additionally lands in a
// <stage>_c<W> row of the same file. The converter ignores those extra rows
// (it matches total_c<W> alone), so they cost the site nothing and give the CSV
// and the log summary the split that matters locally: txhash's found and
// not-found lookups do different amounts of work, and a blended p99 cannot say
// which of them moved.
func recordCell(sink *csvSink, qtype string, w int, res sweepResult) {
	total := queryCellRow(w)
	for _, s := range res.samples {
		sink.observe(qtype, total, s.d, s.items)
		if s.stage != "" {
			sink.observe(qtype, s.stage+"_c"+strconv.Itoa(w), s.d, s.items)
		}
	}
	sink.observe(fileDriver, queryDriverRow(qtype, w), res.wall, len(res.samples))
}

// runQueryBench is the body both bench-query subcommands share: prepare --out,
// open the tier's fixture (timing the open into driver.csv), sweep, and report.
// A failed sweep still writes the partial report and the peak RSS, so the setup
// rows and whatever cells completed survive.
func runQueryBench(
	ctx context.Context, logger *supportlog.Entry, p queryPlan, outDir string,
	open func() (*queryFixture, func(), error),
) error {
	// Surface an unwritable --out before opening the dataset.
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return fmt.Errorf("create --out dir %s: %w", outDir, err)
	}
	sink := newSchemaCSVSink(querySpecs(p.Types, p.Concurrency))

	start := time.Now()
	f, release, err := open()
	if err != nil {
		return err
	}
	defer release()
	sink.observe(fileDriver, driverQueryOpen, time.Since(start), len(f.Chunks))
	logger.Infof("serving ledgers [%d, %d] over %d chunk(s)", f.FirstLedger, f.LastLedger, len(f.Chunks))

	err = runQuerySweep(ctx, logger, f, p, sink)
	// VmHWM never decreases, so it can be read before the error check and a
	// failed run's partial CSV still gets the row.
	recordPeakRSS(logger, sink, readPeakRSS)
	if err != nil {
		writePartialCSVs(logger, sink, outDir)
		return err
	}

	sink.logSummary(logger)
	written, err := sink.writeCSVs(outDir)
	if err != nil {
		return err
	}
	logger.Infof("wrote %d CSVs to %s", len(written), outDir)
	return nil
}

// chunkRange returns the ascending chunk IDs in [start, start+num). The caller's
// validate() proved start+num-1 stays within maxChunkID.
func chunkRange(start chunk.ID, num int) []chunk.ID {
	chunks := make([]chunk.ID, 0, num)
	for i := range uint32(num) { //nolint:gosec // num >= 1, bounded by validate()
		chunks = append(chunks, start+chunk.ID(i))
	}
	return chunks
}
