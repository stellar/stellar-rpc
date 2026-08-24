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
	driverQueryOpen     = "open" // fixture open: catalog, handles, first read view
)

// queryCellRow is a per-type CSV's row label for concurrency level w.
func queryCellRow(w int) string { return queryRowTotalPrefix + strconv.Itoa(w) }

// queryDriverRow is driver.csv's cell wall-clock row label for one query type
// at concurrency level w.
func queryDriverRow(qtype string, w int) string {
	return qtype + "_c" + strconv.Itoa(w)
}

// errQueryTypeUnimplemented is what a sweep cell returns until #856 B2 fills in
// the per-type bodies. It is deliberately loud: a run reaches it only after the
// flag surface, the fixture, and the report schema have all been exercised, and
// the PARTIAL report it leaves behind carries the setup rows.
var errQueryTypeUnimplemented = errors.New("query bench body not implemented")

// queryFlags is the sweep flag set both bench-query subcommands share, beyond
// the --out and profiling flags newBenchCommand binds. The spellings and value
// formats are the campaign runner's argv contract: --types is a comma-separated
// type list, --query-concurrency a comma-separated reader-count list.
type queryFlags struct {
	types       string
	concurrency string
	iters       int
	warmup      int
	warmupBound bool // bind --warmup (hot only; a cold cell evicts instead of warming)
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
}

// queryPlan is the validated sweep: which types, at which concurrency levels,
// for how many queries per cell.
type queryPlan struct {
	Types       []string
	Concurrency []int
	Iters       int
	Warmup      int
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
	if f.iters < 1 {
		return queryPlan{}, fmt.Errorf("--iters must be >= 1, got %d", f.iters)
	}
	if f.warmup < 0 {
		return queryPlan{}, fmt.Errorf("--warmup must be >= 0, got %d", f.warmup)
	}
	return queryPlan{Types: types, Concurrency: concurrency, Iters: f.iters, Warmup: f.warmup}, nil
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

	// Chunks is the benchmarked chunk range, ascending.
	Chunks []chunk.ID

	// FirstLedger and LastLedger bound the ledgers the corpora may sample:
	// the chunk range's span for a cold fixture, and for a hot one what the hot
	// database actually holds, narrowed further by --sample-ledgers.
	FirstLedger, LastLedger uint32
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

// runQuerySweep sweeps every requested type at every concurrency level against
// the fixture, recording each cell's per-query distribution in the sink.
//
// The per-type bodies and the concurrency worker pool are #856 B2: each cell
// currently returns errQueryTypeUnimplemented naming its type, so a run proves
// out the flag surface, the fixture open, and the report schema, then writes a
// PARTIAL report holding the setup rows.
func runQuerySweep(
	ctx context.Context, logger *supportlog.Entry, f *queryFixture, p queryPlan, sink *csvSink,
) error {
	for _, qtype := range p.Types {
		for _, w := range p.Concurrency {
			logger.Infof("query %s at concurrency %d: %d iters (warmup %d)", qtype, w, p.Iters, p.Warmup)
			if err := runQueryCell(ctx, f, p, sink, qtype, w); err != nil {
				return fmt.Errorf("query %s at concurrency %d: %w", qtype, w, err)
			}
		}
	}
	return nil
}

// runQueryCell runs one (type, concurrency) cell: p.Warmup unmeasured queries
// then p.Iters measured ones, recording each query as a total_c<W> sample in
// the type's CSV and the cell's wall-clock as driver.csv's <qtype>_c<W> row.
//
// #856 B2 fills in the bodies against the ReadView seams — ScanLedgers for
// ledgers and txpage, the read_assembly lookup for txhash, QueryEvents for
// events — plus the worker pool that gives a level above 1 its readers, and the
// page-cache eviction a cold cell runs first.
func runQueryCell(
	_ context.Context, _ *queryFixture, _ queryPlan, _ *csvSink, qtype string, _ int,
) error {
	switch qtype {
	case queryTypeLedgers, queryTypeTxPage, queryTypeTxHash, queryTypeEvents:
		return fmt.Errorf("%w: %s", errQueryTypeUnimplemented, qtype)
	default:
		// Unreachable: parseQueryTypes rejects anything else.
		return fmt.Errorf("unknown query type %q", qtype)
	}
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
