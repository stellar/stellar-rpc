package bench

import (
	"context"
	"errors"
	"fmt"
	"math"
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

// Query report row labels. A row belonging to one leg carries an _r<rate>
// segment holding that leg's target rate as --target-rps spelled it: a per-type
// CSV names its latency rows total_r<rate> and service_r<rate>, and driver.csv
// names one leg's rows <qtype>_r<rate> plus the three suffixed variants below.
// A driver row with no _r<rate> segment belongs to the run's setup.
const (
	queryRowTotalPrefix   = "total_r"
	queryRowServicePrefix = "service_r"
	driverLegRPSSuffix    = "_millirps"
	driverLegLagSuffix    = "_lag"
	driverLegShedSuffix   = "_shed"
	driverQueryOpen       = "open"  // fixture open: catalog, handles, first read view
	driverQueryEvict      = "evict" // one page-cache eviction pass before a cold leg
)

// formatRPS renders a target rate as its row label spells it: the shortest
// decimal that reads back as the same rate, so 0.5 stays "0.5" and 300 stays
// "300".
func formatRPS(rps float64) string { return strconv.FormatFloat(rps, 'f', -1, 64) }

// queryTotalRow is a per-type CSV's scheduled-latency row label for the leg at
// rate rps.
func queryTotalRow(rps float64) string { return queryRowTotalPrefix + formatRPS(rps) }

// queryServiceRow is a per-type CSV's service-time row label for the leg at
// rate rps.
func queryServiceRow(rps float64) string { return queryRowServicePrefix + formatRPS(rps) }

// queryStageRow is a per-type CSV's row label for one sub-stage of the leg at
// rate rps.
func queryStageRow(stage string, rps float64) string { return stage + "_r" + formatRPS(rps) }

// queryDriverRow is driver.csv's wall-clock row label for one query type's leg
// at rate rps.
func queryDriverRow(qtype string, rps float64) string { return qtype + "_r" + formatRPS(rps) }

// queryDriverLegRow is driver.csv's row label for one of a leg's driver
// metrics: queryDriverRow's label plus the metric's suffix.
func queryDriverLegRow(qtype string, rps float64, suffix string) string {
	return queryDriverRow(qtype, rps) + suffix
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

// Defaults for the two flags that shape a leg. defaultLegDuration is long
// enough that a slow rate still schedules a useful number of requests and short
// enough that a four-type ladder finishes in minutes; defaultTargetRPS is a
// single modest rate, so a bare invocation runs one leg per type.
const (
	defaultLegDuration = 60 * time.Second
	defaultTargetRPS   = "10"
)

// maxTargetRPS is the highest arrival rate --target-rps accepts: the ceiling at
// which the single dispatch loop that issues a leg's arrivals can still keep to
// its schedule. A leg above it measures the loop's own speed.
const maxTargetRPS = 1_000_000

// minLegSamples is the number of measured requests below which a leg's
// percentiles are reported with a warning: a p99 over fewer samples than this
// is one or two requests wide.
const minLegSamples = 100

// queryFlags is the flag set both bench-query subcommands share, beyond the
// --out and profiling flags newBenchCommand binds. The spellings and value
// formats of --types, --target-rps, --duration, and --warmup are the campaign
// runner's argv contract; the read-shape flags after them are bench-side only
// and outside that contract.
type queryFlags struct {
	types       string
	targetRPS   string
	duration    time.Duration
	warmup      int
	warmupBound bool // bind --warmup (hot only; a cold leg evicts its page cache)

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
		"comma-separated query types to run: "+strings.Join(allQueryTypes, " | "))
	fs.StringVar(&f.targetRPS, "target-rps", defaultTargetRPS,
		"comma-separated arrival rates to run, in requests per second, e.g. 0.5,1,2")
	fs.DurationVar(&f.duration, "duration", defaultLegDuration, "how long each --target-rps leg runs")
	if f.warmupBound {
		fs.IntVar(&f.warmup, "warmup", f.warmup,
			"unmeasured queries per leg, dispatched at the leg's rate before measurement starts, "+
				"warming the store's caches")
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

// queryPlan is the validated run: which types, at which arrival rates, how long
// each leg runs, and the shape of each query.
type queryPlan struct {
	Types     []string
	TargetRPS []float64
	Duration  time.Duration
	Warmup    int

	LedgersSpan  uint32
	TxPageSpan   uint32
	TxPageLimit  int
	EventsLimit  int
	MissFraction float64
	Passphrase   string
	Seed         int64

	// Evict drops the cold artifacts from the OS page cache before each leg's
	// measured requests. Cold only: the hot tier's steady state is a warm cache,
	// so its legs warm one up first.
	Evict bool
}

// plan parses and validates the flags.
func (f *queryFlags) plan() (queryPlan, error) {
	types, err := parseQueryTypes(f.types)
	if err != nil {
		return queryPlan{}, err
	}
	rates, err := parseTargetRPS(f.targetRPS)
	if err != nil {
		return queryPlan{}, err
	}
	switch {
	case f.duration <= 0:
		return queryPlan{}, fmt.Errorf("--duration must be > 0, got %v", f.duration)
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
		TargetRPS:    rates,
		Duration:     f.duration,
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
// empty list, an unknown type, or a repeat (a type names its own CSV file, so
// it may appear once).
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

// parseTargetRPS splits --target-rps into the arrival rates to run, keeping the
// caller's order and rejecting an empty list, an empty entry, a non-number, a
// rate that is zero, negative, NaN or infinite, a rate above maxTargetRPS, or a
// repeat (a rate names its leg's CSV rows, so it may appear once).
func parseTargetRPS(s string) ([]float64, error) {
	fields := strings.Split(s, ",")
	rates := make([]float64, 0, len(fields))
	for _, f := range fields {
		field := strings.TrimSpace(f)
		if field == "" {
			return nil, fmt.Errorf("--target-rps has an empty entry: %q", s)
		}
		rps, err := strconv.ParseFloat(field, 64)
		if err != nil {
			return nil, fmt.Errorf("--target-rps: %q is not a list of numbers", s)
		}
		if rps <= 0 || math.IsNaN(rps) || math.IsInf(rps, 0) {
			return nil, fmt.Errorf("--target-rps rates must be > 0, got %v", rps)
		}
		if rps > maxTargetRPS {
			return nil, fmt.Errorf("--target-rps rates must be <= %v, got %v", float64(maxTargetRPS), rps)
		}
		if slices.Contains(rates, rps) {
			return nil, fmt.Errorf("--target-rps repeats %v", rps)
		}
		rates = append(rates, rps)
	}
	return rates, nil
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

	// EvictPaths are the on-disk artifacts a cold leg drops from the page cache
	// before it measures. Empty for a hot fixture, whose data lives in RocksDB's
	// own caches, which the bench cannot advise the kernel about.
	EvictPaths []string
}

// view acquires one read view. Every measured query takes its own, as a served
// request does; the caller MUST Release it.
func (f *queryFixture) view() (*query.ReadView, error) {
	return f.registry.NewReadView()
}

// verifyServes acquires a read view and resolves each benchmarked chunk's
// ledger store, so a dataset that cannot be served fails at open, with the
// chunk named. It runs the real routing (ReadView.Ledgers → resolveTier), which
// is also what makes it a check: the fixture publishes state for one tier only.
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
// and reports how many files it advised. It runs before every leg, so each leg
// reads from disk and its numbers are cold.
//
// A file that cannot be opened is skipped and the run continues: the artifact
// set is derived from the layout, so a kind the dataset never produced is a
// legitimate absence.
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

// runQueryLegs runs every requested type at every requested rate against the
// fixture, recording each leg's per-request distribution in the sink.
//
// A type's corpus is sampled once, before its first leg, and shared by every
// rate, so the legs of one type all read the same work and what separates two
// of them is the rate.
func runQueryLegs(
	ctx context.Context, logger *supportlog.Entry, f *queryFixture, p queryPlan, sink *csvSink,
) error {
	for _, qtype := range p.Types {
		req, err := newQueryRequest(ctx, logger, f, p, qtype)
		if err != nil {
			return fmt.Errorf("prepare the %s benchmark: %w", qtype, err)
		}
		for _, rps := range p.TargetRPS {
			if err := runQueryLeg(ctx, logger, f, p, sink, qtype, rps, req); err != nil {
				return fmt.Errorf("query %s at %s rps: %w", qtype, formatRPS(rps), err)
			}
			if err := ctx.Err(); err != nil {
				return err
			}
		}
	}
	return nil
}

// runQueryLeg runs one (type, rate) leg: page-cache eviction for a cold run,
// then p.Warmup unmeasured requests and the leg's measured ones, all dispatched
// at rps requests per second over p.Duration.
//
// The leg's seed mixes in the type, so two types read different ledgers in
// different orders and neither inherits the other's warm cache.
func runQueryLeg(
	ctx context.Context, logger *supportlog.Entry, f *queryFixture, p queryPlan, sink *csvSink,
	qtype string, rps float64, req queryRequest,
) error {
	if p.Evict {
		start := time.Now()
		evicted, err := f.evictColdArtifacts()
		if err != nil {
			return err
		}
		sink.observe(fileDriver, driverQueryEvict, time.Since(start), evicted)
	}
	measured := measuredRequests(rps, p.Duration)
	logger.Infof("query %s at %s rps for %s: %d measured requests, %d warmup",
		qtype, formatRPS(rps), p.Duration, measured, p.Warmup)
	if measured < minLegSamples {
		logger.Warnf("query %s at %s rps measures %d requests: its percentiles rest on fewer than "+
			"%d samples, so a longer --duration makes them steadier",
			qtype, formatRPS(rps), measured, minLegSamples)
	}

	res, err := runPacedLeg(ctx, rps, p.Duration, p.Warmup, p.Seed+int64(len(qtype)), req)
	if err != nil {
		return err
	}
	if res.errs > 0 {
		return fmt.Errorf("%d of %d requests failed", res.errs, res.dispatched)
	}
	recordLeg(sink, qtype, rps, res)
	return nil
}

// recordLeg files one leg's samples into the report.
//
// In the type's own CSV every request lands in the total_r<rate> row — the
// scheduled latency the results converter reads — and in the service_r<rate>
// row, which holds the same requests' service times; a request carrying a
// sub-stage lands in a <stage>_r<rate> row too. The converter matches
// total_r<rate> alone, so the side rows cost the site nothing and give the CSV
// and the log summary the splits that matter locally: what the store spent
// against what a client waited, and txhash's found lookups against its
// not-found ones, which do different amounts of work.
//
// driver.csv gets the leg's four driver rows. <qtype>_r<rate> is the leg wall,
// which runs to the last request's completion and so covers the leg's drain
// tail. The _millirps row's duration columns carry the achieved rate times 1000
// as an integer, the way peak_rss_bytes carries bytes; it is the answered
// requests over the window the leg offered, and the wall row carries the drain
// tail. The _lag row holds one sample per measured position, shed positions
// included, so it is the distribution of how far behind schedule the dispatcher
// ran. The _shed row is written for every leg, so a leg that shed nothing
// reports a zero.
func recordLeg(sink *csvSink, qtype string, rps float64, res legResult) {
	total := queryTotalRow(rps)
	service := queryServiceRow(rps)
	for _, s := range res.samples {
		sink.observe(qtype, total, s.scheduled, s.items)
		sink.observe(qtype, service, s.service, s.items)
		if s.stage != "" {
			sink.observe(qtype, queryStageRow(s.stage, rps), s.scheduled, s.items)
		}
	}

	answered := len(res.samples)
	sink.observe(fileDriver, queryDriverRow(qtype, rps), res.wall, answered)
	sink.observe(fileDriver, queryDriverLegRow(qtype, rps, driverLegRPSSuffix),
		achievedMilliRPS(answered, res.offered), answered)
	for _, lag := range res.lags {
		sink.observe(fileDriver, queryDriverLegRow(qtype, rps, driverLegLagSuffix), lag, 1)
	}
	sink.observe(fileDriver, queryDriverLegRow(qtype, rps, driverLegShedSuffix), 0, res.shed)
}

// milliPerUnit is the scale the _millirps row stores an achieved rate at, so a
// fractional rate survives an integer CSV column.
const milliPerUnit = 1000

// achievedMilliRPS returns the rate answered requests were served at over the
// window the leg offered, scaled by milliPerUnit and carried as a duration so
// it fits the CSV's duration columns. A leg with no offered window to divide by
// reports zero.
func achievedMilliRPS(answered int, offered time.Duration) time.Duration {
	if offered <= 0 {
		return 0
	}
	return time.Duration(math.Round(float64(answered) / offered.Seconds() * milliPerUnit))
}

// runQueryBench is the body both bench-query subcommands share: prepare --out,
// open the tier's fixture (timing the open into driver.csv), run the legs, and
// report. A failed run still writes the partial report and the peak RSS, so the
// setup rows and whatever legs completed survive.
func runQueryBench(
	ctx context.Context, logger *supportlog.Entry, p queryPlan, outDir string,
	open func() (*queryFixture, func(), error),
) error {
	// Surface an unwritable --out before opening the dataset.
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return fmt.Errorf("create --out dir %s: %w", outDir, err)
	}
	sink := newSchemaCSVSink(querySpecs(p.Types, p.TargetRPS))

	start := time.Now()
	f, release, err := open()
	if err != nil {
		return err
	}
	defer release()
	sink.observe(fileDriver, driverQueryOpen, time.Since(start), len(f.Chunks))
	logger.Infof("serving ledgers [%d, %d] over %d chunk(s)", f.FirstLedger, f.LastLedger, len(f.Chunks))

	err = runQueryLegs(ctx, logger, f, p, sink)
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
