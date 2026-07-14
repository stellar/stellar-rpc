package bench

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/ingest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/hotchunk"
)

// This file is the bench CSV reporter, in three layers: the report schema
// (fileSpecs — which CSV files exist and their fixed row orders), recording
// (csvSink, the ingest.MetricSink that stores every signal as a raw sample on
// a (file, row) key), and reporting (aggregation into percentile rows, then
// CSV/log rendering).

// csvHeader is every report CSV's header row.
const csvHeader = "stage,n,n_items,total_ns,p50_ns,p90_ns,p99_ns,max_ns"

// CSV file basenames not named after a cold data type.
const (
	fileHot    = "hot"    // hot.csv: per-phase rows
	fileDriver = "driver" // driver.csv: per-chunk aggregate rows
)

// Driver-report row labels. chunk_wall, ingest_total and read_blocked are
// observed by the bench drivers themselves (observeDriver); the rest arrive
// through the MetricSink cold signals.
const (
	driverChunkWall   = "chunk_wall"   // per-chunk wall-clock incl. stream open, seen by the driver
	driverIngestTotal = "ingest_total" // hot only: per-ledger end-to-end Ingest time (all phases, source wait excluded)
	driverReadBlocked = "read_blocked" // hot only: wait on the source between ledgers
	driverChunkTotal  = "chunk_total"  // ColdChunkTotal: per-chunk ColdService lifetime
	driverTotalSuffix = "_total"       // ColdIngest per data type: "<type>_total"
)

// fileSpec is one CSV file of the report: its basename (without .csv) and the
// fixed top-to-bottom order of its rows.
type fileSpec struct {
	name     string
	rowOrder []string
}

// fileSpecs is the whole report schema, in file emission order:
//
//   - one CSV per cold data type the ingest engine reports (ledgers.csv,
//     txhash.csv, events.csv), one row per cold pipeline stage (extract →
//     term_index → write → finalize) as reported via MetricSink.IngestStage;
//   - hot.csv, one row per hot ingest phase, in hotchunk.Phase order;
//   - driver.csv, the per-chunk aggregates: the driver-observed chunk
//     wall-clock, the engine's ColdChunkTotal, one "<type>_total" row per
//     cold data type (ColdIngest), and the hot bench's per-ledger ingest and
//     source-wait rows.
//
// A label recorded outside this vocabulary is still reported: withUnknown
// appends it after the known rows (or files) rather than silently dropping it.
//
//nolint:gochecknoglobals // fixed report schema, read-only
var fileSpecs = func() []fileSpec {
	coldTypes := []string{ingest.DataTypeLedgers, ingest.DataTypeTxhash, ingest.DataTypeEvents}
	coldStages := []string{ingest.StageExtract, ingest.StageTermIndex, ingest.StageWrite, ingest.StageFinalize}

	hotRows := make([]string, hotchunk.NumPhases)
	for p := range hotchunk.NumPhases {
		hotRows[p] = p.String()
	}

	driverRows := make([]string, 0, len(coldTypes)+4)
	driverRows = append(driverRows, driverChunkWall, driverChunkTotal)
	for _, dt := range coldTypes {
		driverRows = append(driverRows, dt+driverTotalSuffix)
	}
	driverRows = append(driverRows, driverIngestTotal, driverReadBlocked)

	specs := make([]fileSpec, 0, len(coldTypes)+2)
	for _, dt := range coldTypes {
		specs = append(specs, fileSpec{name: dt, rowOrder: coldStages})
	}
	return append(specs,
		fileSpec{name: fileHot, rowOrder: hotRows},
		fileSpec{name: fileDriver, rowOrder: driverRows},
	)
}()

// sample is one observed (duration, item-count) pair.
type sample struct {
	d     time.Duration
	items int
}

// series accumulates samples for one CSV row.
type series struct {
	samples []sample
}

func (s *series) observe(d time.Duration, items int) {
	s.samples = append(s.samples, sample{d: d, items: items})
}

// rowKey locates one CSV row: the file basename it lands in and its row label.
type rowKey struct {
	file, row string
}

// csvSink is an ingest.MetricSink that records every signal in memory and, on
// writeCSVs, aggregates them into percentile CSVs
// (stage,n,n_items,total_ns,p50_ns,p90_ns,p99_ns,max_ns) laid out per
// fileSpecs. n counts only non-zero-duration samples (an empty ledger's
// zero-duration stage does not skew percentiles) and n_items sums each
// included sample's natural item count. Rows with no included samples — and
// files with no rows — are suppressed.
//
// All methods are safe for concurrent use (one mutex), as the MetricSink
// contract requires: the cold drivers run several WriteColdChunk workers
// against one sink.
type csvSink struct {
	mu   sync.Mutex
	rows map[rowKey]*series // every signal is one sample on a (file, row) key
}

var _ ingest.MetricSink = (*csvSink)(nil)

// newCSVSink returns an empty recorder.
func newCSVSink() *csvSink {
	return &csvSink{rows: make(map[rowKey]*series)}
}

// HotPhase records one phase of one hot ledger ingest.
func (s *csvSink) HotPhase(phase hotchunk.Phase, d time.Duration, items int, _ error) {
	s.observe(fileHot, phase.String(), d, items)
}

// ColdIngest records one cold ingester's per-chunk total.
func (s *csvSink) ColdIngest(dataType string, d time.Duration, items int, _ error) {
	s.observe(fileDriver, dataType+driverTotalSuffix, d, items)
}

// ColdChunkTotal records the per-chunk aggregate wall-clock.
func (s *csvSink) ColdChunkTotal(d time.Duration) {
	s.observe(fileDriver, driverChunkTotal, d, 0)
}

// IngestStage records one cold ingester's per-stage wall-clock.
func (s *csvSink) IngestStage(dataType, stage string, d time.Duration, items int) {
	s.observe(dataType, stage, d, items)
}

// observe appends one sample to the (file, row) series, creating it on first
// use. Every recording method lands here. (funcorder pins it below the
// exported methods it serves.)
func (s *csvSink) observe(fileName, rowName string, d time.Duration, items int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := rowKey{file: fileName, row: rowName}
	sr := s.rows[k]
	if sr == nil {
		sr = &series{}
		s.rows[k] = sr
	}
	sr.observe(d, items)
}

// observeDriver records a driver-level row (driverChunkWall, driverReadBlocked)
// outside the MetricSink interface — timings only the bench driver's own loop
// can see.
func (s *csvSink) observeDriver(name string, d time.Duration, items int) {
	s.observe(fileDriver, name, d, items)
}

// sumDriver returns the summed duration of a driver row's samples — the
// numerator of the cold driver's effective-concurrency summary.
func (s *csvSink) sumDriver(name string) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	var total time.Duration
	if sr := s.rows[rowKey{file: fileDriver, row: name}]; sr != nil {
		for _, sm := range sr.samples {
			total += sm.d
		}
	}
	return total
}

// row is one aggregated CSV row.
type row struct {
	name  string
	n     int
	items int
	total time.Duration
	p50   time.Duration
	p90   time.Duration
	p99   time.Duration
	maxv  time.Duration
}

// aggregate reduces a series to a row, filtering out zero-duration samples so
// work too fast for the timer (an empty ledger's stage) doesn't skew the
// percentiles. ok is false when no sample survives the filter — the row is
// suppressed.
func aggregate(name string, s *series) (row, bool) {
	durs := make([]time.Duration, 0, len(s.samples))
	items := 0
	for _, sm := range s.samples {
		if sm.d > 0 {
			durs = append(durs, sm.d)
			items += sm.items
		}
	}
	if len(durs) == 0 {
		return row{}, false
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
	return row{
		name: name, n: len(durs), items: items, total: total,
		p50: pick(0.50), p90: pick(0.90), p99: pick(0.99), maxv: durs[len(durs)-1],
	}, true
}

// withUnknown returns order plus any keys of m not already in it, sorted and
// appended after the known order — a recorded label outside the known
// vocabulary is reported after the known rows rather than silently dropped.
func withUnknown[V any](order []string, m map[string]V) []string {
	var extra []string
	for k := range m {
		if !slices.Contains(order, k) {
			extra = append(extra, k)
		}
	}
	if len(extra) == 0 {
		return order
	}
	slices.Sort(extra)
	return append(slices.Clone(order), extra...)
}

// file is one aggregated CSV file: its basename (without .csv) and its rows.
type file struct {
	name string
	rows []row
}

// files aggregates every recorded series into the report's CSV files, in
// fileSpecs order (a file outside the schema is appended after, sorted, its
// rows ordered by sorted label).
func (s *csvSink) files() []file {
	s.mu.Lock()
	defer s.mu.Unlock()

	byFile := make(map[string]map[string]*series)
	for k, sr := range s.rows {
		if byFile[k.file] == nil {
			byFile[k.file] = make(map[string]*series)
		}
		byFile[k.file][k.row] = sr
	}

	names := make([]string, len(fileSpecs))
	rowOrders := make(map[string][]string, len(fileSpecs))
	for i, spec := range fileSpecs {
		names[i] = spec.name
		rowOrders[spec.name] = spec.rowOrder
	}

	var out []file
	for _, name := range withUnknown(names, byFile) {
		byRow := byFile[name]
		var rows []row
		for _, label := range withUnknown(rowOrders[name], byRow) {
			if sr := byRow[label]; sr != nil {
				if r, ok := aggregate(label, sr); ok {
					rows = append(rows, r)
				}
			}
		}
		if len(rows) > 0 {
			out = append(out, file{name: name, rows: rows})
		}
	}
	return out
}

// writeCSVs writes every non-empty aggregated CSV under outDir (created if
// missing) and returns the files it wrote.
func (s *csvSink) writeCSVs(outDir string) ([]string, error) {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", outDir, err)
	}
	var written []string
	for _, f := range s.files() {
		path := filepath.Join(outDir, f.name+".csv")
		if err := writeCSV(path, f.rows); err != nil {
			return written, err
		}
		written = append(written, path)
	}
	return written, nil
}

func writeCSV(path string, rows []row) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	if _, err := fmt.Fprintln(f, csvHeader); err != nil {
		return fmt.Errorf("write header: %w", err)
	}
	for _, r := range rows {
		if _, err := fmt.Fprintf(f, "%s,%d,%d,%d,%d,%d,%d,%d\n",
			r.name, r.n, r.items, r.total.Nanoseconds(),
			r.p50.Nanoseconds(), r.p90.Nanoseconds(), r.p99.Nanoseconds(), r.maxv.Nanoseconds(),
		); err != nil {
			return fmt.Errorf("write row %s: %w", r.name, err)
		}
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close %s: %w", path, err)
	}
	return nil
}

// logSummary logs one percentile line per aggregated row, through the logger
// (forbidigo bans fmt.Print*).
func (s *csvSink) logSummary(logger *supportlog.Entry) {
	for _, f := range s.files() {
		for _, r := range f.rows {
			logger.Infof("%-10s %-12s n=%-7d items=%-9d total=%-12s p50=%-10s p90=%-10s p99=%-10s max=%s",
				f.name, r.name, r.n, r.items,
				r.total.Round(time.Microsecond),
				r.p50.Round(time.Microsecond),
				r.p90.Round(time.Microsecond),
				r.p99.Round(time.Microsecond),
				r.maxv.Round(time.Microsecond))
		}
	}
}
