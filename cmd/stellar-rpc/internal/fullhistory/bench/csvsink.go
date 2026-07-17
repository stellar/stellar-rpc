package bench

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/ingest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/observability"
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

// Driver-report row labels. run_wall is recorded by the hot bench driver
// itself via observe; backfill_wall and index_rebuild arrive through the
// observability.Metrics signals the backfill scheduler emits; ingest_total is
// reconstructed in HotPhase from each ledger's phase burst; the rest arrive
// through the MetricSink cold signals.
const (
	driverBackfillWall = "backfill_wall" // cold only: RunBackfill's whole plan-and-execute wall (Metrics.Freeze)
	driverIndexRebuild = "index_rebuild" // cold only: one txhash index build incl. eager sweep (Metrics.Rebuild)
	driverChunkTotal   = "chunk_total"   // ColdChunkTotal: per-chunk ColdService lifetime
	driverTotalSuffix  = "_total"        // ColdIngest per data type: "<type>_total"
	// cold only: the shared per-ledger ExtractLedgerEvents walk (ColdExtract),
	// ledger-scoped and belonging to no single data type, so it lands in
	// driver.csv.
	driverColdExtract = "cold_extract"
	// hot only: per-ledger end-to-end ingest time, reconstructed as the sum of
	// one ledger's HotPhase burst (per-phase percentiles can't be summed).
	driverIngestTotal = "ingest_total"
	driverRunWall     = "run_wall" // hot only: whole-run wall-clock, seen by the driver
	// hot only, paced runs (--close-interval > 0): per-ledger lag behind the
	// close schedule at commit. Its "duration" columns carry a lag time; a
	// lag that grows across the run means the backlog is growing (see
	// recordPaceLag).
	driverPaceLag = "pace_lag"
	// cold AND hot: the run's peak resident set size (VmHWM), recorded once
	// at run end (see recordPeakRSS). Its "duration" columns carry BYTES, not
	// nanoseconds: storing the byte count as a duration lets the row use the
	// same CSV format as every other row.
	driverPeakRSS = "peak_rss_bytes"
)

// Cold data-type and stage report labels, fixing the file/row order of the cold
// CSVs (see fileSpecs). They MUST equal the strings the ingest engine emits
// through MetricSink (ColdIngest's data_type, IngestStage's data_type/stage) —
// they set the report ORDER. A label that drifts from the engine's is not dropped:
// withUnknown appends any unrecognized row after the known ones; it only loses
// its curated position.
const (
	coldTypeLedgers = "ledgers"
	coldTypeTxhash  = "txhash"
	coldTypeEvents  = "events"

	coldStageTermIndex = "term_index"
	coldStageWrite     = "write"
	coldStageFinalize  = "finalize"
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
//     txhash.csv, events.csv), one row per cold pipeline stage (term_index →
//     write → finalize) as reported via MetricSink.IngestStage;
//   - hot.csv, one row per hot ingest phase, in hotchunk.Phase order;
//   - driver.csv, the run-level aggregates: the cold scheduler's backfill wall
//     and per-index-build rebuild rows (observability.Metrics), the engine's
//     ColdChunkTotal, one "<type>_total" row per cold data type (ColdIngest),
//     the shared per-ledger cold extract walk (ColdExtract), the hot
//     per-ledger end-to-end ingest_total (reconstructed from each ledger's
//     phase burst in HotPhase), the hot bench's driver-observed run wall-clock,
//     the paced hot run's per-ledger pace_lag, and — for both modes, wherever
//     VmHWM is readable — the run's peak_rss_bytes.
//
// The driver row order groups by producer: the cold rows first, then the
// hot-only rows (ingest_total, run_wall, pace_lag together), and finally
// peak_rss_bytes, which both modes emit. A row that recorded nothing is
// suppressed, so each mode's report carries only its own rows — and
// peak_rss_bytes is absent where /proc is unavailable (macOS; see
// recordPeakRSS).
//
// A label recorded outside this vocabulary is still reported: withUnknown
// appends it after the known rows (or files), so nothing is silently dropped.
//
//nolint:gochecknoglobals // fixed report schema, read-only
var fileSpecs = func() []fileSpec {
	coldTypes := []string{coldTypeLedgers, coldTypeTxhash, coldTypeEvents}
	coldStages := []string{coldStageTermIndex, coldStageWrite, coldStageFinalize}

	hotRows := make([]string, hotchunk.NumPhases)
	for p := range hotchunk.NumPhases {
		hotRows[p] = p.String()
	}

	driverRows := make([]string, 0, len(coldTypes)+8)
	driverRows = append(driverRows, driverBackfillWall, driverIndexRebuild, driverChunkTotal)
	for _, dt := range coldTypes {
		driverRows = append(driverRows, dt+driverTotalSuffix)
	}
	// cold_extract closes the cold rows; ingest_total, run_wall, and pace_lag
	// keep the hot-only rows grouped after them; peak_rss_bytes comes last,
	// emitted by both modes.
	driverRows = append(driverRows, driverColdExtract,
		driverIngestTotal, driverRunWall, driverPaceLag, driverPeakRSS)

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

// csvSink is an ingest.MetricSink AND an observability.Metrics that records
// every signal in memory and, on writeCSVs, aggregates them into percentile
// CSVs (stage,n,n_items,total_ns,p50_ns,p90_ns,p99_ns,max_ns) laid out per
// fileSpecs. n counts only non-zero-duration samples (an empty ledger's
// zero-duration stage does not skew percentiles) and n_items sums each
// included sample's natural item count. Rows with no included samples — and
// files with no rows — are suppressed. Most signals map one-to-one onto a
// sample; HotPhase additionally reconstructs the per-ledger ingest_total row
// from each ledger's phase burst (see HotPhase).
//
// Of the observability signals only the durations a bench run produces are
// recorded (Freeze — the backfill wall — and Rebuild — one index build each);
// LastCommitted is tracked as a gauge for the hot driver's completion check,
// and the rest carry no bench signal so they are dropped — Prune does fire
// from each index build's eager sweep, but its wall is already inside Rebuild
// and a fresh scratch run sweeps ~nothing; the others never fire.
//
// All methods are safe for concurrent use (one mutex), as the MetricSink
// contract requires: the backfill scheduler runs several chunk freezes against
// one sink.
type csvSink struct {
	mu   sync.Mutex
	rows map[rowKey]*series // every signal is one sample on a (file, row) key

	// hotBurst accumulates the current hot ledger's HotPhase durations so
	// HotPhase can reconstruct the per-ledger end-to-end ingest_total (the
	// phases partition the per-ledger total). Guarded by mu.
	hotBurst time.Duration

	// lastSeq mirrors the loop's last-committed gauge (Metrics.LastCommitted,
	// set once per ingested ledger) so the hot driver can verify the bounded
	// run reached its final ledger.
	lastSeq atomic.Uint32

	// schedule is the paced hot run's close schedule (--close-interval > 0);
	// LastCommitted measures each committed ledger's lag against it. Nil in
	// every other run — cold, or unpaced hot — which records no pace_lag.
	schedule *paceSchedule
}

var (
	_ ingest.MetricSink     = (*csvSink)(nil)
	_ observability.Metrics = (*csvSink)(nil)
)

// newCSVSink returns an empty recorder.
func newCSVSink() *csvSink {
	return &csvSink{rows: make(map[rowKey]*series)}
}

// HotPhase records one phase of one hot ledger ingest into hot.csv, and
// reconstructs the per-ledger end-to-end ingest_total for driver.csv from the
// phase burst.
//
// The production hot loop (runIngestionLoop) is a SINGLE goroutine, so HotPhase
// signals arrive as strict per-ledger bursts in hotchunk.Phase order:
// extract → ledgers → txhash → events → commit → apply, one burst per ledger.
// The phases partition the per-ledger IngestLedger wall-clock, so their sum IS
// the per-ledger total — a number per-phase percentiles can't recover
// (percentiles don't sum). PhaseExtract (always first) resets the accumulator
// for a new ledger; PhaseApply (terminal, emitted on the success path only —
// never the failed phase) records one ingest_total sample with items=1. A
// failed ledger never reaches PhaseApply, so it contributes nothing (the run
// aborts and partial CSVs are written anyway).
//
// The accumulator is mutex-guarded so the sink stays safe under the MetricSink
// contract regardless; only the production single-writer pattern yields
// meaningful sums (interleaved bursts would sum across ledgers, never race).
func (s *csvSink) HotPhase(phase hotchunk.Phase, d time.Duration, items int, _ error) {
	s.observe(fileHot, phase.String(), d, items)

	// Accumulator under its own critical section, released before the observe
	// below so the two locks are sequential, never nested.
	s.mu.Lock()
	if phase == hotchunk.PhaseExtract {
		s.hotBurst = 0
	}
	s.hotBurst += d
	total := s.hotBurst
	s.mu.Unlock()

	if phase == hotchunk.PhaseApply {
		s.observe(fileDriver, driverIngestTotal, total, 1)
	}
}

// ColdIngest records one cold ingester's per-chunk total.
func (s *csvSink) ColdIngest(dataType string, d time.Duration, items int, _ error) {
	s.observe(fileDriver, dataType+driverTotalSuffix, d, items)
}

// ColdChunkTotal records the per-chunk aggregate wall-clock.
func (s *csvSink) ColdChunkTotal(d time.Duration) {
	s.observe(fileDriver, driverChunkTotal, d, 0)
}

// ColdExtract records the shared per-ledger ExtractLedgerEvents walk —
// ledger-scoped and type-less, so it lands in driver.csv (see
// driverColdExtract).
func (s *csvSink) ColdExtract(d time.Duration, items int, _ error) {
	s.observe(fileDriver, driverColdExtract, d, items)
}

// IngestStage records one cold ingester's per-stage wall-clock.
func (s *csvSink) IngestStage(dataType, stage string, d time.Duration, items int) {
	s.observe(dataType, stage, d, items)
}

// Freeze records one backfill pass's whole plan-and-execute wall-clock.
func (s *csvSink) Freeze(d time.Duration) {
	s.observe(fileDriver, driverBackfillWall, d, 0)
}

// Rebuild records one txhash index build's wall-clock (including its eager
// post-build sweep).
func (s *csvSink) Rebuild(d time.Duration) {
	s.observe(fileDriver, driverIndexRebuild, d, 0)
}

// LastCommitted tracks the hot loop's per-ledger committed gauge (see lastSeq),
// and — for a paced run — records that ledger's lag behind the close schedule.
func (s *csvSink) LastCommitted(lastCommitted uint32) {
	s.lastSeq.Store(lastCommitted)
	if s.schedule != nil {
		s.recordPaceLag(lastCommitted)
	}
}

// The remaining observability signals carry no useful bench signal and are
// accepted and dropped — Prune does fire (from each index build's eager
// sweep) but its wall is already inside Rebuild; the others never fire.

func (s *csvSink) RetentionFloor(uint32) {}

func (s *csvSink) ChunkBoundary() {}

func (s *csvSink) LiveHotChunks(int) {}

func (s *csvSink) BackfillPass(time.Duration) {}

func (s *csvSink) Discard(int, time.Duration) {}

func (s *csvSink) Prune(int, time.Duration) {}

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

// lastCommittedSeq returns the highest ledger the hot loop reported committed.
func (s *csvSink) lastCommittedSeq() uint32 {
	return s.lastSeq.Load()
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

// recordPaceLag records how far past its scheduled due time ledger seq
// committed: lag = now − due(seq), clamped at ≥ 0 (defensive: through the
// pacer a commit never precedes its due time), as one driverPaceLag sample
// with items=1. On pace, a ledger starts at its due time, so its lag at
// commit ≈ its own ingest time; a lag that grows across the run means
// ingestion cannot keep up with the close cadence and the backlog is growing.
func (s *csvSink) recordPaceLag(seq uint32) {
	due, ok := s.schedule.dueForSeq(seq)
	if !ok {
		return
	}
	lag := max(s.schedule.clock().Sub(due), 0)
	s.observe(fileDriver, driverPaceLag, lag, 1)
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
// appended after the known order, so a recorded label outside the known
// vocabulary still appears, after the known rows.
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
			// peak_rss_bytes carries bytes in its duration fields; render it
			// as a byte count, not a garbled duration.
			if f.name == fileDriver && r.name == driverPeakRSS {
				logger.Infof("%-10s %-12s n=%-7d bytes=%d", f.name, r.name, r.n, r.total.Nanoseconds())
				continue
			}
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
