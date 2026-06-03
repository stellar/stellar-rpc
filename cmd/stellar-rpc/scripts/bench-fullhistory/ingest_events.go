package main

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"time"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/eventstore"
)

// ───────────────────────── Collector ─────────────────────────

// eventSample is one ledger's events extraction + per-tier write
// measurement. Stage fields not used by the active tier remain zero
// (zero-total stages are suppressed from CSV).
type eventSample struct {
	Items      int
	Extract    time.Duration
	TermIndex  time.Duration // cold only
	HotWrite   time.Duration // hot only
	ColdAppend time.Duration // cold only
}

// EventsCollector accumulates events samples. Under multi-chunk runs,
// `finish` has one entry per chunk; under single-chunk a one-element
// slice.
type EventsCollector struct {
	samples []eventSample
	finish  []time.Duration // cold only; Finish + WriteColdIndex per chunk
}

func NewEventsCollector(n int) *EventsCollector {
	return &EventsCollector{samples: make([]eventSample, 0, n)}
}

// Merge folds other's samples + per-chunk scalars into this collector.
func (c *EventsCollector) Merge(other *EventsCollector) {
	c.samples = append(c.samples, other.samples...)
	c.finish = append(c.finish, other.finish...)
}

// perLedgerRows computes the nonzero-filtered per-ledger stage rows in a
// single pass shared by PrintSummary, WriteCSV, and InPipelineTime. Row
// names match the CSV schema. Each tier populates a disjoint subset
// (hot: extract+hot_write; cold: extract+term_index+cold_append), so the
// other tier's rows are empty and suppressed.
//
//nolint:funcorder // helper for the exported summary methods below
func (c *EventsCollector) perLedgerRows() []stageRow {
	var (
		extracts, terms, hotWrites, coldApp   []time.Duration
		extractIts, termIts, writeIts, appIts int
	)
	for _, s := range c.samples {
		if s.Extract > 0 {
			extracts = append(extracts, s.Extract)
			extractIts += s.Items
		}
		if s.TermIndex > 0 {
			terms = append(terms, s.TermIndex)
			termIts += s.Items
		}
		if s.HotWrite > 0 {
			hotWrites = append(hotWrites, s.HotWrite)
			writeIts += s.Items
		}
		if s.ColdAppend > 0 {
			coldApp = append(coldApp, s.ColdAppend)
			appIts += s.Items
		}
	}
	return []stageRow{
		{name: "extract", durs: extracts, items: extractIts},
		{name: "term_index", durs: terms, items: termIts},
		{name: "hot_write", durs: hotWrites, items: writeIts},
		{name: "cold_append", durs: coldApp, items: appIts},
	}
}

func (c *EventsCollector) PrintSummary(tier string, w io.Writer) {
	rows := c.perLedgerRows()
	printNamedStage(w, tier+".events.extract", rows, "extract", len(c.samples))
	if tier == "hot" {
		printNamedStage(w, tier+".events.write", rows, "hot_write", len(c.samples))
	} else {
		printNamedStage(w, tier+".events.term_index", rows, "term_index", len(c.samples))
		printNamedStage(w, tier+".events.cold_append", rows, "cold_append", len(c.samples))
		printChunkScalar(w, "cold.events.finish", c.finish)
	}
}

func (c *EventsCollector) WriteCSV(outDir, filenamePrefix string) error {
	return writeStageCSV(outDir, filenamePrefix, c.perLedgerRows())
}

func (c *EventsCollector) TotalItems() int {
	total := 0
	for _, s := range c.samples {
		total += s.Items
	}
	return total
}

// InPipelineTime returns the per-type pipeline time across all stages
// the events ingest runs (extract + write for hot; extract + term_index
// + cold_append + finish for cold) summed across all chunks. Used as
// the "extract+write" denominator for throughput.
func (c *EventsCollector) InPipelineTime() time.Duration {
	total := stageRowsTotal(c.perLedgerRows())
	for _, d := range c.finish {
		total += d
	}
	return total
}

// ───────────────────────── Hot ingester ─────────────────────────

// EventsHot decodes LCM → events.Payload (via view or parsed extractor)
// and writes them with IngestLedgerEvents. Each call is one atomic
// RocksDB batch (sync=true) plus an in-memory mirror update.
//
// IngestLedgerEvents is called on every ledger, including ones with
// zero payloads — LedgerOffsets.Append requires a contiguous sequence
// and would reject the next non-empty ledger if an empty one were
// skipped.
type EventsHot struct {
	store     *eventstore.HotStore
	xdrViews  bool
	collector *EventsCollector

	payloadScratch []events.Payload // reused across ledgers (view path) to avoid a per-ledger alloc
}

func NewEventsHot(c *EventsCollector, dir string, chunkID chunk.ID, logger *supportlog.Entry, xdrViews bool) (*EventsHot, error) {
	if err := os.MkdirAll(filepath.Dir(dir), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir parent of %s: %w", dir, err)
	}
	store, err := eventstore.OpenHotStore(dir, chunkID, logger, eventstore.WithXDRViews(xdrViews))
	if err != nil {
		return nil, fmt.Errorf("eventstore.OpenHotStore %s: %w", dir, err)
	}
	return &EventsHot{store: store, xdrViews: xdrViews, collector: c}, nil
}

func (e *EventsHot) Ingest(_ context.Context, l Ledger) error {
	t0 := time.Now()
	var (
		payloads []events.Payload
		err      error
	)
	if e.xdrViews {
		payloads, err = extractEventsView(pubnetPassphrase, l.Raw, e.payloadScratch)
		e.payloadScratch = payloads // retain grown buffer for the next ledger
	} else {
		if l.LCM == nil {
			return fmt.Errorf("EventsHot is parsed-mode but ledger %d has no LCM", l.Seq)
		}
		payloads, err = extractEventsParsed(pubnetPassphrase, *l.LCM)
	}
	if err != nil {
		return fmt.Errorf("extract seq %d: %w", l.Seq, err)
	}
	extractDur := time.Since(t0)

	t1 := time.Now()
	if err := e.store.IngestLedgerEvents(l.Seq, payloads); err != nil {
		return fmt.Errorf("IngestLedgerEvents(seq=%d, n=%d): %w", l.Seq, len(payloads), err)
	}
	writeDur := time.Since(t1)

	e.collector.samples = append(e.collector.samples, eventSample{
		Items: len(payloads), Extract: extractDur, HotWrite: writeDur,
	})
	return nil
}

func (e *EventsHot) Close() error { return e.store.Close() }

// ───────────────────────── Cold ingester ─────────────────────────

// EventsCold models the backfill path: per-ledger LCM → payloads →
// term-index accumulate + cold append, then chunk-end Finish +
// WriteColdIndex. No HotStore is involved (matches the
// events/cold_index.go backfill contract: maintain an in-memory
// events.Bitmaps via NewBitmaps + per-event TermsFor).
type EventsCold struct {
	chunkID   chunk.ID
	xdrViews  bool
	writer    *eventstore.ColdWriter
	mirror    events.Bitmaps
	offsets   *events.LedgerOffsets
	bucketDir string
	collector *EventsCollector

	payloadScratch []events.Payload // reused across ledgers (view path) to avoid a per-ledger alloc
}

// EventsColdOpts is per-packfile tuning for the events.pack writer.
type EventsColdOpts struct {
	Concurrency  int
	BytesPerSync int
}

func NewEventsCold(c *EventsCollector, outRoot string, chunkID chunk.ID, opts EventsColdOpts, xdrViews bool) (*EventsCold, error) {
	bucketDir := filepath.Join(outRoot, chunkID.BucketID())
	w, err := eventstore.NewColdWriter(chunkID, bucketDir, eventstore.ColdWriterOptions{
		Concurrency:  opts.Concurrency,
		BytesPerSync: opts.BytesPerSync,
	})
	if err != nil {
		return nil, fmt.Errorf("eventstore.NewColdWriter: %w", err)
	}
	return &EventsCold{
		chunkID: chunkID, xdrViews: xdrViews,
		writer: w, mirror: events.NewBitmaps(),
		offsets:   events.NewLedgerOffsets(chunkID.FirstLedger()),
		bucketDir: bucketDir, collector: c,
	}, nil
}

func (e *EventsCold) Ingest(_ context.Context, l Ledger) error {
	t0 := time.Now()
	var (
		payloads []events.Payload
		err      error
	)
	if e.xdrViews {
		payloads, err = extractEventsView(pubnetPassphrase, l.Raw, e.payloadScratch)
		e.payloadScratch = payloads // retain grown buffer for the next ledger
	} else {
		if l.LCM == nil {
			return fmt.Errorf("EventsCold is parsed-mode but ledger %d has no LCM", l.Seq)
		}
		payloads, err = extractEventsParsed(pubnetPassphrase, *l.LCM)
	}
	if err != nil {
		return fmt.Errorf("extract seq %d: %w", l.Seq, err)
	}
	extractDur := time.Since(t0)

	startID := e.offsets.TotalEvents()
	if uint64(startID)+uint64(len(payloads)) > math.MaxUint32 {
		return fmt.Errorf("chunk %s would overflow uint32 event-id space at ledger %d", e.chunkID, l.Seq)
	}

	// Empty-payload ledger: short-circuit before any timer fires so the
	// percentile filter isn't polluted by sub-microsecond non-zero stage
	// samples. offsets.Append is still called below to preserve the
	// LedgerOffsets monotonicity invariant.
	if len(payloads) == 0 {
		if oerr := e.offsets.Append(l.Seq, 0); oerr != nil {
			return fmt.Errorf("offsets append seq %d: %w", l.Seq, oerr)
		}
		e.collector.samples = append(e.collector.samples, eventSample{
			Items: 0, Extract: extractDur,
		})
		return nil
	}

	// Term-index stage: derive keys (TermsFor in the parsed path, or
	// reuse precomputed Terms when present in view-extracted payloads)
	// and AddTo the in-memory mirror.
	tTerm := time.Now()
	for i := range payloads {
		keys, ok := payloads[i].TermKeys()
		if !ok {
			keys, err = events.TermsFor(payloads[i].ContractEvent)
			if err != nil {
				return fmt.Errorf("TermsFor seq %d eventIdx %d: %w", l.Seq, i, err)
			}
		}
		eventID := startID + uint32(i)
		for _, k := range keys {
			e.mirror.AddTo(k, eventID)
		}
	}
	termDur := time.Since(tTerm)

	// Cold-append stage: write each payload to events.pack.
	tCold := time.Now()
	for i := range payloads {
		if appendErr := e.writer.Append(payloads[i]); appendErr != nil {
			return fmt.Errorf("cold Append seq %d eventIdx %d: %w", l.Seq, i, appendErr)
		}
	}
	appendDur := time.Since(tCold)

	// offsets.Append LAST — it is the commit point for the ledger.
	// If any earlier stage failed, offsets isn't advanced and the
	// chunk state stays recoverable.
	if oerr := e.offsets.Append(l.Seq, uint32(len(payloads))); oerr != nil {
		return fmt.Errorf("offsets append seq %d: %w", l.Seq, oerr)
	}

	e.collector.samples = append(e.collector.samples, eventSample{
		Items:      len(payloads),
		Extract:    extractDur,
		TermIndex:  termDur,
		ColdAppend: appendDur,
	})
	return nil
}

// Finalize writes the events.pack trailer (Finish) + materializes the
// cold index (WriteColdIndex). Errors from either step indicate the
// chunk did not durably land.
func (e *EventsCold) Finalize(ctx context.Context) error {
	t0 := time.Now()
	if err := e.writer.Finish(e.offsets); err != nil {
		return fmt.Errorf("events ColdWriter.Finish: %w", err)
	}
	if err := eventstore.WriteColdIndex(ctx, e.chunkID, e.mirror, e.bucketDir); err != nil {
		return fmt.Errorf("WriteColdIndex: %w", err)
	}
	e.collector.finish = append(e.collector.finish, time.Since(t0))
	return nil
}

func (e *EventsCold) Close() error { return e.writer.Close() }
