package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
)

// ───────────────────────── Collector ─────────────────────────

// ledgerSample is one ledger's measurement for both hot and cold tiers.
// Hot tier records Write only; cold tier also records Write per ledger
// (AppendLedger latency). Per-chunk scalars (commit) live separately.
type ledgerSample struct {
	Write time.Duration
}

// LedgerCollector accumulates ledger-tier samples for the run.
// Under multi-chunk runs, commit is one entry per chunk; under
// single-chunk runs it is a one-element slice. Reporting handles
// both cases uniformly via the existing percentile machinery.
type LedgerCollector struct {
	samples []ledgerSample
	commit  []time.Duration // cold-only; ColdWriter.Commit duration per chunk
}

func NewLedgerCollector(n int) *LedgerCollector {
	return &LedgerCollector{samples: make([]ledgerSample, 0, n)}
}

// Merge folds other's samples + commit entries into this collector.
// Called by the cold driver after a multi-chunk run to aggregate
// per-worker collectors into the single report-time collector.
func (c *LedgerCollector) Merge(other *LedgerCollector) {
	c.samples = append(c.samples, other.samples...)
	c.commit = append(c.commit, other.commit...)
}

// perLedgerRows computes the nonzero-filtered per-ledger stage rows in a
// single pass. PrintSummary, WriteCSV, and InPipelineTime all consume
// these rows, so the filtering lives in one place rather than being
// re-derived per method. Row names match the CSV schema; for ledgers,
// n_items is one per sample (one written ledger).
//
//nolint:funcorder // helper for the exported summary methods below
func (c *LedgerCollector) perLedgerRows() []stageRow {
	writes := make([]time.Duration, 0, len(c.samples))
	for _, s := range c.samples {
		if s.Write > 0 {
			writes = append(writes, s.Write)
		}
	}
	return []stageRow{{name: "write", durs: writes, items: len(c.samples)}}
}

// Writes returns the per-ledger write duration of every sample (incl.
// empties), for the cold parity line — without exposing the sample shape.
func (c *LedgerCollector) Writes() []time.Duration {
	out := make([]time.Duration, len(c.samples))
	for i, s := range c.samples {
		out[i] = s.Write
	}
	return out
}

// PrintSummary writes the per-stage percentile summary lines to w.
// tier is "hot" or "cold"; affects only labeling.
func (c *LedgerCollector) PrintSummary(tier string, w io.Writer) {
	rows := c.perLedgerRows()
	printNamedStage(w, tier+".ledgers.write", rows, "write", len(c.samples))
	if tier == "cold" {
		printChunkScalar(w, "cold.ledgers.commit", c.commit)
	}
}

// InPipelineTime returns the sum of per-ledger work (plus per-chunk
// finalize for cold) attributed to this data type. Used by the
// throughput line as the "extract+write" denominator so the rate is
// per-type, not shared with other types' fan-out time.
func (c *LedgerCollector) InPipelineTime() time.Duration {
	total := stageRowsTotal(c.perLedgerRows())
	for _, d := range c.commit {
		total += d
	}
	return total
}

// WriteCSV writes the per-stage aggregation CSV for this collector.
// filenamePrefix is the leading part of the filename (e.g.,
// "hot-ledgers-view"); the suffix ".csv" is appended.
func (c *LedgerCollector) WriteCSV(outDir, filenamePrefix string) error {
	return writeStageCSV(outDir, filenamePrefix, c.perLedgerRows())
}

// ───────────────────────── Hot ingester ─────────────────────────

// LedgersHot writes raw ledger bytes verbatim into a ledger.HotStore.
// AddLedgers(entry) for a single entry uses Store.Put with Sync=true,
// so each call WAL-fsyncs before returning (durability per ledger).
type LedgersHot struct {
	store     *ledger.HotStore
	collector *LedgerCollector
}

func NewLedgersHot(c *LedgerCollector, dir string, logger *supportlog.Entry) (*LedgersHot, error) {
	if err := os.MkdirAll(filepath.Dir(dir), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir parent of %s: %w", dir, err)
	}
	store, err := ledger.OpenHotStore(dir, logger)
	if err != nil {
		return nil, fmt.Errorf("ledger.OpenHotStore %s: %w", dir, err)
	}
	return &LedgersHot{store: store, collector: c}, nil
}

func (h *LedgersHot) Ingest(_ context.Context, l Ledger) error {
	t0 := time.Now()
	if err := h.store.AddLedgers(ledger.Entry{Seq: l.Seq, Bytes: l.Raw}); err != nil {
		return fmt.Errorf("AddLedgers(seq=%d): %w", l.Seq, err)
	}
	h.collector.samples = append(h.collector.samples, ledgerSample{Write: time.Since(t0)})
	return nil
}

func (h *LedgersHot) Close() error { return h.store.Close() }

// ───────────────────────── Cold ingester ─────────────────────────

// LedgersCold writes raw ledger bytes into a per-chunk ledger.ColdWriter
// (one packfile per chunk). Finalize calls Commit which finalizes the
// trailer + fsyncs. Close cleans up the partial file when Finalize
// never ran (idempotent — no-op after Commit).
type LedgersCold struct {
	writer    *ledger.ColdWriter
	collector *LedgerCollector
}

// LedgersColdOpts is the per-packfile tuning passed to the underlying
// writer. Zero-value picks library defaults (serial, no writeback).
type LedgersColdOpts struct {
	Concurrency  int
	BytesPerSync int
}

func NewLedgersCold(c *LedgerCollector, outRoot string, chunkID chunk.ID, opts LedgersColdOpts) (*LedgersCold, error) {
	path := packPath(outRoot, uint32(chunkID))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", filepath.Dir(path), err)
	}
	w, err := ledger.NewColdWriter(path, chunkID.FirstLedger(), ledger.ColdWriterOptions{
		Concurrency:  opts.Concurrency,
		BytesPerSync: opts.BytesPerSync,
	})
	if err != nil {
		return nil, fmt.Errorf("ledger.NewColdWriter %s: %w", path, err)
	}
	return &LedgersCold{writer: w, collector: c}, nil
}

func (c *LedgersCold) Ingest(_ context.Context, l Ledger) error {
	t0 := time.Now()
	if err := c.writer.AppendLedger(l.Seq, l.Raw); err != nil {
		return fmt.Errorf("AppendLedger(seq=%d): %w", l.Seq, err)
	}
	c.collector.samples = append(c.collector.samples, ledgerSample{Write: time.Since(t0)})
	return nil
}

func (c *LedgersCold) Finalize(_ context.Context) error {
	// ColdWriter.Commit errors on a zero-append pack ("commit with no
	// appends"). If the per-ledger loop bailed out before any
	// AppendLedger succeeded, skip Commit so the failure surface is
	// clean (Close in deferred cleanup removes the partial pack).
	if len(c.collector.samples) == 0 {
		return nil
	}
	t0 := time.Now()
	if err := c.writer.Commit(); err != nil {
		return fmt.Errorf("ledger ColdWriter.Commit: %w", err)
	}
	c.collector.commit = append(c.collector.commit, time.Since(t0))
	return nil
}

func (c *LedgersCold) Close() error { return c.writer.Close() }
