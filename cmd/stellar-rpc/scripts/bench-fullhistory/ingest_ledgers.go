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
// Per-chunk scalar `commit` is populated by the cold ingester at
// Finalize time and printed in the summary; not included in the
// per-stage CSV.
type LedgerCollector struct {
	samples []ledgerSample
	commit  time.Duration // cold-only; ColdWriter.Commit duration
}

func NewLedgerCollector(n int) *LedgerCollector {
	return &LedgerCollector{samples: make([]ledgerSample, 0, n)}
}

// PrintSummary writes the per-stage percentile summary lines to w.
// tier is "hot" or "cold"; affects only labeling.
func (c *LedgerCollector) PrintSummary(tier string, w io.Writer) {
	writes := make([]time.Duration, 0, len(c.samples))
	for _, s := range c.samples {
		if s.Write > 0 {
			writes = append(writes, s.Write)
		}
	}
	printStageSummary(w, tier+".ledgers.write", writes, len(c.samples))
	if tier == "cold" {
		fmt.Fprintf(w, "  cold.ledgers.commit       = %s\n", c.commit.Round(time.Microsecond))
	}
}

// InPipelineTime returns the sum of per-ledger work (plus per-chunk
// finalize for cold) attributed to this data type. Used by the
// throughput line as the "extract+write" denominator so the rate is
// per-type, not shared with other types' fan-out time.
func (c *LedgerCollector) InPipelineTime() time.Duration {
	var total time.Duration
	for _, s := range c.samples {
		total += s.Write
	}
	total += c.commit
	return total
}

// WriteCSV writes the per-stage aggregation CSV for this collector.
// filenamePrefix is the leading part of the filename (e.g.,
// "hot-ledgers-view"); the suffix ".csv" is appended.
func (c *LedgerCollector) WriteCSV(outDir, filenamePrefix string) error {
	writes := make([]time.Duration, 0, len(c.samples))
	for _, s := range c.samples {
		if s.Write > 0 {
			writes = append(writes, s.Write)
		}
	}
	return writeStageCSV(outDir, filenamePrefix, []stageRow{
		{name: "write", durs: writes, items: len(c.samples)},
	})
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
	c.collector.commit = time.Since(t0)
	return nil
}

func (c *LedgersCold) Close() error { return c.writer.Close() }
