package ingest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
)

// ───────────────────────── Cold ingester ─────────────────────────

// ledgerCold writes raw ledger bytes into a per-chunk ledger.ColdWriter (one
// packfile per chunk). Finalize calls Commit (trailer + fsync). Close cleans up
// the partial file when Finalize never ran (idempotent — no-op after Commit).
type ledgerCold struct {
	path     string
	writer   *ledger.ColdWriter
	metrics  coldMetrics
	appended bool
}

// NewLedgerColdIngester opens a per-chunk cold ledger writer at packPath — the
// caller's geometry.Layout.LedgerPackPath(chunkID), so the write path is Layout's
// single derivation, not a second copy — and returns a ColdIngester that owns it.
// The writer uses its zero-value options; driver-level tuning is a follow-up (issue #836).
func NewLedgerColdIngester(packPath string, chunkID chunk.ID, sink MetricSink) (ColdIngester, error) {
	if err := os.MkdirAll(filepath.Dir(packPath), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", filepath.Dir(packPath), err)
	}
	w, err := ledger.NewColdWriter(packPath, chunkID.FirstLedger(), ledger.ColdWriterOptions{})
	if err != nil {
		return nil, fmt.Errorf("ledger.NewColdWriter %s: %w", packPath, err)
	}
	return &ledgerCold{path: packPath, writer: w, metrics: newColdMetrics(sink, dataTypeLedgers)}, nil
}

func (c *ledgerCold) Ingest(_ context.Context, seq uint32, lcm xdr.LedgerCloseMetaView) error {
	start := time.Now()
	if err := c.writer.AppendLedger(seq, []byte(lcm)); err != nil {
		c.metrics.observe(time.Since(start), 0, err) // terminal: observe emits the per-ingester signal
		return fmt.Errorf("AppendLedger(seq=%d): %w", seq, err)
	}
	c.metrics.sink.IngestStage(dataTypeLedgers, stageWrite, time.Since(start), 1)
	c.appended = true
	c.metrics.observe(time.Since(start), 1, nil)
	return nil
}

func (c *ledgerCold) Finalize(_ context.Context) error {
	start := time.Now()
	// ColdWriter.Commit errors on a zero-append pack. If the per-ledger loop
	// bailed out before any AppendLedger succeeded, skip Commit so the failure
	// surface is clean (Close in deferred cleanup removes the partial pack).
	if !c.appended {
		c.metrics.emit(time.Since(start), nil)
		return nil
	}
	if err := c.writer.Commit(); err != nil {
		err = fmt.Errorf("ledger ColdWriter.Commit: %w", err)
		c.metrics.emit(time.Since(start), err)
		return err
	}
	c.metrics.sink.IngestStage(dataTypeLedgers, stageFinalize, time.Since(start), 0)
	c.metrics.emit(time.Since(start), nil)
	return nil
}

// Close drops the partial pack when Finalize never ran. It does NOT emit the cold
// metric: a terminal Ingest error or Finalize already emitted it, and an ingester
// that never got that far (a rolled-back build) must produce no phantom sample.
// The writer.Close error is returned unchanged.
func (c *ledgerCold) Close() error {
	return c.writer.Close()
}
