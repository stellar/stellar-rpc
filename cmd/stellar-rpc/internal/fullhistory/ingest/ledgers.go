package ingest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/ledger"
)

// ───────────────────────── Cold writer ─────────────────────────

// ledgerCold writes raw ledger bytes into a per-chunk ledger.ColdWriter (one
// packfile per chunk). finalize calls Commit (trailer + fsync). close cleans up
// the partial file when finalize never ran (idempotent — no-op after Commit).
type ledgerCold struct {
	path    string
	writer  *ledger.ColdWriter
	metrics coldMetrics
}

// newLedgerCold opens a per-chunk cold ledger writer at packPath — the
// caller's geometry.Layout.LedgerPackPath(chunkID), so the write path is Layout's
// single derivation, not a second copy. The writer opts into the batch tuning
// (coldEncoderConcurrency/coldBytesPerSync): WriteColdChunk, the sole production
// caller, is always a batch freeze/backfill.
func newLedgerCold(packPath string, chunkID chunk.ID, sink MetricSink) (*ledgerCold, error) {
	if err := os.MkdirAll(filepath.Dir(packPath), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", filepath.Dir(packPath), err)
	}
	w, err := ledger.NewColdWriter(packPath, chunkID.FirstLedger(), ledger.ColdWriterOptions{
		Concurrency:  coldEncoderConcurrency,
		BytesPerSync: coldBytesPerSync,
	})
	if err != nil {
		return nil, fmt.Errorf("ledger.NewColdWriter %s: %w", packPath, err)
	}
	return &ledgerCold{path: packPath, writer: w, metrics: newColdMetrics(sink, dataTypeLedgers)}, nil
}

// write appends one ledger's raw wire bytes. raw ALIASES the source stream's
// borrowed buffer, valid only for this call — AppendLedger copies it
// synchronously.
func (c *ledgerCold) write(seq uint32, raw []byte) error {
	start := time.Now()
	if err := c.writer.AppendLedger(seq, raw); err != nil {
		c.metrics.observe(time.Since(start), 0, err) // terminal: observe emits the per-writer signal
		return fmt.Errorf("AppendLedger(seq=%d): %w", seq, err)
	}
	c.metrics.sink.IngestStage(dataTypeLedgers, stageWrite, time.Since(start), 1)
	c.metrics.observe(time.Since(start), 1, nil)
	return nil
}

func (c *ledgerCold) finalize(_ context.Context) error {
	start := time.Now()
	if err := c.writer.Commit(); err != nil {
		err = fmt.Errorf("ledger ColdWriter.Commit: %w", err)
		c.metrics.emit(time.Since(start), err)
		return err
	}
	c.metrics.sink.IngestStage(dataTypeLedgers, stageFinalize, time.Since(start), 0)
	c.metrics.emit(time.Since(start), nil)
	return nil
}

// close drops the partial pack when finalize never ran. It does NOT emit the cold
// metric: a terminal write error or finalize already emitted it, and a writer
// that never got that far (a rolled-back build) must produce no phantom sample.
// The writer.Close error is returned unchanged.
func (c *ledgerCold) close() error {
	return c.writer.Close()
}
