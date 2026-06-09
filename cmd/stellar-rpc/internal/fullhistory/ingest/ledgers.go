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

// ───────────────────────── Hot ingester ─────────────────────────

// ledgerHot writes raw ledger bytes verbatim into a long-lived ledger.HotStore.
// AddLedgers fsyncs once per call, so each ledger is durable before Ingest
// returns. The store is INJECTED and owned by the caller — ledgerHot never
// opens or closes it.
type ledgerHot struct {
	store *ledger.HotStore
	sink  MetricSink
}

// NewLedgerHotIngester returns a HotIngester writing raw ledger bytes into the
// injected, caller-owned store.
func NewLedgerHotIngester(store *ledger.HotStore, sink MetricSink) HotIngester {
	return &ledgerHot{store: store, sink: orNop(sink)}
}

func (h *ledgerHot) Ingest(_ context.Context, lcm xdr.LedgerCloseMetaView) (err error) {
	m := newHotMetrics(h.sink, dataTypeLedgers)
	defer func() { m.emit(err) }()

	seq, serr := ledgerSeqOf(lcm)
	if serr != nil {
		return fmt.Errorf("ledger seq: %w", serr)
	}
	// ledger.HotStore.AddLedgers copies the bytes into its RocksDB batch
	// synchronously, so aliasing the borrowed view buffer here is safe.
	m.items = 1
	if aerr := h.store.AddLedgers(ledger.Entry{Seq: seq, Bytes: []byte(lcm)}); aerr != nil {
		return fmt.Errorf("AddLedgers(seq=%d): %w", seq, aerr)
	}
	return nil
}

// ───────────────────────── Cold ingester ─────────────────────────

// ledgerCold writes raw ledger bytes into a per-chunk ledger.ColdWriter (one
// packfile per chunk). Finalize calls Commit (trailer + fsync). Close cleans up
// the partial file when Finalize never ran (idempotent — no-op after Commit).
type ledgerCold struct {
	writer   *ledger.ColdWriter
	metrics  coldMetrics
	appended bool
}

// NewLedgerColdIngester opens a per-chunk cold ledger writer under coldDir and
// returns a ColdIngester that owns it. The writer uses its zero-value options;
// driver-level tuning is a follow-up via Config.
func NewLedgerColdIngester(coldDir string, chunkID chunk.ID, sink MetricSink) (ColdIngester, error) {
	path := packPath(coldDir, chunkID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", filepath.Dir(path), err)
	}
	w, err := ledger.NewColdWriter(path, chunkID.FirstLedger(), ledger.ColdWriterOptions{})
	if err != nil {
		return nil, fmt.Errorf("ledger.NewColdWriter %s: %w", path, err)
	}
	return &ledgerCold{writer: w, metrics: newColdMetrics(sink, dataTypeLedgers)}, nil
}

func (c *ledgerCold) Ingest(_ context.Context, lcm xdr.LedgerCloseMetaView) error {
	start := time.Now()
	seq, err := ledgerSeqOf(lcm)
	if err != nil {
		c.metrics.observe(time.Since(start), 0, err)
		return fmt.Errorf("ledger seq: %w", err)
	}
	if err := c.writer.AppendLedger(seq, []byte(lcm)); err != nil {
		c.metrics.observe(time.Since(start), 0, err)
		return fmt.Errorf("AppendLedger(seq=%d): %w", seq, err)
	}
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
	c.metrics.emit(time.Since(start), nil)
	return nil
}

// Close drops the partial pack when Finalize never ran, and emits the cold
// metrics if Finalize did not already (the failure path). The writer.Close
// error is folded into the emitted metric so a close-time failure is counted in
// errors_total. emit is a no-op after a successful Finalize, so this never
// double-counts. Error propagation is unchanged: the writer.Close error is
// still returned.
func (c *ledgerCold) Close() error {
	cerr := c.writer.Close()
	c.metrics.emit(0, cerr)
	return cerr
}

// abortMetric records a synthetic abort error so a subsequent Close emit does
// not look like a clean success. Used by the constructor-rollback path.
func (c *ledgerCold) abortMetric(err error) { c.metrics.recordErr(err) }
