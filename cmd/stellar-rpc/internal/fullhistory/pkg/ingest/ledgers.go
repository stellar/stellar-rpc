package ingest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
)

// ───────────────────────── Hot ingester ─────────────────────────

// ledgersHot writes raw ledger bytes verbatim into a ledger.HotStore.
// AddLedgers fsyncs once per call, so each ledger is durable before Ingest
// returns.
type ledgersHot struct {
	store *ledger.HotStore
}

func newLedgersHot(dir string, logger *supportlog.Entry) (*ledgersHot, error) {
	if err := os.MkdirAll(filepath.Dir(dir), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir parent of %s: %w", dir, err)
	}
	store, err := ledger.OpenHotStore(dir, logger)
	if err != nil {
		return nil, fmt.Errorf("ledger.OpenHotStore %s: %w", dir, err)
	}
	return &ledgersHot{store: store}, nil
}

func (h *ledgersHot) Ingest(_ context.Context, l Ledger) error {
	if err := h.store.AddLedgers(ledger.Entry{Seq: l.Seq, Bytes: l.Raw}); err != nil {
		return fmt.Errorf("AddLedgers(seq=%d): %w", l.Seq, err)
	}
	return nil
}

func (h *ledgersHot) Close() error { return h.store.Close() }

// ───────────────────────── Cold ingester ─────────────────────────

// LedgersColdOpts is the per-packfile tuning passed to the underlying writer.
// The zero value picks library defaults (serial, no writeback).
type LedgersColdOpts struct {
	Concurrency  int
	BytesPerSync int
}

// ledgersCold writes raw ledger bytes into a per-chunk ledger.ColdWriter (one
// packfile per chunk). Finalize calls Commit, which finalizes the trailer and
// fsyncs. Close cleans up the partial file when Finalize never ran (idempotent
// — no-op after Commit).
type ledgersCold struct {
	writer   *ledger.ColdWriter
	appended bool
}

func newLedgersCold(outRoot string, chunkID chunk.ID, opts LedgersColdOpts) (*ledgersCold, error) {
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
	return &ledgersCold{writer: w}, nil
}

func (c *ledgersCold) Ingest(_ context.Context, l Ledger) error {
	if err := c.writer.AppendLedger(l.Seq, l.Raw); err != nil {
		return fmt.Errorf("AppendLedger(seq=%d): %w", l.Seq, err)
	}
	c.appended = true
	return nil
}

func (c *ledgersCold) Finalize(_ context.Context) error {
	// ColdWriter.Commit errors on a zero-append pack. If the per-ledger loop
	// bailed out before any AppendLedger succeeded, skip Commit so the failure
	// surface is clean (Close in deferred cleanup removes the partial pack).
	if !c.appended {
		return nil
	}
	if err := c.writer.Commit(); err != nil {
		return fmt.Errorf("ledger ColdWriter.Commit: %w", err)
	}
	return nil
}

func (c *ledgersCold) Close() error { return c.writer.Close() }
