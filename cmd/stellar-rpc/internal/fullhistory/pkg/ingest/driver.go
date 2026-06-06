package ingest

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"golang.org/x/sync/errgroup"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// streamFactory opens an independent LedgerStream for one chunk. Each cold
// chunk worker uses it to acquire its own stream (a BSB cursor cannot be
// shared); it is also the injection seam for tests.
type streamFactory func(chunkID chunk.ID) (ledgerbackend.LedgerStream, error)

// buildLedger turns one ledger's raw bytes (borrowed from the stream) into a
// Ledger. When needsLCM is set (events or txhash enabled), it decodes the raw
// bytes into an xdr.LedgerCloseMeta once, shared read-only across the enabled
// ingesters. When only ledgers are enabled, the decode is skipped (the ledgers
// ingesters write raw bytes verbatim).
//
// raw is BORROWED and valid only for the current iteration step; the resulting
// Ledger must be fully consumed before the next stream yield, which the driver
// guarantees by waiting on the per-ledger errgroup before pulling again.
func buildLedger(seq uint32, raw []byte, needsLCM bool) (Ledger, error) {
	if !needsLCM {
		return Ledger{Seq: seq, Raw: raw}, nil
	}
	return newParsedLedger(seq, raw)
}

// closeAll closes every ingester, joining each Close error into err. Used in
// the deferred cleanup of both drivers so a store-handle leak or partial-file
// removal failure surfaces on the returned error.
func closeAll[T ingester](ings []T, err error) error {
	for _, ing := range ings {
		if cerr := ing.Close(); cerr != nil {
			err = errors.Join(err, fmt.Errorf("close: %w", cerr))
		}
	}
	return err
}

// RunHot streams a single chunk's ledgers from stream and fans each one out to
// the enabled hot ingesters, writing into per-type subdirectories under hotDir
// (hotDir/ledgers, hotDir/txhash, hotDir/events). Ingest errors abort the run
// fast; the per-ledger errgroup gates the next stream pull so the borrowed raw
// slice is never read past its lifetime. Ingester Close is deferred and
// idempotent.
func RunHot(
	ctx context.Context,
	logger *supportlog.Entry,
	stream ledgerbackend.LedgerStream,
	chunkID chunk.ID,
	hotDir string,
	cfg Config,
) (err error) {
	if verr := cfg.validate(); verr != nil {
		return verr
	}

	var ings []ingester
	if cfg.Ledgers {
		ing, ierr := newLedgersHot(filepath.Join(hotDir, "ledgers"), logger)
		if ierr != nil {
			return closeAll(ings, fmt.Errorf("open ledgers hot: %w", ierr))
		}
		ings = append(ings, ing)
	}
	if cfg.Txhash {
		ing, ierr := newTxhashHot(filepath.Join(hotDir, "txhash"), logger)
		if ierr != nil {
			return closeAll(ings, fmt.Errorf("open txhash hot: %w", ierr))
		}
		ings = append(ings, ing)
	}
	if cfg.Events {
		ing, ierr := newEventsHot(filepath.Join(hotDir, "events"), chunkID, logger, cfg.Passphrase)
		if ierr != nil {
			return closeAll(ings, fmt.Errorf("open events hot: %w", ierr))
		}
		ings = append(ings, ing)
	}
	defer func() { err = closeAll(ings, err) }()

	first, last := chunkID.FirstLedger(), chunkID.LastLedger()
	needsLCM := cfg.needsLCM()
	seq := first
	for raw, serr := range stream.RawLedgers(ctx, ledgerbackend.BoundedRange(first, last)) {
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		if serr != nil {
			return fmt.Errorf("RawLedgers(%d): %w", seq, serr)
		}
		l, lerr := buildLedger(seq, raw, needsLCM)
		if lerr != nil {
			return lerr
		}
		if ferr := fanOut(ctx, ings, l); ferr != nil {
			return ferr
		}
		seq++
	}
	return nil
}

// RunCold ingests numChunks consecutive chunks starting at startChunk into the
// cold stores under coldDir, processing up to chunkWorkers chunks concurrently.
// Each chunk worker opens its own stream via OpenChunkStream(src, opts, id).
func RunCold(
	ctx context.Context,
	logger *supportlog.Entry,
	src Source,
	opts SourceOpts,
	coldDir string,
	startChunk chunk.ID,
	numChunks, chunkWorkers int,
	cfg Config,
) error {
	factory := func(id chunk.ID) (ledgerbackend.LedgerStream, error) {
		return OpenChunkStream(src, opts, id)
	}
	return runColdWithStreamFactory(ctx, logger, factory, coldDir, startChunk, numChunks, chunkWorkers, cfg)
}

// runColdWithStreamFactory is the testable core of RunCold: it orchestrates the
// chunk range using factory to open each worker's stream. Tests inject a fake
// stream factory here.
func runColdWithStreamFactory(
	ctx context.Context,
	logger *supportlog.Entry,
	factory streamFactory,
	coldDir string,
	startChunk chunk.ID,
	numChunks, chunkWorkers int,
	cfg Config,
) error {
	if verr := cfg.validate(); verr != nil {
		return verr
	}
	if numChunks < 1 {
		return fmt.Errorf("ingest: numChunks must be >= 1, got %d", numChunks)
	}
	if chunkWorkers < 1 {
		return fmt.Errorf("ingest: chunkWorkers must be >= 1, got %d", chunkWorkers)
	}
	if chunkWorkers > numChunks {
		logger.Infof("chunkWorkers=%d > numChunks=%d; clamping to %d", chunkWorkers, numChunks, numChunks)
		chunkWorkers = numChunks
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(chunkWorkers)
	for i := range numChunks {
		chunkID := startChunk + chunk.ID(uint32(i))
		g.Go(func() error {
			if rerr := runOneChunkCold(gctx, factory, coldDir, chunkID, cfg); rerr != nil {
				return fmt.Errorf("chunk %d: %w", uint32(chunkID), rerr)
			}
			return nil
		})
	}
	return g.Wait()
}

// runOneChunkCold processes a single chunk: opens its own stream via factory,
// builds the enabled cold ingesters, runs the per-ledger fan-out, then calls
// Finalize on each cold ingester (explicit, error-checked, not deferred).
// Ingester Close is deferred and idempotent — on the failure path it removes
// any partial cold artifact for writers whose Finalize never ran.
func runOneChunkCold(
	ctx context.Context,
	factory streamFactory,
	coldDir string,
	chunkID chunk.ID,
	cfg Config,
) (err error) {
	stream, oerr := factory(chunkID)
	if oerr != nil {
		return oerr
	}

	var ings []coldIngester
	if cfg.Ledgers {
		ing, ierr := newLedgersCold(filepath.Join(coldDir, "ledgers"), chunkID, LedgersColdOpts{})
		if ierr != nil {
			return closeAll(ings, fmt.Errorf("open ledgers cold: %w", ierr))
		}
		ings = append(ings, ing)
	}
	if cfg.Txhash {
		ing, ierr := newTxhashCold(filepath.Join(coldDir, "txhash"), chunkID)
		if ierr != nil {
			return closeAll(ings, fmt.Errorf("open txhash cold: %w", ierr))
		}
		ings = append(ings, ing)
	}
	if cfg.Events {
		ing, ierr := newEventsCold(filepath.Join(coldDir, "events"), chunkID, EventsColdOpts{}, cfg.Passphrase)
		if ierr != nil {
			return closeAll(ings, fmt.Errorf("open events cold: %w", ierr))
		}
		ings = append(ings, ing)
	}
	defer func() { err = closeAll(ings, err) }()

	first, last := chunkID.FirstLedger(), chunkID.LastLedger()
	needsLCM := cfg.needsLCM()
	seq := first
	for raw, serr := range stream.RawLedgers(ctx, ledgerbackend.BoundedRange(first, last)) {
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		if serr != nil {
			return fmt.Errorf("RawLedgers(%d): %w", seq, serr)
		}
		l, lerr := buildLedger(seq, raw, needsLCM)
		if lerr != nil {
			return lerr
		}
		if ferr := fanOutCold(ctx, ings, l); ferr != nil {
			return ferr
		}
		seq++
	}

	// Finalize each chunk's cold artifact. Explicit and error-checked — never
	// deferred. A Finalize error means the chunk did not durably land.
	for _, ing := range ings {
		if ferr := ing.Finalize(ctx); ferr != nil {
			return fmt.Errorf("finalize: %w", ferr)
		}
	}
	return nil
}

// fanOut runs every hot ingester on l concurrently, waiting for all to finish
// before returning so the borrowed raw is safe to release. The first Ingest
// error cancels the siblings via the errgroup context.
func fanOut(ctx context.Context, ings []ingester, l Ledger) error {
	g, gctx := errgroup.WithContext(ctx)
	for _, ing := range ings {
		g.Go(func() error { return ing.Ingest(gctx, l) })
	}
	return g.Wait()
}

// fanOutCold is fanOut for the cold-ingester slice type.
func fanOutCold(ctx context.Context, ings []coldIngester, l Ledger) error {
	g, gctx := errgroup.WithContext(ctx)
	for _, ing := range ings {
		g.Go(func() error { return ing.Ingest(gctx, l) })
	}
	return g.Wait()
}
