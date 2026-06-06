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
	l, err := newParsedLedger(seq, raw)
	if err != nil {
		return Ledger{}, err
	}
	// Guard against a misordered/mislabeled backend before it corrupts the
	// event-ID offsets: the decoded ledger seq must match what the driver
	// expects at this position in the range.
	if got := l.LCM.LedgerSequence(); got != seq {
		return Ledger{}, fmt.Errorf("ingest: ledger at expected seq %d decoded as seq %d", seq, got)
	}
	return l, nil
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

// ingestType is one of the three data types, carrying its config selector and
// its per-tier on-disk subdirectory name. Centralizing these removes the
// per-type copy-paste (the "ledgers"/"txhash"/"events" literals and the
// enable-check branches) from both driver setup paths.
type ingestType struct {
	subdir    string
	enabledIn func(Config) bool
}

var ingestTypes = []ingestType{
	{subdir: "ledgers", enabledIn: func(c Config) bool { return c.Ledgers }},
	{subdir: "txhash", enabledIn: func(c Config) bool { return c.Txhash }},
	{subdir: "events", enabledIn: func(c Config) bool { return c.Events }},
}

// buildIngesters constructs one ingester per data type enabled in cfg, in
// canonical ledgers→txhash→events order. mk constructs a type's ingester given
// its on-disk subdirectory; on any constructor error it closes the ingesters
// built so far (joining errors) and returns. T is ingester for the hot driver
// and coldIngester for the cold driver.
func buildIngesters[T ingester](cfg Config, mk func(subdir string) (T, error)) ([]T, error) {
	var ings []T
	for _, t := range ingestTypes {
		if !t.enabledIn(cfg) {
			continue
		}
		ing, err := mk(t.subdir)
		if err != nil {
			return nil, closeAll(ings, fmt.Errorf("open %s ingester: %w", t.subdir, err))
		}
		ings = append(ings, ing)
	}
	return ings, nil
}

// drainStream pulls the chunk's raw ledgers from stream and fans each out to
// ings, then verifies the full [first,last] range was consumed. The borrowed
// raw is fully consumed inside fanOut (which waits for all ingesters) before
// the next pull. The completeness check lives here so both drivers share one
// error message: a stream that ends early is rejected before any cold Finalize.
func drainStream[T ingester](ctx context.Context, stream ledgerbackend.LedgerStream, chunkID chunk.ID, ings []T, needsLCM bool) error {
	first, last := chunkID.FirstLedger(), chunkID.LastLedger()
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
	// Guard against a stream that ended early (partial chunk, backend stopped
	// without error): the range must have been fully consumed. For the cold
	// path this runs before Finalize, so a short stream never produces a
	// finalized — and thus reader-trusted — but truncated artifact.
	if seq != last+1 {
		return fmt.Errorf("ingest: stream for chunk %d ended at %d, expected through %d", uint32(chunkID), seq-1, last)
	}
	return nil
}

// RunHot opens one stream for chunkID from source and fans each ledger out to
// the enabled hot ingesters, writing into per-type subdirectories under hotDir
// (hotDir/ledgers, hotDir/txhash, hotDir/events). Ingest errors abort the run
// fast; the per-ledger errgroup gates the next stream pull so the borrowed raw
// slice is never read past its lifetime. Ingester Close is deferred and
// idempotent.
func RunHot(
	ctx context.Context,
	logger *supportlog.Entry,
	source ChunkSource,
	chunkID chunk.ID,
	hotDir string,
	cfg Config,
) (err error) {
	if verr := cfg.validate(); verr != nil {
		return verr
	}

	stream, oerr := source.OpenStream(chunkID)
	if oerr != nil {
		return fmt.Errorf("open stream for chunk %d: %w", uint32(chunkID), oerr)
	}

	ings, berr := buildIngesters(cfg, func(subdir string) (ingester, error) {
		dir := filepath.Join(hotDir, subdir)
		switch subdir {
		case "ledgers":
			return newLedgersHot(dir, logger)
		case "txhash":
			return newTxhashHot(dir, logger)
		default: // "events"
			return newEventsHot(dir, chunkID, logger, cfg.Passphrase)
		}
	})
	if berr != nil {
		return berr
	}
	defer func() { err = closeAll(ings, err) }()

	return drainStream(ctx, stream, chunkID, ings, cfg.needsLCM())
}

// RunCold ingests numChunks consecutive chunks starting at startChunk into the
// cold stores under coldDir, processing up to chunkWorkers chunks concurrently.
// Each chunk worker opens its own stream via source.OpenStream(chunkID), so the
// chunks run with fully independent backend pipelines.
func RunCold(
	ctx context.Context,
	logger *supportlog.Entry,
	source ChunkSource,
	coldDir string,
	startChunk chunk.ID,
	numChunks, chunkWorkers int,
	cfg Config,
) error {
	return runColdWithSource(ctx, logger, source, coldDir, startChunk, numChunks, chunkWorkers, cfg)
}

// runColdWithSource is the testable core of RunCold: it orchestrates the chunk
// range, each worker opening its own stream from source. Tests inject a custom
// ChunkSource (e.g. a ChunkSourceFunc) here.
func runColdWithSource(
	ctx context.Context,
	logger *supportlog.Entry,
	source ChunkSource,
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
			if rerr := runOneChunkCold(gctx, source, coldDir, chunkID, cfg); rerr != nil {
				return fmt.Errorf("chunk %d: %w", uint32(chunkID), rerr)
			}
			return nil
		})
	}
	return g.Wait()
}

// runOneChunkCold processes a single chunk: opens its own stream from source,
// builds the enabled cold ingesters, runs the per-ledger fan-out, then calls
// Finalize on each cold ingester (explicit, error-checked, not deferred).
// Ingester Close is deferred and idempotent — on the failure path it removes
// any partial cold artifact for writers whose Finalize never ran.
func runOneChunkCold(
	ctx context.Context,
	source ChunkSource,
	coldDir string,
	chunkID chunk.ID,
	cfg Config,
) (err error) {
	stream, oerr := source.OpenStream(chunkID)
	if oerr != nil {
		return oerr
	}

	ings, berr := buildIngesters(cfg, func(subdir string) (coldIngester, error) {
		dir := filepath.Join(coldDir, subdir)
		switch subdir {
		case "ledgers":
			return newLedgersCold(dir, chunkID, ledgersColdOpts{})
		case "txhash":
			return newTxhashCold(dir, chunkID)
		default: // "events"
			return newEventsCold(dir, chunkID, eventsColdOpts{}, cfg.Passphrase)
		}
	})
	if berr != nil {
		return berr
	}
	defer func() { err = closeAll(ings, err) }()

	if derr := drainStream(ctx, stream, chunkID, ings, cfg.needsLCM()); derr != nil {
		return derr
	}

	// Finalize each chunk's cold artifact. Explicit and error-checked — never
	// deferred. A Finalize error means the chunk did not durably land. drainStream
	// already verified the full range was consumed, so no truncated artifact is
	// finalized.
	for _, ing := range ings {
		if ferr := ing.Finalize(ctx); ferr != nil {
			return fmt.Errorf("finalize: %w", ferr)
		}
	}
	return nil
}

// fanOut runs every ingester on l concurrently, waiting for all to finish
// before returning so the borrowed raw is safe to release. The first Ingest
// error cancels the siblings via the errgroup context. It is generic over the
// ingester type so both the hot ([]ingester) and cold ([]coldIngester) drivers
// share one implementation.
//
// Single-ingester fast path: the common single-type config skips the errgroup
// and goroutine spawn entirely and calls Ingest directly.
func fanOut[T ingester](ctx context.Context, ings []T, l Ledger) error {
	if len(ings) == 1 {
		return ings[0].Ingest(ctx, l)
	}
	g, gctx := errgroup.WithContext(ctx)
	for _, ing := range ings {
		g.Go(func() error { return ing.Ingest(gctx, l) })
	}
	return g.Wait()
}
