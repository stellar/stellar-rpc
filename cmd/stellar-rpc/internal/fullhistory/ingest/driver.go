package ingest

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/hotchunk"
)

// HotStores holds the caller-owned per-chunk hot DB injected into RunHot: the
// daemon opens and closes it, RunHot only borrows it. The DB is chunk-bound, so
// it must already match the chunk being ingested (RunHot rejects a mismatch); a
// nil DB with any data type enabled in cfg is a config error.
type HotStores struct {
	// HotDB is the per-chunk hot DB. Required when any hot data type is enabled.
	HotDB *hotchunk.DB
}

// ingestContributions maps the ingest Config's enabled data types onto the
// hotchunk.Ingest toggles that select which CFs the single per-ledger batch
// writes.
func ingestContributions(cfg Config) hotchunk.Ingest {
	return hotchunk.Ingest{Ledgers: cfg.Ledgers}
}

// buildColdIngesters opens one ColdIngester per data type enabled in cfg,
// each opening its own per-chunk writer under coldDir/<type> (constructors
// create their own directories and freely overwrite any prior attempt's
// files — see the package doc's artifact model). On any constructor error it
// closes the ingesters built so far and returns.
func buildColdIngesters(coldDir string, chunkID chunk.ID, sink MetricSink, cfg Config) ([]ColdIngester, error) {
	ctors := []struct {
		enabled  bool
		dataType string
		open     func(string, chunk.ID, MetricSink) (ColdIngester, error)
	}{
		{cfg.Ledgers, dataTypeLedgers, NewLedgerColdIngester},
	}
	var ings []ColdIngester
	for _, c := range ctors {
		if !c.enabled {
			continue
		}
		ing, err := c.open(filepath.Join(coldDir, c.dataType), chunkID, sink)
		if err != nil {
			return nil, closeColdAll(ings, fmt.Errorf("open %s cold ingester: %w", c.dataType, err))
		}
		ings = append(ings, ing)
	}
	return ings, nil
}

// errColdBuildAborted is the synthetic error recorded against an
// already-built cold ingester's metric when a LATER constructor fails and the
// build is rolled back. Without it, closing a fully-built ingester would emit
// a clean (nil-err, 0-items) ColdIngest — a phantom "success" for a chunk that
// never actually ingested anything.
var errColdBuildAborted = errors.New("ingest: cold ingester build aborted (sibling constructor failed)")

// coldAborter is implemented by the concrete cold ingesters so the
// constructor-rollback path can mark their per-chunk metric as aborted before
// Close emits it, turning what would be a phantom success into a recorded
// abort. Optional: an ingester that does not implement it just gets its normal
// Close emission.
type coldAborter interface {
	abortMetric(err error)
}

// closeColdAll closes every cold ingester built so far, joining each Close error
// into err. Used when a LATER constructor fails mid-build: the already-built
// ingesters never ingested anything, so each one's metric is first marked
// aborted (so the deferred Close emit is not a phantom success).
func closeColdAll(ings []ColdIngester, err error) error {
	for _, ing := range ings {
		if a, ok := ing.(coldAborter); ok {
			a.abortMetric(errColdBuildAborted)
		}
		if cerr := ing.Close(); cerr != nil {
			err = errors.Join(err, fmt.Errorf("close: %w", cerr))
		}
	}
	return err
}

// RunHot opens one stream for chunkID and feeds each ledger (as a borrowed view)
// to a HotService over the INJECTED, caller-owned hot DB in hotStores. Each
// ledger commits as one atomic synced WriteBatch (decision (a)); the view is
// consumed synchronously before the next pull. The hot DB is NOT closed here —
// the caller owns its lifecycle.
func RunHot(
	ctx context.Context,
	logger *supportlog.Entry,
	source ChunkSource,
	chunkID chunk.ID,
	hotStores HotStores,
	sink MetricSink,
	cfg Config,
) error {
	if verr := cfg.validate(); verr != nil {
		return verr
	}
	anyEnabled := cfg.Ledgers
	if anyEnabled && hotStores.HotDB == nil {
		return errors.New("ingest: a hot data type is enabled but HotStores.HotDB is nil")
	}
	// Chunk-bound: an injected DB bound to a different chunk would silently
	// interleave two chunks' data, so catch the mismatch up front.
	if hotStores.HotDB != nil && hotStores.HotDB.ChunkID() != chunkID {
		return fmt.Errorf("ingest: RunHot chunk %d but injected hot DB is bound to chunk %d",
			uint32(chunkID), uint32(hotStores.HotDB.ChunkID()))
	}
	stream, oerr := source.OpenStream(chunkID)
	if oerr != nil {
		return fmt.Errorf("open stream for chunk %d: %w", uint32(chunkID), oerr)
	}
	logger.Debugf("RunHot: ingesting chunk %d [%d, %d]", uint32(chunkID), chunkID.FirstLedger(), chunkID.LastLedger())
	service := NewHotService(hotStores.HotDB, ingestContributions(cfg), sink)
	return drain(ctx, stream, chunkID, service)
}

// drain pulls the chunk's raw ledgers and feeds each (as a view) to the service,
// then verifies the full [first,last] range was consumed. For the cold path this
// completeness check runs before Finalize, so a short stream never produces a
// finalized truncated artifact.
func drain(ctx context.Context, stream ledgerbackend.LedgerStream, chunkID chunk.ID, ing HotIngester) error {
	first, last := chunkID.FirstLedger(), chunkID.LastLedger()
	seq := first
	// Cancellation is the stream's job: RawLedgers yields an error once ctx is
	// canceled (part of the ChunkSource contract), so the loop needs no ctx
	// poll of its own.
	for raw, serr := range stream.RawLedgers(ctx, ledgerbackend.BoundedRange(first, last)) {
		if serr != nil {
			return fmt.Errorf("RawLedgers(%d): %w", seq, serr)
		}
		// Reject a stream that runs PAST the chunk before ingesting anything
		// out-of-chunk. Without this, an in-order overrun would only trip the
		// post-loop count check after the extra ledgers were durably ingested
		// (the ledger and txhash hot stores accept any sequence). All in-repo
		// sources bound themselves; this guards custom ChunkSources.
		if seq > last {
			return fmt.Errorf("ingest: stream for chunk %d yielded a ledger past %d (chunk overrun)",
				uint32(chunkID), last)
		}
		lcm := xdr.LedgerCloseMetaView(raw)
		// Validate the actual ledger sequence before ingesting. The final
		// count check below only catches a short/long stream; a source that
		// yields a duplicate or out-of-order ledger with the right total
		// count would otherwise pass silently (e.g. on the txhash and
		// ledger-hot paths, which key on the LCM's own seq).
		actual, aerr := lcm.LedgerSequence()
		if aerr != nil {
			return fmt.Errorf("ingest: stream for chunk %d: ledger sequence at expected %d: %w",
				uint32(chunkID), seq, aerr)
		}
		if actual != seq {
			return fmt.Errorf("ingest: stream for chunk %d yielded ledger %d, expected %d",
				uint32(chunkID), actual, seq)
		}
		// seq is now VALIDATED as lcm's sequence — pass it through so the
		// ingesters consume it instead of each re-deriving it from the view.
		if err := ing.Ingest(ctx, seq, lcm); err != nil {
			return err
		}
		seq++
	}
	if seq != last+1 {
		return fmt.Errorf("ingest: stream for chunk %d ended at %d, expected through %d", uint32(chunkID), seq-1, last)
	}
	return nil
}

// ColdDirs names the per-data-type output root for one chunk's cold artifacts.
// Each field is the directory UNDER WHICH the matching cold ingester composes
// its {bucketID:05d}/ subdirectory — i.e. the same `coldDir` the per-type
// constructor (NewLedgerColdIngester) takes. A field left "" for a data type
// enabled in cfg is a configuration error caught by RunColdChunk.
//
// ColdDirs lets a caller whose layout is not coldDir/<dataType> place each
// artifact at its own canonical path while reusing the cold pipeline verbatim.
type ColdDirs struct {
	Ledgers string
}

// buildColdIngestersIn is the ColdDirs counterpart of buildColdIngesters: same
// constructors and rollback-on-error semantics, but each ingester's root comes
// from an explicit dirs field rather than coldDir/<dataType>.
func buildColdIngestersIn(dirs ColdDirs, chunkID chunk.ID, sink MetricSink, cfg Config) ([]ColdIngester, error) {
	ctors := []struct {
		enabled  bool
		dataType string
		dir      string
		open     func(string, chunk.ID, MetricSink) (ColdIngester, error)
	}{
		{cfg.Ledgers, dataTypeLedgers, dirs.Ledgers, NewLedgerColdIngester},
	}
	var ings []ColdIngester
	for _, c := range ctors {
		if !c.enabled {
			continue
		}
		if c.dir == "" {
			return nil, closeColdAll(ings, fmt.Errorf("ingest: %s enabled but ColdDirs.%s is empty", c.dataType, c.dataType))
		}
		ing, err := c.open(c.dir, chunkID, sink)
		if err != nil {
			return nil, closeColdAll(ings, fmt.Errorf("open %s cold ingester: %w", c.dataType, err))
		}
		ings = append(ings, ing)
	}
	return ings, nil
}

// RunColdChunk ingests EXACTLY ONE chunk's cold artifacts from source into the
// per-data-type roots named by dirs, in a single streaming pass. It is the
// single-chunk, explicit-layout sibling of RunCold (same ingesters, ColdService,
// and drain loop with its completeness check before Finalize).
//
// The cold ingesters overwrite any prior attempt's files at their canonical
// paths, so RunColdChunk is the re-materialization primitive the streaming
// freeze protocol drives: a partial file from a crashed attempt is inert scratch
// the next call overwrites.
func RunColdChunk(
	ctx context.Context,
	logger *supportlog.Entry,
	source ChunkSource,
	dirs ColdDirs,
	chunkID chunk.ID,
	sink MetricSink,
	cfg Config,
) (err error) {
	if verr := cfg.validate(); verr != nil {
		return verr
	}
	sink = orNop(sink)
	start := time.Now()
	if cerr := ctx.Err(); cerr != nil {
		sink.ColdChunkTotal(time.Since(start))
		return cerr
	}
	stream, oerr := source.OpenStream(chunkID)
	if oerr != nil {
		sink.ColdChunkTotal(time.Since(start))
		return fmt.Errorf("open stream for chunk %d: %w", uint32(chunkID), oerr)
	}
	ings, berr := buildColdIngestersIn(dirs, chunkID, sink, cfg)
	if berr != nil {
		sink.ColdChunkTotal(time.Since(start))
		return berr
	}
	logger.Debugf("RunColdChunk: ingesting chunk %d [%d, %d]",
		uint32(chunkID), chunkID.FirstLedger(), chunkID.LastLedger())
	service := NewColdService(ings, sink)
	defer func() {
		if cerr := service.Close(); cerr != nil {
			err = errors.Join(err, fmt.Errorf("close: %w", cerr))
		}
	}()
	if derr := drain(ctx, stream, chunkID, service); derr != nil {
		return derr
	}
	return service.Finalize(ctx)
}

// RunCold ingests numChunks consecutive chunks starting at startChunk into the
// cold stores under coldDir, processing up to chunkWorkers chunks concurrently.
// Each chunk worker opens its own stream via source.OpenStream(chunkID), builds
// the enabled cold ingesters (which open their own writers), drives the ledgers
// through a ColdService, then Finalizes. A deferred Close drops partials on the
// failure path.
func RunCold(
	ctx context.Context,
	logger *supportlog.Entry,
	source ChunkSource,
	coldDir string,
	startChunk chunk.ID,
	numChunks, chunkWorkers int,
	sink MetricSink,
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
			if rerr := runOneChunkCold(gctx, source, coldDir, chunkID, sink, cfg); rerr != nil {
				return fmt.Errorf("chunk %d: %w", uint32(chunkID), rerr)
			}
			return nil
		})
	}
	return g.Wait()
}

// runOneChunkCold processes a single chunk: opens its own stream, builds the
// enabled cold ingesters into a ColdService, drives the per-ledger Ingest, then
// Finalizes (explicit, error-checked). Close is deferred and idempotent. On any
// failure the chunk attempt is simply abandoned — leftover files under coldDir
// are inert scratch (see the package doc's artifact model) and the retry's
// overwrite is the cleanup.
func runOneChunkCold(
	ctx context.Context,
	source ChunkSource,
	coldDir string,
	chunkID chunk.ID,
	sink MetricSink,
	cfg Config,
) (err error) {
	sink = orNop(sink)

	// Pre-service failures (ctx, OpenStream, and the constructor failure
	// below) emit the chunk's single ColdChunkTotal here: the ColdService
	// that normally owns that aggregate isn't built yet, but the invariant
	// is "exactly one ColdChunkTotal per chunk attempt, including failures."
	start := time.Now()
	if cerr := ctx.Err(); cerr != nil {
		sink.ColdChunkTotal(time.Since(start))
		return cerr
	}
	stream, oerr := source.OpenStream(chunkID)
	if oerr != nil {
		sink.ColdChunkTotal(time.Since(start))
		return oerr
	}

	ings, berr := buildColdIngesters(coldDir, chunkID, sink, cfg)
	if berr != nil {
		// A constructor failure is still a chunk attempt
		// (closeColdAll only emitted the per-ingester aborts).
		sink.ColdChunkTotal(time.Since(start))
		return berr
	}
	service := NewColdService(ings, sink)
	defer func() {
		if cerr := service.Close(); cerr != nil {
			err = errors.Join(err, fmt.Errorf("close: %w", cerr))
		}
	}()

	if derr := drain(ctx, stream, chunkID, service); derr != nil {
		return derr
	}
	// drain verified the full range was consumed, so Finalize never commits a
	// truncated artifact.
	return service.Finalize(ctx)
}
