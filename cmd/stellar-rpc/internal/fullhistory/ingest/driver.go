package ingest

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"
	"golang.org/x/sync/errgroup"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/eventstore"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash"
)

// HotStores holds the long-lived, caller-owned hot stores injected into RunHot.
// The caller (the daemon) opens and closes these; RunHot only borrows them to
// build the per-type hot ingesters. A field left nil for an enabled data type is
// a configuration error caught by RunHot.
type HotStores struct {
	Ledgers *ledger.HotStore
	Txhash  *txhash.HotStore
	// Events must already be bound to the chunk being ingested.
	Events *eventstore.HotStore
}

// buildHotIngesters constructs one HotIngester per data type enabled in cfg, in
// canonical ledgers→txhash→events order, from the injected stores. It errors if
// an enabled type's store is nil.
func buildHotIngesters(stores HotStores, sink MetricSink, cfg Config) ([]HotIngester, error) {
	var ings []HotIngester
	if cfg.Ledgers {
		if stores.Ledgers == nil {
			return nil, fmt.Errorf("ingest: Ledgers enabled but HotStores.Ledgers is nil")
		}
		ings = append(ings, NewLedgerHotIngester(stores.Ledgers, sink))
	}
	if cfg.Txhash {
		if stores.Txhash == nil {
			return nil, fmt.Errorf("ingest: Txhash enabled but HotStores.Txhash is nil")
		}
		ings = append(ings, NewTxhashHotIngester(stores.Txhash, sink))
	}
	if cfg.Events {
		if stores.Events == nil {
			return nil, fmt.Errorf("ingest: Events enabled but HotStores.Events is nil")
		}
		ings = append(ings, NewEventsHotIngester(stores.Events, sink))
	}
	return ings, nil
}

// buildColdIngesters opens one ColdIngester per data type enabled in cfg, in
// canonical order, each opening its own per-chunk writer under coldDir/<type>.
// On any constructor error it closes the ingesters built so far and returns.
func buildColdIngesters(coldDir string, chunkID chunk.ID, sink MetricSink, cfg Config) ([]ColdIngester, error) {
	var ings []ColdIngester
	add := func(ing ColdIngester, err error, what string) error {
		if err != nil {
			return fmt.Errorf("open %s cold ingester: %w", what, err)
		}
		ings = append(ings, ing)
		return nil
	}
	if cfg.Ledgers {
		ing, err := NewLedgerColdIngester(filepath.Join(coldDir, dataTypeLedgers), chunkID, sink)
		if aerr := add(ing, err, dataTypeLedgers); aerr != nil {
			return nil, closeColdAll(ings, aerr)
		}
	}
	if cfg.Txhash {
		ing, err := NewTxhashColdIngester(filepath.Join(coldDir, dataTypeTxhash), chunkID, sink)
		if aerr := add(ing, err, dataTypeTxhash); aerr != nil {
			return nil, closeColdAll(ings, aerr)
		}
	}
	if cfg.Events {
		ing, err := NewEventsColdIngester(filepath.Join(coldDir, dataTypeEvents), chunkID, sink)
		if aerr := add(ing, err, dataTypeEvents); aerr != nil {
			return nil, closeColdAll(ings, aerr)
		}
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

// RunHot opens one stream for chunkID from source and feeds each ledger (as a
// view) to a HotService over the enabled hot ingesters, built from the INJECTED,
// caller-owned stores in hotStores. Ingest errors abort fast; HotService.Ingest
// waits for all ingesters before the loop pulls again so the borrowed view is
// never read past its lifetime. The hot stores are NOT closed here — the caller
// owns their lifecycle.
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
	// The events hot store is bound to a chunk at open time; if the caller
	// injected one bound to a different chunk than we're ingesting, every
	// per-ledger write would later fail with an out-of-range offset. Catch the
	// mismatch up front with a clear message. Only meaningful when Events is
	// enabled and a store was actually injected.
	if cfg.Events && hotStores.Events != nil {
		if got := hotStores.Events.ChunkID(); got != chunkID {
			return fmt.Errorf("ingest: RunHot chunk %d but injected Events store is bound to chunk %d",
				uint32(chunkID), uint32(got))
		}
	}
	ings, berr := buildHotIngesters(hotStores, sink, cfg)
	if berr != nil {
		return berr
	}
	stream, oerr := source.OpenStream(chunkID)
	if oerr != nil {
		return fmt.Errorf("open stream for chunk %d: %w", uint32(chunkID), oerr)
	}
	logger.Debugf("RunHot: ingesting chunk %d [%d, %d]", uint32(chunkID), chunkID.FirstLedger(), chunkID.LastLedger())
	service := NewHotService(ings, sink)
	return drain(ctx, stream, chunkID, service)
}

// ledgerIngester is the one method drain needs from either tier's service
// (*HotService or *ColdService): feed one borrowed ledger view.
type ledgerIngester interface {
	Ingest(ctx context.Context, lcm xdr.LedgerCloseMetaView) error
}

// drain pulls the chunk's raw ledgers and feeds each (as a view) to the service,
// then verifies the full [first,last] range was consumed. For the cold path this
// completeness check runs before Finalize, so a short stream never produces a
// finalized truncated artifact.
func drain(ctx context.Context, stream ledgerbackend.LedgerStream, chunkID chunk.ID, ing ledgerIngester) error {
	first, last := chunkID.FirstLedger(), chunkID.LastLedger()
	seq := first
	for raw, serr := range stream.RawLedgers(ctx, ledgerbackend.BoundedRange(first, last)) {
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		if serr != nil {
			return fmt.Errorf("RawLedgers(%d): %w", seq, serr)
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
		if err := ing.Ingest(ctx, lcm); err != nil {
			return err
		}
		seq++
	}
	if seq != last+1 {
		return fmt.Errorf("ingest: stream for chunk %d ended at %d, expected through %d", uint32(chunkID), seq-1, last)
	}
	return nil
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
// Finalizes (explicit, error-checked). Close is deferred and idempotent — on the
// failure path it removes any partial cold artifact for writers whose Finalize
// never ran.
func runOneChunkCold(
	ctx context.Context,
	source ChunkSource,
	coldDir string,
	chunkID chunk.ID,
	sink MetricSink,
	cfg Config,
) (err error) {
	sink = orNop(sink)

	// Validate the context and open the source stream BEFORE building the cold
	// ingesters. Their constructors are destructive — the ledger and events
	// cold writers open packfiles with Overwrite, truncating any existing
	// per-chunk artifact, and the txhash ingester drops its stale .bin. Building
	// them on an already-canceled context (a sibling chunk failed) or a missing
	// source would destroy good artifacts from a prior run and then bail without
	// ingesting. Opening the stream here is non-destructive (readers open lazily
	// in RawLedgers), so discarding it on a later error leaks nothing.
	//
	// These two pre-build failures emit the chunk's single ColdChunkTotal here:
	// the ColdService that normally owns that aggregate isn't built yet, but the
	// invariant is "exactly one ColdChunkTotal per chunk attempt, including
	// failures."
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
