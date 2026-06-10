package ingest

import (
	"context"
	"errors"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// errOrFirst returns prev if it is non-nil, else cur. Used to retain the FIRST
// error a cold ingester saw across its Ingest/Finalize lifetime for the sink's
// per-chunk err report.
func errOrFirst(prev, cur error) error {
	if prev != nil {
		return prev
	}
	return cur
}

// HotService fans one ledger out to a set of HotIngesters concurrently, waiting
// for all to finish before returning (so the borrowed view is safe to release),
// and emits the aggregate per-ledger wall-clock via the sink.
type HotService struct {
	ingesters []HotIngester
	sink      MetricSink
}

// NewHotService builds a HotService over the enabled hot ingesters. A nil sink
// defaults to NopSink.
func NewHotService(ingesters []HotIngester, sink MetricSink) *HotService {
	return &HotService{ingesters: ingesters, sink: orNop(sink)}
}

// Ingest runs every hot ingester on lcm concurrently and waits for all of them.
// The first ingester error is returned; the production HotIngester.Ingest
// implementations do not check ctx.Err(), so the siblings run to completion
// regardless (g.Wait still returns the first error). The single-ingester config
// skips the errgroup entirely. HotLedgerTotal is emitted with the fan-out
// wall-clock regardless of success.
func (s *HotService) Ingest(ctx context.Context, lcm xdr.LedgerCloseMetaView) error {
	start := time.Now()
	switch len(s.ingesters) {
	case 0:
		// No hot ingesters enabled for this tier: nothing to do.
		s.sink.HotLedgerTotal(time.Since(start))
		return nil
	case 1:
		// Single ingester: call directly, skipping the errgroup overhead.
		err := s.ingesters[0].Ingest(ctx, lcm)
		s.sink.HotLedgerTotal(time.Since(start))
		return err
	default:
		// Two or more: concurrent fan-out, waiting for all.
		g, gctx := errgroup.WithContext(ctx)
		for _, ing := range s.ingesters {
			g.Go(func() error { return ing.Ingest(gctx, lcm) })
		}
		err := g.Wait()
		s.sink.HotLedgerTotal(time.Since(start))
		return err
	}
}

// ColdService drives a set of ColdIngesters for one chunk: sequential per-ledger
// Ingest, then Finalize on each. It times from the first Ingest (or, if none ran,
// from the Finalize/Close call) and emits the aggregate ColdChunkTotal exactly
// once for the chunk — in Finalize on the success path, otherwise in Close on the
// failure path (an Ingest error or short stream short-circuits before Finalize).
// The totalEmitted flag prevents a double-emit: Finalize sets it so the caller's
// deferred Close is a no-op for the aggregate. (A ctx/OpenStream/constructor
// failure happens before the service is built — runOneChunkCold emits that
// chunk's single ColdChunkTotal directly.)
type ColdService struct {
	ingesters    []ColdIngester
	sink         MetricSink
	start        time.Time
	totalEmitted bool
}

// NewColdService builds a ColdService over the enabled cold ingesters. A nil
// sink defaults to NopSink. The per-chunk aggregate timer starts here; the only
// case where no Ingest follows is an already-errored short/empty stream, where
// the timing sample is meaningless anyway.
func NewColdService(ingesters []ColdIngester, sink MetricSink) *ColdService {
	return &ColdService{ingesters: ingesters, sink: orNop(sink), start: time.Now()}
}

// emitChunkTotal reports the aggregate ColdChunkTotal exactly once for the chunk.
func (s *ColdService) emitChunkTotal() {
	if s.totalEmitted {
		return
	}
	s.totalEmitted = true
	s.sink.ColdChunkTotal(time.Since(s.start))
}

// Ingest runs every cold ingester on lcm sequentially (each owns mutable
// per-chunk state, so no concurrency within the service). The first error
// aborts the ledger.
func (s *ColdService) Ingest(ctx context.Context, lcm xdr.LedgerCloseMetaView) error {
	for _, ing := range s.ingesters {
		if err := ing.Ingest(ctx, lcm); err != nil {
			return err
		}
	}
	return nil
}

// Finalize commits each cold ingester's chunk artifact (explicit, error-checked,
// never deferred). The first Finalize error STOPS the loop: finalizing (and so
// publishing) the remaining ingesters' artifacts after a sibling failed would
// leave a failed-and-not-retried chunk with a complete txhash .bin / events
// pack+index but no ledger pack — exactly the complete-but-orphaned state a
// later index build could consume. The unfinalized ingesters are not stranded:
// the caller's deferred Close releases their handles and drops their partials
// without publishing, so a failed chunk leaves no newly committed artifacts
// past the one that failed. The per-chunk ColdChunkTotal is emitted here on
// the success path.
func (s *ColdService) Finalize(ctx context.Context) error {
	var ferr error
	for _, ing := range s.ingesters {
		if err := ing.Finalize(ctx); err != nil {
			ferr = fmt.Errorf("finalize: %w", err)
			break
		}
	}
	s.emitChunkTotal()
	return ferr
}

// Close closes every cold ingester, joining each Close error, and emits the
// aggregate ColdChunkTotal if Finalize never reached it (the failure path). Each
// ingester's own Close in turn emits that ingester's per-chunk ColdIngest if its
// Finalize never ran, so a failed chunk still produces one per-ingester signal
// and one aggregate. Idempotent: on the failure path a writer's Close drops its
// partial file; after a successful Finalize all emissions are no-ops.
func (s *ColdService) Close() error {
	var err error
	for _, ing := range s.ingesters {
		if cerr := ing.Close(); cerr != nil {
			err = errors.Join(err, fmt.Errorf("close: %w", cerr))
		}
	}
	s.emitChunkTotal()
	return err
}
