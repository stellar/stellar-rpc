package ingest

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/eventstore"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash"
)

// HotStores holds the long-lived, caller-owned hot stores injected into RunHot.
// The caller (the daemon) opens and closes these; RunHot only borrows them to
// build the per-type hot ingesters. A field left nil for an enabled data type is
// a configuration error caught by RunHot. Every hot store is chunk-bound (each
// instance accumulates exactly one chunk before being frozen into cold
// artifacts), so each injected store must already be bound to the chunk being
// ingested — RunHot rejects a mismatch up front.
type HotStores struct {
	Ledgers *ledger.HotStore
	Txhash  *txhash.HotStore
	Events  *eventstore.HotStore
}

// buildHotIngesters constructs one HotIngester per data type enabled in cfg, in
// canonical ledgers→txhash→events order, from the injected stores. It errors if
// an enabled type's store is nil.
func buildHotIngesters(stores HotStores, sink MetricSink, cfg Config) ([]HotIngester, error) {
	var ings []HotIngester
	if cfg.Ledgers {
		if stores.Ledgers == nil {
			return nil, errors.New("ingest: Ledgers enabled but HotStores.Ledgers is nil")
		}
		ings = append(ings, NewLedgerHotIngester(stores.Ledgers, sink))
	}
	if cfg.Txhash {
		if stores.Txhash == nil {
			return nil, errors.New("ingest: Txhash enabled but HotStores.Txhash is nil")
		}
		ings = append(ings, NewTxhashHotIngester(stores.Txhash, sink))
	}
	if cfg.Events {
		if stores.Events == nil {
			return nil, errors.New("ingest: Events enabled but HotStores.Events is nil")
		}
		ings = append(ings, NewEventsHotIngester(stores.Events, sink))
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

// RunHot feeds each ledger of chunkID (as a view) from the injected stream to a
// HotService over the enabled hot ingesters, built from the INJECTED,
// caller-owned stores in hotStores. Ingest errors abort fast; HotService.Ingest
// waits for all ingesters before the loop pulls again so the borrowed view is
// never read past its lifetime. The hot stores are NOT closed here, and neither
// is the stream — the caller owns both lifecycles.
func RunHot(
	ctx context.Context,
	logger *supportlog.Entry,
	stream ledgerbackend.LedgerStream,
	chunkID chunk.ID,
	hotStores HotStores,
	sink MetricSink,
	cfg Config,
) error {
	if verr := cfg.validate(); verr != nil {
		return verr
	}
	// Every hot store is chunk-bound — each instance accumulates exactly one
	// chunk's data before being frozen into the chunk's cold artifacts — and
	// records its chunk at open time. An injected store bound to a different
	// chunk than we're ingesting would silently interleave two chunks' data
	// (ledgers, txhash) or fail every per-ledger write with an out-of-range
	// offset (events, whose LedgerOffsets are chunk-relative), so catch the
	// mismatch up front with a clear message. Nil stores are skipped here:
	// buildHotIngesters rejects a nil store for an enabled type with a more
	// specific error.
	checkBinding := func(name string, got chunk.ID) error {
		if got != chunkID {
			return fmt.Errorf("ingest: RunHot chunk %d but injected %s store is bound to chunk %d",
				uint32(chunkID), name, uint32(got))
		}
		return nil
	}
	if cfg.Ledgers && hotStores.Ledgers != nil {
		if err := checkBinding("Ledgers", hotStores.Ledgers.ChunkID()); err != nil {
			return err
		}
	}
	if cfg.Txhash && hotStores.Txhash != nil {
		if err := checkBinding("Txhash", hotStores.Txhash.ChunkID()); err != nil {
			return err
		}
	}
	if cfg.Events && hotStores.Events != nil {
		if err := checkBinding("Events", hotStores.Events.ChunkID()); err != nil {
			return err
		}
	}
	ings, berr := buildHotIngesters(hotStores, sink, cfg)
	if berr != nil {
		return berr
	}
	logger.Debugf("RunHot: ingesting chunk %d [%d, %d]", uint32(chunkID), chunkID.FirstLedger(), chunkID.LastLedger())
	service := NewHotService(ings, sink)
	raw := stream.RawLedgers(ctx, ledgerbackend.BoundedRange(chunkID.FirstLedger(), chunkID.LastLedger()))
	return drain(ctx, raw, chunkID, service)
}

// drain pulls the chunk's raw ledgers from the iterator and feeds each (as a view)
// to the service, then verifies the full [first,last] range was consumed. For the
// cold path this completeness check runs before Finalize, so a short stream never
// produces a finalized truncated artifact. The caller passes an iterator already
// bounded to the chunk's range; cancellation is the iterator's job (RawLedgers
// yields an error once ctx is canceled), so the loop needs no ctx poll of its own.
func drain(ctx context.Context, ledgers iter.Seq2[[]byte, error], chunkID chunk.ID, ing HotIngester) error {
	first, last := chunkID.FirstLedger(), chunkID.LastLedger()
	seq := first
	for raw, serr := range ledgers {
		if serr != nil {
			return fmt.Errorf("RawLedgers(%d): %w", seq, serr)
		}
		// Reject a stream that runs PAST the chunk before ingesting anything
		// out-of-chunk. Without this, an in-order overrun would only trip the
		// post-loop count check after the extra ledgers were durably ingested
		// (the ledger and txhash hot stores accept any sequence). All in-repo
		// sources bound themselves; this guards custom iterators.
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

// ColdDirs is the per-type output root for one chunk's cold artifacts. An empty
// field for an enabled type is a config error.
type ColdDirs struct {
	Ledgers string
	Txhash  string
	Events  string
}

// buildColdIngesters opens one ColdIngester per enabled type under its dirs field.
// Single definition site of the ctor table, order, and rollback.
func buildColdIngesters(dirs ColdDirs, chunkID chunk.ID, sink MetricSink, cfg Config) ([]ColdIngester, error) {
	ctors := []struct {
		enabled  bool
		dataType string
		dir      string
		open     func(string, chunk.ID, MetricSink) (ColdIngester, error)
	}{
		{cfg.Ledgers, dataTypeLedgers, dirs.Ledgers, NewLedgerColdIngester},
		{cfg.Txhash, dataTypeTxhash, dirs.Txhash, NewTxhashColdIngester},
		{cfg.Events, dataTypeEvents, dirs.Events, NewEventsColdIngester},
	}
	ings := make([]ColdIngester, 0, len(ctors))
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

// WriteColdChunk materializes ONE chunk's cold artifacts into the roots named by
// dirs, in a single pass, from the already-opened raw ledger iterator. It is
// SOURCE-BLIND: the caller (backfill) resolves the chunk's ledger source — the
// local frozen .pack or the bulk backend — and hands its RawLedgers iterator here,
// so the cold materializer never learns where the bytes came from and is faked in
// tests with a literal slice iterator. The ingesters overwrite any crashed
// partial, so this is the freeze protocol's re-materialization. On any failure the
// attempt is abandoned — leftover files are inert scratch (see the package doc's
// artifact model) and a retry's overwrite is the cleanup.
//
// Source resolution (pack-stat, coverage wait) runs in the caller BEFORE this, so
// a pack-missing or coverage-timeout failure is metered there rather than as a
// ColdChunkTotal attempt here. The only pre-service failures left to meter here
// are a canceled ctx and a cold-ingester constructor failure.
func WriteColdChunk(
	ctx context.Context,
	logger *supportlog.Entry,
	chunkID chunk.ID,
	raw iter.Seq2[[]byte, error],
	dirs ColdDirs,
	sink MetricSink,
	cfg Config,
) (err error) {
	if verr := cfg.validate(); verr != nil {
		return verr
	}
	sink = orNop(sink)

	// Pre-service failures (ctx and the constructor failure below) emit the
	// chunk's single ColdChunkTotal here: the ColdService that normally owns that
	// aggregate isn't built yet, but the invariant is "exactly one ColdChunkTotal
	// per chunk attempt, including failures."
	start := time.Now()
	if cerr := ctx.Err(); cerr != nil {
		sink.ColdChunkTotal(time.Since(start))
		return cerr
	}

	ings, berr := buildColdIngesters(dirs, chunkID, sink, cfg)
	if berr != nil {
		// A constructor failure is still a chunk attempt
		// (closeColdAll only emitted the per-ingester aborts).
		sink.ColdChunkTotal(time.Since(start))
		return berr
	}
	logger.Debugf("cold ingest chunk %d [%d, %d]", uint32(chunkID), chunkID.FirstLedger(), chunkID.LastLedger())
	service := NewColdService(ings, sink)
	defer func() {
		if cerr := service.Close(); cerr != nil {
			err = errors.Join(err, fmt.Errorf("close: %w", cerr))
		}
	}()

	if derr := drain(ctx, raw, chunkID, service); derr != nil {
		return derr
	}
	// drain verified the full range was consumed, so Finalize never commits a
	// truncated artifact.
	return service.Finalize(ctx)
}
