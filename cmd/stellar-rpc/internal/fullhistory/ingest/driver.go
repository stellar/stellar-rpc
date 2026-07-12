package ingest

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"time"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
)

// closeColdAll closes every cold ingester built so far, joining each Close error
// into err. Used when a LATER constructor fails mid-build. The already-built
// ingesters never ingested or finalized, and Close emits no per-ingester
// ColdIngest sample, so a rolled-back build produces no phantom-success sample — no
// abort bookkeeping needed here.
func closeColdAll(ings []ColdIngester, err error) error {
	for _, ing := range ings {
		if cerr := ing.Close(); cerr != nil {
			err = errors.Join(err, fmt.Errorf("close: %w", cerr))
		}
	}
	return err
}

// drain feeds each of the chunk's raw ledgers (as a borrowed view) to the
// service on a local sequence counter, then verifies the full [first,last] range
// was consumed — for cold this runs before Finalize, so a short stream never
// finalizes a truncated artifact. The in-order contract is enforced at the SOURCE
// (packStream reads positionally by key; hotLedgerStream key-checks its own
// keyspace; the SDK backends validate their own output), so drain trusts the
// counter rather than re-parsing every view's sequence. Cancellation is the
// iterator's job (RawLedgers errors on a canceled ctx), so there is no ctx poll
// here.
func drain(ctx context.Context, ledgers iter.Seq2[[]byte, error], chunkID chunk.ID, svc *ColdService) error {
	first, last := chunkID.FirstLedger(), chunkID.LastLedger()
	seq := first
	for raw, serr := range ledgers {
		if serr != nil {
			return fmt.Errorf("ingest: stream for chunk %d: %w", uint32(chunkID), serr)
		}
		// Reject a stream that runs PAST the chunk before ingesting out-of-chunk.
		// All in-repo sources self-bound; this guards a custom iterator.
		if seq > last {
			return fmt.Errorf("ingest: stream for chunk %d yielded a ledger past %d (chunk overrun)",
				uint32(chunkID), last)
		}
		if err := svc.Ingest(ctx, seq, xdr.LedgerCloseMetaView(raw)); err != nil {
			return err
		}
		seq++
	}
	if seq != last+1 {
		return fmt.Errorf("ingest: stream for chunk %d ended at %d, expected through %d", uint32(chunkID), seq-1, last)
	}
	return nil
}

// ColdDirs holds ONE chunk's RESOLVED cold-artifact destinations, derived by the
// caller from geometry.Layout so the ingesters write exactly where the freeze
// barrier and the sweeps resolve — the path formula lives in Layout alone, never
// re-derived here. LedgerPack and TxhashBin are the chunk's full file paths;
// EventsDir is its events bucket dir. An empty field for an enabled type is a
// config error.
type ColdDirs struct {
	LedgerPack string
	TxhashBin  string
	EventsDir  string
}

// buildColdIngesters opens one ColdIngester per enabled type at its resolved path.
// Single definition site of the ctor table, order, and rollback.
func buildColdIngesters(dirs ColdDirs, chunkID chunk.ID, sink MetricSink, cfg Config) ([]ColdIngester, error) {
	ctors := []struct {
		enabled  bool
		dataType string
		path     string
		open     func(string, chunk.ID, MetricSink) (ColdIngester, error)
	}{
		{cfg.Ledgers, DataTypeLedgers, dirs.LedgerPack, NewLedgerColdIngester},
		{cfg.Txhash, DataTypeTxhash, dirs.TxhashBin, NewTxhashColdIngester},
		{cfg.Events, DataTypeEvents, dirs.EventsDir, NewEventsColdIngester},
	}
	ings := make([]ColdIngester, 0, len(ctors))
	for _, c := range ctors {
		if !c.enabled {
			continue
		}
		if c.path == "" {
			return nil, closeColdAll(ings, fmt.Errorf("ingest: %s enabled but its ColdDirs path is empty", c.dataType))
		}
		ing, err := c.open(c.path, chunkID, sink)
		if err != nil {
			return nil, closeColdAll(ings, fmt.Errorf("open %s cold ingester: %w", c.dataType, err))
		}
		ings = append(ings, ing)
	}
	return ings, nil
}

// WriteColdChunk materializes ONE chunk's cold artifacts at the resolved paths
// named by dirs, in a single pass, from the already-opened raw ledger iterator. It is
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
		// A constructor failure is still a chunk attempt: emit the aggregate
		// (closeColdAll rolled back the built ingesters with no per-ingester emit).
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
