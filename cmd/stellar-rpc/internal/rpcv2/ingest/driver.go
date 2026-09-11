package ingest

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"time"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
)

// drain feeds each of the chunk's raw ledgers (as a borrowed view) to the cold
// chunk on a local sequence counter, then verifies the full [first,last] range
// was consumed — this runs before finalize, so a short stream never finalizes a
// truncated artifact. The in-order contract is enforced at the SOURCE
// (packStream reads positionally by key; hotLedgerStream key-checks its own
// keyspace; the SDK backends validate their own output), so drain trusts the
// counter rather than re-parsing every view's sequence. Cancellation is the
// iterator's job (RawLedgers errors on a canceled ctx), so there is no ctx poll
// here.
func drain(ledgers iter.Seq2[[]byte, error], chunkID chunk.ID, cc *coldChunk) error {
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
		if err := cc.ingest(seq, xdr.LedgerCloseMetaView(raw)); err != nil {
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

// WriteColdChunk materializes ONE chunk's cold artifacts at the resolved paths
// named by dirs, in a single pass, from the already-opened raw ledger iterator. It is
// SOURCE-BLIND: the caller (backfill) resolves the chunk's ledger source — the
// local frozen .pack or the bulk backend — and hands its RawLedgers iterator here,
// so the cold materializer never learns where the bytes came from and is faked in
// tests with a literal slice iterator. The writers overwrite any crashed
// partial, so this is the freeze protocol's re-materialization. On any failure the
// attempt is abandoned — leftover files are inert scratch (see the package doc's
// artifact model) and a retry's overwrite is the cleanup.
//
// Source resolution (pack-stat, coverage wait) runs in the caller BEFORE this, so
// a pack-missing or coverage-timeout failure is metered there rather than as a
// ColdChunkTotal attempt here. The only pre-open failures left to meter here
// are a canceled ctx and a cold-writer open failure.
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

	// One deferred emit covers every return below exactly once — the invariant
	// "exactly one ColdChunkTotal per chunk attempt, including pre-open
	// failures" is a property of the control flow, not a runtime latch. It sits
	// after validate, so a config error still emits nothing. The window is
	// deliberately the whole attempt — open, the drain of the source stream,
	// every ingest, finalize, and the deferred close (see coldBuckets).
	start := time.Now()
	defer func() { sink.ColdChunkTotal(time.Since(start)) }()

	if cerr := ctx.Err(); cerr != nil {
		return cerr
	}
	cc, oerr := openColdChunk(dirs, chunkID, sink, cfg)
	if oerr != nil {
		return oerr
	}
	defer func() { err = errors.Join(err, cc.close()) }()
	logger.Debugf("cold ingest chunk %d [%d, %d]", uint32(chunkID), chunkID.FirstLedger(), chunkID.LastLedger())

	if derr := drain(raw, chunkID, cc); derr != nil {
		return derr
	}
	// drain verified the full range was consumed, so finalize never commits a
	// truncated artifact.
	return cc.finalize(ctx)
}
