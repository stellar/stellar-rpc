package ingest

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"time"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// errColdBuildAborted is recorded against an already-built cold ingester when a
// LATER constructor fails and the build rolls back — without it, closing a
// fully-built ingester emits a clean ColdIngest, a phantom "success" for a chunk
// that ingested nothing.
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

// ValidatedLedger is one sequence-validated ledger from a raw stream: its
// verified sequence and the borrowed view (valid only until the next iteration
// step, per the LedgerStream contract).
type ValidatedLedger struct {
	Seq  uint32
	View xdr.LedgerCloseMetaView
}

// SeqValidatedCursor adapts a raw ledger stream into contiguous, sequence-checked
// ledgers starting at `from`: for each yielded frame it reads the view's own
// LedgerSequence() and rejects a gap, duplicate, or out-of-order ledger before
// handing it on. Both the cold drain and the hot ingestion loop consume it, so the
// sole writer of recent history never trusts an injected source blindly (the SDK
// backend also validates its own output — this is defense-in-depth, a zero-copy
// header read). A source error, a decode error, or a non-contiguous sequence is
// yielded as the error element and ends iteration; the view is borrowed.
func SeqValidatedCursor(
	ledgers iter.Seq2[[]byte, error], from uint32,
) iter.Seq2[ValidatedLedger, error] {
	return func(yield func(ValidatedLedger, error) bool) {
		seq := from
		for raw, serr := range ledgers {
			if serr != nil {
				yield(ValidatedLedger{Seq: seq}, fmt.Errorf("RawLedgers(%d): %w", seq, serr))
				return
			}
			lcm := xdr.LedgerCloseMetaView(raw)
			actual, aerr := lcm.LedgerSequence()
			if aerr != nil {
				yield(ValidatedLedger{Seq: seq}, fmt.Errorf("ledger sequence at expected %d: %w", seq, aerr))
				return
			}
			if actual != seq {
				yield(ValidatedLedger{Seq: seq}, fmt.Errorf("yielded ledger %d, expected %d", actual, seq))
				return
			}
			if !yield(ValidatedLedger{Seq: seq, View: lcm}, nil) {
				return
			}
			seq++
		}
	}
}

// drain feeds each of the chunk's raw ledgers (as a validated view) to the
// service, then verifies the full [first,last] range was consumed — for cold this
// runs before Finalize, so a short stream never finalizes a truncated artifact.
// Cancellation is the iterator's job (RawLedgers errors on canceled ctx), so no
// ctx poll here. The per-ledger sequence guard lives in the shared cursor.
func drain(ctx context.Context, ledgers iter.Seq2[[]byte, error], chunkID chunk.ID, ing LedgerIngester) error {
	first, last := chunkID.FirstLedger(), chunkID.LastLedger()
	seq := first
	for vl, verr := range SeqValidatedCursor(ledgers, first) {
		if verr != nil {
			return fmt.Errorf("ingest: stream for chunk %d: %w", uint32(chunkID), verr)
		}
		// Reject a stream that runs PAST the chunk before ingesting out-of-chunk.
		// The cursor already validated vl.Seq is contiguous; this bounds it above.
		// All in-repo sources bound themselves; this guards custom iterators.
		if vl.Seq > last {
			return fmt.Errorf("ingest: stream for chunk %d yielded a ledger past %d (chunk overrun)",
				uint32(chunkID), last)
		}
		if err := ing.Ingest(ctx, vl.Seq, vl.View); err != nil {
			return err
		}
		seq = vl.Seq + 1
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
