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

// coldAborter lets the rollback path mark an ingester's metric aborted before
// Close emits it. Optional — a non-implementer just gets its normal emission.
type coldAborter interface {
	abortMetric(err error)
}

// closeColdAll closes every ingester built so far (joining errors), first marking
// each aborted so the deferred Close emit is not a phantom success. Used when a
// LATER constructor fails mid-build.
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

// drain feeds each of the chunk's raw ledgers (as a view) to the service, then
// verifies the full [first,last] range was consumed — for cold this runs before
// Finalize, so a short stream never finalizes a truncated artifact. Cancellation
// is the iterator's job (RawLedgers errors on canceled ctx), so no ctx poll here.
func drain(ctx context.Context, ledgers iter.Seq2[[]byte, error], chunkID chunk.ID, ing HotIngester) error {
	first, last := chunkID.FirstLedger(), chunkID.LastLedger()
	seq := first
	for raw, serr := range ledgers {
		if serr != nil {
			return fmt.Errorf("RawLedgers(%d): %w", seq, serr)
		}
		// Reject an overrun before ingesting it: without this, the post-loop
		// count check would only trip AFTER the extra ledgers were durably
		// written (the ledger/txhash hot stores accept any seq). Guards custom
		// iterators; in-repo sources self-bound.
		if seq > last {
			return fmt.Errorf("ingest: stream for chunk %d yielded a ledger past %d (chunk overrun)",
				uint32(chunkID), last)
		}
		lcm := xdr.LedgerCloseMetaView(raw)
		// Validate the actual seq before ingesting: the count check only catches
		// short/long streams, so a duplicate or out-of-order ledger with the
		// right total count would otherwise pass silently.
		actual, aerr := lcm.LedgerSequence()
		if aerr != nil {
			return fmt.Errorf("ingest: stream for chunk %d: ledger sequence at expected %d: %w",
				uint32(chunkID), seq, aerr)
		}
		if actual != seq {
			return fmt.Errorf("ingest: stream for chunk %d yielded ledger %d, expected %d",
				uint32(chunkID), actual, seq)
		}
		// seq is now VALIDATED as lcm's sequence — pass it through so ingesters
		// needn't each re-derive it.
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

// WriteColdChunk materializes ONE chunk's cold artifacts into dirs in a single
// pass from the raw ledger iterator. SOURCE-BLIND: the caller resolves the ledger
// source (local .pack or bulk backend) and hands its iterator here, so the
// materializer never learns where the bytes came from. Ingesters overwrite any
// crashed partial (the freeze protocol's re-materialization); on failure the
// attempt is abandoned, leftover files inert (package doc's artifact model).
//
// Source resolution (pack-stat, coverage wait) runs in the caller BEFORE this, so
// the only pre-service failures left to meter here are a canceled ctx and a
// constructor failure.
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

	// Pre-service failures emit the chunk's single ColdChunkTotal here (the owning
	// ColdService isn't built yet) — invariant: one ColdChunkTotal per attempt.
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
