package ingest

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

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
// Each field is the directory under which the matching cold ingester composes
// its {bucketID:05d}/ subdirectory — the same `coldDir` the per-type constructor
// takes. A field left "" for a data type enabled in cfg is a configuration error
// caught by RunColdChunk.
//
// ColdDirs lets a caller with its own on-disk layout (e.g. the streaming daemon,
// whose raw txhash runs live under txhash/raw, not txhash) place each artifact at
// its own canonical path — passing an explicit per-type root instead of deriving
// coldDir/<dataType> — while reusing the very same cold ingesters, ColdService,
// and drain loop.
type ColdDirs struct {
	Ledgers string
	Txhash  string
	Events  string
}

// buildColdIngestersIn opens one ColdIngester per data type enabled in cfg, each
// under its OWN root from dirs (rather than coldDir/<dataType>). The constructor
// table below is the single definition site of the canonical
// ledgers→txhash→events order; on any constructor error it closes the ingesters
// built so far and returns (rollback).
func buildColdIngestersIn(dirs ColdDirs, chunkID chunk.ID, sink MetricSink, cfg Config) ([]ColdIngester, error) {
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
// per-data-type roots named by dirs, in a single streaming pass over the
// chunk's ledgers. It builds the enabled cold ingester constructors, drives them
// through a ColdService over the shared drain loop (sequence/overrun validation,
// full-range completeness check before Finalize), and takes explicit per-type
// output roots so a caller whose layout is not coldDir/<dataType> can reuse the
// cold pipeline verbatim.
//
// The cold ingesters overwrite any prior attempt's files at their canonical
// paths (the package doc's artifact model), so RunColdChunk is the
// re-materialization primitive the streaming freeze protocol drives: a partial
// from a crashed attempt is inert scratch the next call overwrites.
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
	logger.Debugf("RunColdChunk: ingesting chunk %d [%d, %d]", uint32(chunkID), chunkID.FirstLedger(), chunkID.LastLedger())
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
