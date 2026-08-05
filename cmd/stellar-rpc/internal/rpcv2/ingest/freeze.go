package ingest

// freeze.go — FreezeColdChunk, the zero-decompression twin of WriteColdChunk
// for the ONE case where the chunk's source is its own complete hot DB. Where
// WriteColdChunk drains a raw-ledger stream (decompressing every hot frame to
// feed derivation writers), this entry point takes NO ledger stream at all:
// each artifact kind is built from the hot state that already holds it in
// final form — ledgers as verbatim zstd frames, txhash and events by merging
// each engine's manifest-listed sealed runs with its un-sealed CF tail.
// The signature is the invariant: with no stream parameter there is nothing
// to drain, so the freeze cannot decompress by accident.
//
// Artifact bytes are identical to WriteColdChunk's by construction and by
// test (the cross-path identity gates in each store package plus the
// composition gate in freeze_test.go). Failure semantics are WriteColdChunk's
// too: first error stops, earlier artifacts are inert scratch without the
// orchestrator's completion record, every builder overwrites on retry.

import (
	"context"
	"errors"
	"fmt"
	"time"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/hotchunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/ledger"
)

// FreezeColdChunk materializes ONE chunk's cold artifacts at the resolved
// paths named by dirs, directly from the chunk's complete (read-only) hot DB.
// Destination-dir creation is owned by each store's FreezeColdFromStore
// (mirroring the walk path, where the store-level constructors own it), so
// the first freeze into a fresh bucket cannot ENOENT. Metrics mirror the walk path's envelope: exactly one
// ColdChunkTotal per attempt (post-validate), one ColdIngest per enabled kind
// with the kind's real item count where the builder reports one, and a
// (kind, finalize) stage sample per success — all pre-existing series.
func FreezeColdChunk(
	ctx context.Context,
	logger *supportlog.Entry,
	chunkID chunk.ID,
	db *hotchunk.DB,
	dirs ColdDirs,
	sink MetricSink,
	cfg Config,
) error {
	if verr := cfg.validate(); verr != nil {
		return verr
	}
	sink = orNop(sink)

	// Same invariant as WriteColdChunk: one ColdChunkTotal per chunk attempt,
	// emitted for every post-validate return below (nil-DB included), none
	// for a config error above.
	start := time.Now()
	defer func() { sink.ColdChunkTotal(time.Since(start)) }()

	if db == nil {
		return errors.New("ingest: FreezeColdChunk with a nil hot DB")
	}

	if cerr := ctx.Err(); cerr != nil {
		return cerr
	}
	logger.Debugf("freeze chunk %d [%d, %d] from hot DB",
		uint32(chunkID), chunkID.FirstLedger(), chunkID.LastLedger())

	// Canonical ledgers→txhash→events order, sequential (a chunk's freeze is
	// I/O-shaped; the worker pool provides cross-chunk parallelism).
	if cfg.Ledgers {
		if dirs.LedgerPack == "" {
			return errors.New("ingest: ledgers enabled but its ColdDirs path is empty")
		}
		if err := freezeArm(sink, dataTypeLedgers, func() (int, error) {
			// Concurrency is meaningless in PreCompressed mode (no encoder);
			// writeback smoothing applies as in every batch build. The copy
			// runs at full speed and is CORRECT on any topology; the
			// RECOMMENDED deployment puts cold artifacts on their own device,
			// because on a shared disk an unpaced copy stream queues ahead of
			// the hot WAL fdatasync (measured: ~50ms median fsync waits →
			// co-located ingest commit p50 +28ms for the arm's duration).
			return db.FreezeLedgersCold(ctx, dirs.LedgerPack, ledger.ColdWriterOptions{
				BytesPerSync: coldBytesPerSync,
			})
		}); err != nil {
			return err
		}
	}
	if cfg.Txhash {
		if dirs.TxhashBin == "" {
			return errors.New("ingest: txhash enabled but its ColdDirs path is empty")
		}
		if err := freezeArm(sink, dataTypeTxhash, func() (int, error) {
			return db.FreezeTxhashCold(ctx, dirs.TxhashBin)
		}); err != nil {
			return err
		}
	}
	if cfg.Events {
		if dirs.EventsDir == "" {
			return errors.New("ingest: events enabled but its ColdDirs path is empty")
		}
		scratch := eventsScratchDir(dirs.EventsDir, chunkID)
		if err := freezeArm(sink, dataTypeEvents, func() (int, error) {
			// The events builder does not report an item count (its ColdIngest
			// stays items=0, as the freeze-by-merge events build always has).
			return 0, db.FreezeEventsCold(ctx, scratch, dirs.EventsDir, event.ColdWriterOptions{
				Concurrency:  coldEncoderConcurrency,
				BytesPerSync: coldBytesPerSync,
			})
		}); err != nil {
			return err
		}
	}
	return nil
}

// freezeArm runs one kind's freeze build inside the walk path's signal shape:
// one ColdIngest with the build's wall and item count, plus the
// (kind, finalize) stage sample on success. A single observation needs no
// accumulator — the sink is called directly.
func freezeArm(sink MetricSink, dataType string, build func() (int, error)) error {
	start := time.Now()
	n, err := build()
	if err != nil {
		err = fmt.Errorf("freeze %s from hot DB: %w", dataType, err)
	}
	d := time.Since(start)
	if err == nil {
		sink.IngestStage(dataType, stageFinalize, d, n)
	}
	sink.ColdIngest(dataType, d, n, err)
	return err
}
