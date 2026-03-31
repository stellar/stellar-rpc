package backfill

import (
	"context"
	"fmt"
	"os"

	"github.com/stellar/stellar-rpc/full-history/pkg/geometry"
	"github.com/stellar/stellar-rpc/full-history/pkg/logging"
	"github.com/stellar/stellar-rpc/full-history/pkg/memory"
)

// =============================================================================
// Task Types — DAG Nodes for the Backfill Pipeline
// =============================================================================
//
// Three task types per the design doc:
//
//   process_chunk(chunk_id)        — Ingests one 10K-ledger chunk.
//   build_txhash_index(index_id)  — Builds RecSplit index from .bin files.
//   cleanup_txhash(index_id)      — Deletes raw .bin files + meta keys after index build.
//
// Dependency graph per txhash index:
//
//   process_chunk(C+0) ─┐
//   process_chunk(C+1) ─┤
//   ...                  ├──► build_txhash_index(idx) ──► cleanup_txhash(idx)
//   process_chunk(C+N) ─┘
//
// Each task's Execute() checks its own completion state and returns early if done.

const (
	TaskTypeProcessChunk     = "process_chunk"
	TaskTypeBuildTxHashIndex = "build_txhash_index"
	TaskTypeCleanupTxHash    = "cleanup_txhash"
)

func ProcessChunkTaskID(chunkID uint32) TaskID {
	return TaskID(fmt.Sprintf("process_chunk(%08d)", chunkID))
}

func BuildTxHashIndexTaskID(indexID uint32) TaskID {
	return TaskID(fmt.Sprintf("build_txhash_index(%08d)", indexID))
}

func CleanupTxHashTaskID(indexID uint32) TaskID {
	return TaskID(fmt.Sprintf("cleanup_txhash(%08d)", indexID))
}

// =============================================================================
// processChunkTask
// =============================================================================

type processChunkTask struct {
	id            TaskID
	chunkID       uint32
	ledgersPath   string
	txHashRawPath string
	eventsPath    string
	meta          BackfillMetaStore
	memory        memory.Monitor
	factory       LedgerSourceFactory
	log           logging.Logger
	progress      *IndexProgress
	geo           geometry.Geometry
}

func (t *processChunkTask) ID() TaskID { return t.id }

// Execute checks each output flag independently and only produces missing outputs.
func (t *processChunkTask) Execute(ctx context.Context) error {
	// Per-task no-op: if all outputs present, return immediately.
	lfsDone, err := t.meta.IsChunkLFSDone(t.chunkID)
	if err != nil {
		return fmt.Errorf("check lfs flag for chunk %d: %w", t.chunkID, err)
	}
	txDone, err := t.meta.IsChunkTxHashDone(t.chunkID)
	if err != nil {
		return fmt.Errorf("check txhash flag for chunk %d: %w", t.chunkID, err)
	}
	eventsDone, err := t.meta.IsChunkEventsDone(t.chunkID)
	if err != nil {
		return fmt.Errorf("check events flag for chunk %d: %w", t.chunkID, err)
	}

	if lfsDone && txDone && eventsDone {
		return nil // no-op
	}

	// Determine source: if LFS is done, read from local packfile.
	firstLedger := t.geo.ChunkFirstLedger(t.chunkID)
	lastLedger := t.geo.ChunkLastLedger(t.chunkID)

	var source LedgerSource
	if lfsDone {
		lfsSource, err := NewLFSPackfileSource(t.ledgersPath, t.chunkID, firstLedger, lastLedger)
		if err != nil {
			return fmt.Errorf("open LFS source for chunk %d: %w", t.chunkID, err)
		}
		defer lfsSource.Close()
		source = lfsSource
		t.log.Info("Chunk %08d: LFS-first path (LFS done, producing missing outputs)", t.chunkID)
	} else {
		gcsSource, err := t.factory.Create(ctx, firstLedger, lastLedger)
		if err != nil {
			return fmt.Errorf("create ledger source for chunk %d: %w", t.chunkID, err)
		}
		defer gcsSource.Close()

		if err := gcsSource.PrepareRange(ctx, firstLedger, lastLedger); err != nil {
			return fmt.Errorf("prepare range for chunk %d: %w", t.chunkID, err)
		}
		source = gcsSource
	}

	cw := NewChunkWriter(ChunkWriterConfig{
		ChunkID:       t.chunkID,
		LedgersPath:   t.ledgersPath,
		TxHashRawPath: t.txHashRawPath,
		EventsPath:    t.eventsPath,
		Meta:          t.meta,
		Memory:        t.memory,
		Logger:        t.log,
		Progress:      t.progress,
		Geo:           t.geo,
	})

	_, err = cw.WriteChunk(ctx, source)
	return err
}

// =============================================================================
// buildTxHashIndexTask
// =============================================================================

type buildTxHashIndexTask struct {
	id             TaskID
	indexID        uint32
	txHashRawPath  string
	txHashIdxPath  string
	meta           BackfillMetaStore
	memory         memory.Monitor
	log            logging.Logger
	progress       *IndexProgress
	geo            geometry.Geometry
	verifyRecSplit bool
	useStreamHash  bool
}

func (t *buildTxHashIndexTask) ID() TaskID { return t.id }

// Execute builds the txhash index. No-op if index:{N}:txhash is already set.
// If absent, deletes partial .idx files first (all-or-nothing recovery).
func (t *buildTxHashIndexTask) Execute(ctx context.Context) error {
	done, err := t.meta.IsIndexTxHashDone(t.indexID)
	if err != nil {
		return fmt.Errorf("check index txhash flag for index %d: %w", t.indexID, err)
	}
	if done {
		return nil // no-op
	}

	// All-or-nothing: delete partial .idx files + tmp dir before building.
	idxDir := RecSplitIndexDir(t.txHashIdxPath, t.indexID)
	os.RemoveAll(idxDir)

	t.log.Info("All chunks ingested — starting txhash index build")
	if t.progress != nil {
		t.progress.SetPhase(PhaseRecSplit)
	}

	firstChunk := t.geo.IndexFirstChunk(t.indexID)
	lastChunk := t.geo.IndexLastChunk(t.indexID)

	if t.useStreamHash {
		return t.executeStreamHash(ctx, firstChunk, lastChunk)
	}
	return t.executeRecSplit(ctx, firstChunk, lastChunk)
}

func (t *buildTxHashIndexTask) executeRecSplit(ctx context.Context, firstChunk, lastChunk uint32) error {
	flow := NewRecSplitFlow(RecSplitFlowConfig{
		TxHashRawPath:  t.txHashRawPath,
		TxHashIdxPath:  t.txHashIdxPath,
		IndexID:        t.indexID,
		FirstChunkID:   firstChunk,
		LastChunkID:    lastChunk,
		Meta:           t.meta,
		Memory:         t.memory,
		Logger:         t.log,
		Progress:       t.progress,
		Verify:         t.verifyRecSplit,
	})

	if _, err := flow.Run(ctx); err != nil {
		return fmt.Errorf("recsplit flow: %w", err)
	}

	if t.progress != nil {
		t.progress.SetPhase(PhaseComplete)
	}

	t.log.Separator()
	t.log.Info("INDEX %d COMPLETE", t.indexID)
	t.log.Separator()
	return nil
}

func (t *buildTxHashIndexTask) executeStreamHash(ctx context.Context, firstChunk, lastChunk uint32) error {
	flow := NewStreamHashFlow(StreamHashFlowConfig{
		TxHashRawPath:  t.txHashRawPath,
		TxHashIdxPath:  t.txHashIdxPath,
		IndexID:        t.indexID,
		FirstChunkID:   firstChunk,
		LastChunkID:    lastChunk,
		Meta:           t.meta,
		Memory:         t.memory,
		Logger:         t.log,
		Progress:       t.progress,
		Verify:         t.verifyRecSplit,
	})

	if _, err := flow.Run(ctx); err != nil {
		return fmt.Errorf("streamhash flow: %w", err)
	}

	if t.progress != nil {
		t.progress.SetPhase(PhaseComplete)
	}

	t.log.Separator()
	t.log.Info("INDEX %d COMPLETE (StreamHash)", t.indexID)
	t.log.Separator()
	return nil
}

// =============================================================================
// cleanupTxHashTask
// =============================================================================

type cleanupTxHashTask struct {
	id            TaskID
	indexID       uint32
	txHashRawPath string
	meta          BackfillMetaStore
	log           logging.Logger
	geo           geometry.Geometry
}

func (t *cleanupTxHashTask) ID() TaskID { return t.id }

// Execute deletes raw .bin files + chunk:{C}:txhash meta keys for this index.
// Per-chunk idempotency: each chunk checks its own key before deleting.
func (t *cleanupTxHashTask) Execute(ctx context.Context) error {
	if t.log != nil {
		t.log.Info("Cleaning up raw txhash files for index %d", t.indexID)
	}

	firstChunk := t.geo.IndexFirstChunk(t.indexID)
	lastChunk := t.geo.IndexLastChunk(t.indexID)

	for chunkID := firstChunk; chunkID <= lastChunk; chunkID++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Per-chunk idempotency: skip if already cleaned up.
		hasTxHash, err := t.meta.IsChunkTxHashDone(chunkID)
		if err != nil {
			return fmt.Errorf("check txhash flag for chunk %d: %w", chunkID, err)
		}
		if !hasTxHash {
			continue // already cleaned up
		}

		// Delete .bin file.
		binPath := RawTxHashPath(t.txHashRawPath, chunkID)
		os.Remove(binPath)

		// Delete meta key.
		if err := t.meta.DeleteChunkTxHashKey(chunkID); err != nil {
			return fmt.Errorf("delete txhash key for chunk %d: %w", chunkID, err)
		}
	}

	if t.log != nil {
		t.log.Info("Cleanup complete for index %d", t.indexID)
	}
	return nil
}
