package backfill

import (
	"context"
	"fmt"

	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/geometry"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/logging"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/memory"
)

// =============================================================================
// Task Types — DAG Nodes for the Backfill Pipeline
// =============================================================================
//
// Each task type corresponds to a step in the backfill pipeline. Tasks are
// created by the pipeline during DAG construction and executed by the DAG
// scheduler once their dependencies are satisfied.
//
// There are exactly two task types:
//
//   process_chunk(chunk_id)        — Processes one 10K-ledger chunk.
//                                    Fetches ledgers, writes LFS packfile + raw
//                                    txhash .bin, sets meta store flags after fsync.
//                                    No dependencies on other chunks.
//
//   build_txhash_index(index_id)  — Builds the RecSplit txhash index for one index
//                                    (default: 1000 chunks = 10M ledgers).
//                                    Depends on ALL process_chunk tasks for its index.
//                                    The DAG guarantees all chunk files exist before
//                                    this task runs.
//
// Workers (default 40) is the maximum number of concurrent DAG task slots.
// Any mix of process_chunk and build_txhash_index tasks can occupy these slots.
// There is no separate "instance" or "worker pool" abstraction — each task is
// independently scheduled by the DAG based on its dependencies and slot availability.
//
// The dependency graph for a single index:
//
//   process_chunk(C+0) ─┐
//   process_chunk(C+1) ─┤
//   ...                  ├──► build_txhash_index(indexID)
//   process_chunk(C+N) ─┘
//
// At startup, ALL process_chunk tasks across ALL indexes have in-degree 0
// (no dependencies) and are eligible to run immediately. The DAG dispatches
// them until the worker pool is full. As chunks complete, their slots free up
// for more chunks or for build_txhash_index tasks whose dependencies are met.
//
// Future types (when events support is added):
//
//   build_events_index(index_id)  — Parallel to build_txhash_index
//   complete_index(index_id)      — Depends on all build_* tasks for the index

// Task type constants — used for logging and diagnostics.
const (
	TaskTypeProcessChunk     = "process_chunk"
	TaskTypeBuildTxHashIndex = "build_txhash_index"
)

// ProcessChunkTaskID returns the task ID for a process_chunk task.
// Format: "process_chunk(00006789)" — %08d zero-padded chunk ID for sort order.
func ProcessChunkTaskID(chunkID uint32) TaskID {
	return TaskID(fmt.Sprintf("process_chunk(%08d)", chunkID))
}

// BuildTxHashIndexTaskID returns the task ID for a build_txhash_index task.
// Format: "build_txhash_index(00000003)" — %08d zero-padded index ID.
func BuildTxHashIndexTaskID(indexID uint32) TaskID {
	return TaskID(fmt.Sprintf("build_txhash_index(%08d)", indexID))
}

// =============================================================================
// processChunkTask — One DAG Task Per Chunk
// =============================================================================
//
// Processes a single 10K-ledger chunk end-to-end:
//
//   1. Determine the data source (GCS vs LFS-first — see below)
//   2. Create a ChunkWriter for this chunk
//   3. For each ledger in the chunk:
//      a. Fetch the LedgerCloseMeta from the source
//      b. Compress + append to LFS packfile (.data + .index)
//      c. Extract txhashes + append to raw .bin file
//   4. 3-step fsync sequence: LFS files → txhash .bin → meta store flags
//   5. Task completes; DAG frees the worker slot
//
// === LFS-First Path ===
//
// On crash recovery, a chunk may be in a state where the LFS packfile was
// fsynced (chunk:{C}:lfs = "1") but the raw txhash .bin was NOT (chunk:{C}:txhash
// absent). In this case, re-downloading the ledger data from GCS would be
// wasteful — the LFS packfile already contains all the ledger data on disk.
//
// The LFS-first path detects this condition and reads ledgers from the local
// LFS packfile instead of creating a GCS connection. This is:
//   - Faster: local disk read vs network fetch
//   - Cheaper: no GCS bandwidth cost
//   - Simpler: no BSB/GCS connection lifecycle
//
// The check is: if meta.IsChunkLFSDone(chunkID) returns true, the LFS file
// is durable on disk and we can read from it. The ChunkWriter then only needs
// to write the raw txhash .bin file (it always deletes + recreates both files,
// but the LFS write is just re-creating what was already there — the important
// thing is that it doesn't need GCS).
//
// === Normal (GCS) Path ===
//
// For fresh chunks (neither flag set), the task creates a new LedgerSource
// via the Factory (typically a BSB/GCS connection). The Factory.Create call
// establishes a GCS DataStore and BufferedStorageBackend scoped to exactly
// this chunk's ledger range [firstLedger, lastLedger]. PrepareRange starts
// the prefetch. The source is closed when the task completes.

type processChunkTask struct {
	// id is the DAG task ID, e.g. "process_chunk(006789)".
	id TaskID

	// indexID identifies which index this chunk belongs to.
	// Used to construct the correct txhash .bin file path.
	indexID uint32

	// chunkID is the globally unique chunk identifier.
	// Determines ledger range: [ChunkFirstLedger(chunkID), ChunkLastLedger(chunkID)].
	chunkID uint32

	// immutableBase is the root directory for all immutable data.
	// All paths derive from this: index-{indexID}/ledgers/, index-{indexID}/txhash/, etc.
	immutableBase string

	// meta is the meta store for reading LFS flags and writing completion flags.
	meta BackfillMetaStore

	// memory is the memory monitor, checked after each chunk.
	memory memory.Monitor

	// factory creates LedgerSource connections (GCS or CaptiveCore).
	// Only used on the normal (non-LFS-first) path.
	factory LedgerSourceFactory

	// log is the scoped logger (typically INDEX:XXXX scope).
	log logging.Logger

	// progress is the per-index progress tracker for recording chunk stats.
	progress *IndexProgress

	// geo holds chunk/index boundary math.
	geo geometry.Geometry
}

func (t *processChunkTask) ID() TaskID { return t.id }

// Execute processes a single chunk. It determines the data source (LFS-first
// or GCS), creates a ChunkWriter, and writes the LFS + txhash files.
//
// This is the core ingestion unit. With workers=40, up to 40 of these
// run concurrently across all indexes.
func (t *processChunkTask) Execute(ctx context.Context) error {
	// === Step 1: Determine data source ===
	//
	// Check if the LFS packfile already exists from a prior partial run.
	// If chunk:{C}:lfs = "1", the LFS file is durable and we can read from it
	// instead of re-fetching from GCS. This is the "LFS-first" optimization.
	lfsDone, err := t.meta.IsChunkLFSDone(t.chunkID)
	if err != nil {
		return fmt.Errorf("check LFS flag for chunk %d: %w", t.chunkID, err)
	}

	firstLedger := t.geo.ChunkFirstLedger(t.chunkID)
	lastLedger := t.geo.ChunkLastLedger(t.chunkID)

	var source LedgerSource
	if lfsDone {
		// === LFS-First Path ===
		//
		// The LFS packfile is durable on disk (flag was set after fsync).
		// Create an LFSPackfileSource that reads ledgers sequentially from
		// the local .data + .index files. No GCS connection needed.
		//
		// The ChunkWriter will still delete-and-recreate BOTH files (LFS + txhash)
		// as part of its crash-safe protocol, but the data comes from local disk
		// rather than the network.
		lfsSource, err := NewLFSPackfileSource(t.immutableBase, t.indexID, t.chunkID, firstLedger, lastLedger)
		if err != nil {
			return fmt.Errorf("open LFS source for chunk %d: %w", t.chunkID, err)
		}
		defer lfsSource.Close()
		source = lfsSource
		t.log.Info("Chunk %08d: LFS-first path (LFS done, txhash missing)", t.chunkID)
	} else {
		// === Normal (GCS) Path ===
		//
		// Fresh chunk: create a new LedgerSource via the Factory.
		// For BSB backend, this creates a GCS DataStore + BufferedStorageBackend
		// with prefetch workers scoped to this chunk's ledger range.
		//
		// Each chunk task gets its own GCS connection. With workers=40,
		// up to 40 concurrent GCS connections exist at peak. Each connection
		// has its own internal prefetch workers (configured via bsb.num_workers).
		gcsSource, err := t.factory.Create(ctx, firstLedger, lastLedger)
		if err != nil {
			return fmt.Errorf("create ledger source for chunk %d: %w", t.chunkID, err)
		}
		defer gcsSource.Close()

		// PrepareRange tells the backend to start prefetching.
		// For BSB: begins downloading ledger files from GCS into the buffer.
		// For CaptiveCore: triggers catchup to the specified range.
		if err := gcsSource.PrepareRange(ctx, firstLedger, lastLedger); err != nil {
			return fmt.Errorf("prepare range for chunk %d: %w", t.chunkID, err)
		}
		source = gcsSource
	}

	// === Step 2: Write the chunk ===
	//
	// ChunkWriter handles the full write protocol:
	//   1. Delete any partial files from a previous crash
	//   2. Create LFS writer + TxHash writer
	//   3. For each ledger: compress→LFS, extract txhashes→.bin
	//   4. 3-step fsync: LFS files → txhash .bin → meta store WriteBatch
	//
	// After WriteChunk returns, both flags are set and the chunk is complete.
	cw := NewChunkWriter(ChunkWriterConfig{
		ImmutableBase: t.immutableBase,
		IndexID:       t.indexID,
		ChunkID:       t.chunkID,
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
// buildTxHashIndexTask — One DAG Task Per Index
// =============================================================================
//
// Runs the 4-phase RecSplit pipeline for an index after ALL its chunks are done.
//
// The DAG dependency guarantee means: when Execute is called, every chunk in
// this index has both flags set (chunk:{C}:lfs = "1" AND chunk:{C}:txhash = "1").
// All raw txhash .bin files exist and are durable on disk. There is no need to
// check "are all chunks done?" — the DAG has already enforced this.
//
// RecSplitFlow phases:
//   1. COUNT  — 100 goroutines scan all .bin files, count keys per CF
//   2. ADD    — 100 goroutines re-scan .bin files, add keys to 16 RecSplit builders
//   3. BUILD  — 16 parallel builds (one per CF), produces .idx files
//   4. VERIFY — (optional) 100 goroutines look up every key to confirm correctness
//
// On completion: writes index:{N}:txhash = "1" to the meta store, then
// deletes the raw/ directory to free disk space. The index is COMPLETE.
//
// For crash recovery: if the process crashed during RecSplit, on restart the
// resume triage sees all chunk flags set but no index key → ResumeActionRecSplit.
// The task re-runs from scratch (RecSplit is all-or-nothing; partial .idx files
// are deleted by the reconciler at startup).

type buildTxHashIndexTask struct {
	// id is the DAG task ID, e.g. "build_txhash_index(0003)".
	id TaskID

	// indexID identifies which index to build.
	indexID uint32

	// immutableBase is the root directory for all immutable data.
	immutableBase string

	// meta is the meta store for writing the index completion key.
	meta BackfillMetaStore

	// memory is the memory monitor, checked during build phases.
	memory memory.Monitor

	// log is the scoped logger (typically INDEX:XXXX scope).
	log logging.Logger

	// progress is the per-index progress tracker.
	progress *IndexProgress

	// geo holds chunk/index boundary math.
	geo geometry.Geometry

	// verifyRecSplit controls whether the VERIFY phase runs after BUILD.
	verifyRecSplit bool

	// useStreamHash selects StreamHash instead of RecSplit for txhash index building.
	// When true, builds a single txhash.idx file using streamhash MPHF instead of
	// 16 cf-{nibble}.idx files using RecSplit. CLI-only (--use-streamhash).
	useStreamHash bool
}

func (t *buildTxHashIndexTask) ID() TaskID { return t.id }

// Execute runs the txhash index build pipeline for this index.
// Dispatches to StreamHashFlow or RecSplitFlow based on the useStreamHash flag.
// Called only after all process_chunk tasks for this index have completed.
func (t *buildTxHashIndexTask) Execute(ctx context.Context) error {
	if t.useStreamHash {
		return t.executeStreamHash(ctx)
	}
	return t.executeRecSplit(ctx)
}

// executeRecSplit runs the 4-phase RecSplit pipeline (existing behavior).
func (t *buildTxHashIndexTask) executeRecSplit(ctx context.Context) error {
	t.log.Info("All chunks ingested — starting RecSplit build")
	if t.progress != nil {
		t.progress.SetPhase(PhaseRecSplit)
	}

	firstChunk := t.geo.RangeFirstChunk(t.indexID)
	lastChunk := t.geo.RangeLastChunk(t.indexID)

	flow := NewRecSplitFlow(RecSplitFlowConfig{
		ImmutableBase: t.immutableBase,
		IndexID:       t.indexID,
		FirstChunkID:  firstChunk,
		LastChunkID:   lastChunk,
		Meta:          t.meta,
		Memory:        t.memory,
		Logger:        t.log,
		Progress:      t.progress,
		Verify:        t.verifyRecSplit,
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

// executeStreamHash runs the 3-phase StreamHash pipeline (alternative builder).
// Produces a single txhash.idx file instead of 16 per-CF RecSplit .idx files.
func (t *buildTxHashIndexTask) executeStreamHash(ctx context.Context) error {
	t.log.Info("All chunks ingested — starting StreamHash build (--use-streamhash)")
	if t.progress != nil {
		t.progress.SetPhase(PhaseRecSplit) // Reuse phase constant; progress display says "RECSPLIT" but that's fine for benchmarking
	}

	firstChunk := t.geo.RangeFirstChunk(t.indexID)
	lastChunk := t.geo.RangeLastChunk(t.indexID)

	flow := NewStreamHashFlow(StreamHashFlowConfig{
		ImmutableBase: t.immutableBase,
		IndexID:       t.indexID,
		FirstChunkID:  firstChunk,
		LastChunkID:   lastChunk,
		Meta:          t.meta,
		Memory:        t.memory,
		Logger:        t.log,
		Progress:      t.progress,
		Verify:        t.verifyRecSplit,
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
