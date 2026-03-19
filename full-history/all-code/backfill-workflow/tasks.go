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
// created by the orchestrator during DAG construction and executed by the
// DAG scheduler once their dependencies are satisfied.
//
// Current task types:
//
//	process_instance(range_id, instance_id)  — Chunk cadence (50 chunks per instance)
//	build_txhash_index(range_id)             — Range cadence (10M ledgers)
//
// Cadence indicates the ledger-count scope. Chunk-cadence tasks operate on
// 10K-ledger chunks grouped into 50-chunk instances. Range-cadence tasks
// operate on the full 10M-ledger range after all chunks are ingested.
//
// Future types (when events are added):
//
//	build_events_index(range_id)  — Range cadence, parallel to build_txhash_index
//	complete_range(range_id)      — Range cadence, depends on all build_* tasks
//
// The dependency graph for a single range (current):
//
//	process_instance(R, 0)  ─┐
//	process_instance(R, 1)  ─┤
//	...                      ├──► build_txhash_index(R)
//	process_instance(R, 19) ─┘
//
// Future (with events):
//
//	process_instance(R, 0..19) ──► build_txhash_index(R) ──┐
//	                           └─► build_events_index(R) ──┴──► complete_range(R)

// Task type constants.
const (
	TaskTypeProcessInstance  = "process_instance"
	TaskTypeBuildTxHashIndex = "build_txhash_index"
)

// ProcessInstanceTaskID returns the task ID for a process_instance task.
func ProcessInstanceTaskID(rangeID uint32, instanceID int) TaskID {
	return TaskID(fmt.Sprintf("process_instance(%04d,%02d)", rangeID, instanceID))
}

// BuildTxHashIndexTaskID returns the task ID for a build_txhash_index task.
func BuildTxHashIndexTaskID(rangeID uint32) TaskID {
	return TaskID(fmt.Sprintf("build_txhash_index(%04d)", rangeID))
}

// =============================================================================
// processInstanceTask — Chunk Cadence
// =============================================================================
//
// Wraps a BSBInstance: processes a contiguous slice of chunks within a range.
// Each instance owns ChunksPerIndex/NumInstances chunks (default 50) with a
// shared GCS connection and skip-set awareness.
//
// If ALL chunks in this instance's slice are already done (via skip-set), the
// BSBInstance exits immediately with zero GCS overhead — no LedgerSource is
// created and no bytes are downloaded.

type processInstanceTask struct {
	id         TaskID
	rangeID    uint32
	instanceID int

	firstChunkID uint32
	lastChunkID  uint32
	skipSet      map[uint32]bool
	ledgersBase  string
	txHashBase   string
	meta         BackfillMetaStore
	memory        memory.Monitor
	factory       LedgerSourceFactory
	log           logging.Logger
	progress      *RangeProgress
	geo           geometry.Geometry
}

func (t *processInstanceTask) ID() TaskID { return t.id }

func (t *processInstanceTask) Execute(ctx context.Context) error {
	instance := NewBSBInstance(BSBInstanceConfig{
		InstanceID:   t.instanceID,
		RangeID:      t.rangeID,
		FirstChunkID: t.firstChunkID,
		LastChunkID:  t.lastChunkID,
		SkipSet:      t.skipSet,
		LedgersBase:  t.ledgersBase,
		TxHashBase:   t.txHashBase,
		Meta:         t.meta,
		Memory:       t.memory,
		Factory:      t.factory,
		Logger:       t.log,
		Progress:     t.progress,
		Geo:          t.geo,
	})

	_, err := instance.Run(ctx)
	return err
}

// =============================================================================
// buildTxHashIndexTask — Range Cadence
// =============================================================================
//
// Runs the 4-phase RecSplit pipeline for a range. The DAG guarantees all
// process_instance tasks for this range have completed before Execute is called.
//
// On entry: transitions range state to RECSPLIT_BUILDING.
// RecSplitFlow.Run handles the full lifecycle:
//   - Count → Add → Build → Verify (optional)
//   - Set range state to COMPLETE
//   - Delete raw/ to free disk space
//
// For crash recovery (state already RECSPLIT_BUILDING): the state set is
// idempotent. RecSplitFlow's all-or-nothing cleanup handles partial indexes.

type buildTxHashIndexTask struct {
	id      TaskID
	rangeID uint32

	txHashBase     string
	meta           BackfillMetaStore
	memory         memory.Monitor
	log            logging.Logger
	progress       *RangeProgress
	geo            geometry.Geometry
	verifyRecSplit bool
}

func (t *buildTxHashIndexTask) ID() TaskID { return t.id }

func (t *buildTxHashIndexTask) Execute(ctx context.Context) error {
	// All chunks ingested — proceed to RecSplit building.
	t.log.Info("All chunks ingested — starting RecSplit build")
	if t.progress != nil {
		t.progress.SetPhase(PhaseRecSplit)
	}

	firstChunk := t.geo.RangeFirstChunk(t.rangeID)
	lastChunk := t.geo.RangeLastChunk(t.rangeID)

	flow := NewRecSplitFlow(RecSplitFlowConfig{
		TxHashBase:   t.txHashBase,
		RangeID:      t.rangeID,
		FirstChunkID: firstChunk,
		LastChunkID:  lastChunk,
		Meta:         t.meta,
		Memory:       t.memory,
		Logger:       t.log,
		Progress:     t.progress,
		Verify:       t.verifyRecSplit,
	})

	if _, err := flow.Run(ctx); err != nil {
		return fmt.Errorf("recsplit flow: %w", err)
	}

	if t.progress != nil {
		t.progress.SetPhase(PhaseComplete)
	}

	t.log.Separator()
	t.log.Info("RANGE %d COMPLETE", t.rangeID)
	t.log.Separator()

	return nil
}
