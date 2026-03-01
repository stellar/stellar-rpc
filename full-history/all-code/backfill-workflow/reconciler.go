package backfill

import (
	"fmt"
	"os"

	"github.com/stellar/stellar-rpc/full-history/all-code/helpers"
)

// =============================================================================
// Startup Reconciler
// =============================================================================
//
// The reconciler runs once at process startup before the orchestrator begins
// processing ranges. It examines all existing range states in the meta store
// and performs cleanup or validation as needed.
//
// Reconciliation scenarios (from doc 07):
//
//   COMPLETE → delete leftover raw/ directory if it exists (frees disk space).
//
//   RECSPLIT_BUILDING → verify raw/ directory exists (needed for rebuilding).
//     If raw/ is missing, the range cannot be recovered — log FATAL and abort.
//     Delete partial .idx files for CFs that don't have a done flag.
//
//   INGESTING → no cleanup needed. BuildSkipSet handles resume decisions.
//
//   Orphan detection → if any range has data in the meta store but is not
//     in the configured range [start, end], log a warning. Multiple orphans
//     that can't be explained are suspicious — abort to avoid data loss.

// ReconcilerConfig holds dependencies for the reconciler.
type ReconcilerConfig struct {
	Meta       BackfillMetaStore
	Logger     Logger
	TxHashBase string

	// ConfiguredRanges is the set of range IDs that the current config covers.
	// Any range in the meta store not in this set is an orphan.
	ConfiguredRanges map[uint32]bool
}

// reconciler performs startup reconciliation.
type reconciler struct {
	meta       BackfillMetaStore
	log        Logger
	txhashBase string
	configured map[uint32]bool
}

// NewReconciler creates a reconciler.
func NewReconciler(cfg ReconcilerConfig) *reconciler {
	return &reconciler{
		meta:       cfg.Meta,
		log:        cfg.Logger.WithScope("RECONCILER"),
		txhashBase: cfg.TxHashBase,
		configured: cfg.ConfiguredRanges,
	}
}

// Run performs startup reconciliation for all known ranges.
// Returns an error if an unrecoverable issue is found (e.g., missing raw/ for
// a range in RECSPLIT_BUILDING state).
func (r *reconciler) Run() error {
	r.log.Info("Starting reconciliation...")

	allRanges, err := r.meta.AllRangeIDs()
	if err != nil {
		return fmt.Errorf("list range IDs: %w", err)
	}

	if len(allRanges) == 0 {
		r.log.Info("No existing ranges found — fresh start")
		return nil
	}

	r.log.Info("Found %d existing range(s) in meta store", len(allRanges))

	orphanCount := 0

	for _, rangeID := range allRanges {
		state, err := r.meta.GetRangeState(rangeID)
		if err != nil {
			return fmt.Errorf("get state for range %d: %w", rangeID, err)
		}

		// Check if this range is in the current config
		isConfigured := r.configured[rangeID]
		if !isConfigured && state != "" {
			r.log.Error("WARNING: Range %d has state %q but is not in current config — orphan",
				rangeID, state)
			orphanCount++
			continue
		}

		switch state {
		case RangeStateComplete:
			// Scenario: COMPLETE → delete leftover raw/ if it exists.
			// After RecSplit finishes, raw/ should already be deleted. But if
			// the process crashed after setting COMPLETE but before raw/ deletion,
			// clean it up now.
			r.reconcileComplete(rangeID)

		case RangeStateRecSplitBuilding:
			// Scenario B3 (doc 07): Process crashed during RecSplit build.
			// Verify raw/ exists (needed for rebuilding) and clean partial .idx files.
			if err := r.reconcileRecSplit(rangeID); err != nil {
				return err
			}

		case RangeStateIngesting:
			// No cleanup needed — BuildSkipSet handles resume in range_worker.
			r.log.Info("Range %d: state=%s — will resume ingestion", rangeID, state)

		case "":
			// Range has keys but no state — might have partial chunk data.
			// Log and treat as fresh.
			r.log.Info("Range %d: no state — treating as fresh", rangeID)

		default:
			return fmt.Errorf("range %d has unknown state %q", rangeID, state)
		}
	}

	// Abort if there are multiple orphans — something may be wrong.
	if orphanCount > 1 {
		return fmt.Errorf("found %d orphan ranges — aborting. This may indicate "+
			"a config change or data migration issue. Review ranges manually", orphanCount)
	}

	r.log.Info("Reconciliation complete")
	return nil
}

// reconcileComplete cleans up raw/ directory for a completed range.
func (r *reconciler) reconcileComplete(rangeID uint32) {
	rawDir := RawTxHashDir(r.txhashBase, rangeID)
	if helpers.IsDir(rawDir) {
		r.log.Info("Range %d: COMPLETE — deleting leftover raw/ directory", rangeID)
		if err := os.RemoveAll(rawDir); err != nil {
			r.log.Error("Range %d: failed to delete raw/ directory: %v", rangeID, err)
		}
	} else {
		r.log.Info("Range %d: COMPLETE — no cleanup needed", rangeID)
	}
}

// reconcileRecSplit validates state for a range in RECSPLIT_BUILDING phase.
// Returns a fatal error if raw/ is missing (unrecoverable without re-ingestion).
func (r *reconciler) reconcileRecSplit(rangeID uint32) error {
	rawDir := RawTxHashDir(r.txhashBase, rangeID)

	// FATAL if raw/ directory is missing — we need those .bin files to rebuild indexes.
	if !helpers.IsDir(rawDir) {
		return fmt.Errorf("range %d is in RECSPLIT_BUILDING state but raw/ directory %s "+
			"does not exist. Cannot rebuild RecSplit indexes without raw .bin files. "+
			"The range must be re-ingested from scratch", rangeID, rawDir)
	}

	r.log.Info("Range %d: RECSPLIT_BUILDING — raw/ exists, will resume RecSplit", rangeID)

	// Scenario B3: Delete partial .idx files for CFs that don't have a done flag.
	// A partial .idx file from a crashed RecSplit build may be corrupt/incomplete.
	// By deleting it, the RecSplit builder will rebuild that CF from scratch.
	for cf := 0; cf < CFCount; cf++ {
		done, err := r.meta.IsRecSplitCFDone(rangeID, cf)
		if err != nil {
			return fmt.Errorf("check recsplit cf %d for range %d: %w", cf, rangeID, err)
		}

		idxPath := RecSplitIndexPath(r.txhashBase, rangeID, CFNames[cf])
		if !done && helpers.FileExists(idxPath) {
			// Partial .idx without done flag — delete it so it gets rebuilt.
			r.log.Info("Range %d: deleting partial index cf-%s.idx (no done flag)",
				rangeID, CFNames[cf])
			os.Remove(idxPath)
		}
	}

	// Check for scenario B4: all CFs done but state not updated
	allDone := true
	for cf := 0; cf < CFCount; cf++ {
		done, _ := r.meta.IsRecSplitCFDone(rangeID, cf)
		if !done {
			allDone = false
			break
		}
	}
	if allDone {
		r.log.Info("Range %d: all 16 CFs done — updating state to COMPLETE", rangeID)
		if err := r.meta.SetRangeState(rangeID, RangeStateComplete); err != nil {
			return fmt.Errorf("set range %d complete: %w", rangeID, err)
		}
		r.reconcileComplete(rangeID)
	}

	return nil
}
