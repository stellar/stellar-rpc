package backfill

import (
	"fmt"
	"os"

	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/fsutil"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/logging"
)

// =============================================================================
// Startup Reconciler
// =============================================================================
//
// The reconciler runs once at process startup before the pipeline begins
// processing ranges. It examines all existing index states in the meta store
// and performs cleanup or validation as needed.
//
// Reconciliation scenarios (from doc 07):
//
//   COMPLETE (txhash key set) → delete leftover raw/ directory if it
//     exists (frees disk space).
//
//   Orphan detection → if any index has data in the meta store but is not
//     in the configured range [start, end], log a warning. Multiple orphans
//     that can't be explained are suspicious — abort to avoid data loss.

// ReconcilerConfig holds dependencies for the reconciler.
type ReconcilerConfig struct {
	Meta          BackfillMetaStore
	Logger        logging.Logger
	ImmutableBase string

	// ConfiguredRanges is the set of range IDs that the current config covers.
	// Any range in the meta store not in this set is an orphan.
	ConfiguredRanges map[uint32]bool
}

// reconciler performs startup reconciliation.
type reconciler struct {
	meta          BackfillMetaStore
	log           logging.Logger
	immutableBase string
	configured    map[uint32]bool
}

// NewReconciler creates a reconciler.
func NewReconciler(cfg ReconcilerConfig) *reconciler {
	return &reconciler{
		meta:          cfg.Meta,
		log:           cfg.Logger.WithScope("RECONCILER"),
		immutableBase: cfg.ImmutableBase,
		configured:    cfg.ConfiguredRanges,
	}
}

// Run performs startup reconciliation for all known indexes.
// Returns an error if an unrecoverable issue is found.
func (r *reconciler) Run() error {
	r.log.Info("Starting reconciliation...")

	allIndexes, err := r.meta.AllIndexIDs()
	if err != nil {
		return fmt.Errorf("list index IDs: %w", err)
	}

	if len(allIndexes) == 0 {
		r.log.Info("No existing indexes found — fresh start")
		return nil
	}

	r.log.Info("Found %d existing index(es) in meta store", len(allIndexes))

	orphanCount := 0

	for _, indexID := range allIndexes {
		// Check if this index is in the current config
		isConfigured := r.configured[indexID]
		if !isConfigured {
			r.log.Error("WARNING: Index %d has txhash key but is not in current config — orphan",
				indexID)
			orphanCount++
			continue
		}

		// Index is complete (txhash key present) — clean up leftover raw/
		r.reconcileComplete(indexID)
	}

	// Abort on any orphan — the operator changed the config range or pointed
	// at the wrong meta store. Do not silently ignore.
	if orphanCount > 0 {
		return fmt.Errorf("found %d orphan index(es) in meta store but not in configured range — aborting. "+
			"Either widen the configured range to include them or use a fresh meta store", orphanCount)
	}

	r.log.Info("Reconciliation complete")
	return nil
}

// reconcileComplete cleans up raw/ directory for a completed index.
func (r *reconciler) reconcileComplete(indexID uint32) {
	rawDir := RawTxHashDir(r.immutableBase, indexID)
	if fsutil.IsDir(rawDir) {
		r.log.Info("Index %d: COMPLETE — deleting leftover raw/ directory", indexID)
		if err := os.RemoveAll(rawDir); err != nil {
			r.log.Error("Index %d: failed to delete raw/ directory: %v", indexID, err)
		}
	} else {
		r.log.Info("Index %d: COMPLETE — no cleanup needed", indexID)
	}
}
