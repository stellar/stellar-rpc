package streaming

import (
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/metastore"
)

// Key-driven sweep — the ONLY deletion body in the system. Its ordering is
// load-bearing:
//
//	demote-if-still-"frozen"  (never unlink under a frozen key)
//	  -> unlink file(s)
//	  -> fsyncDir(parent)     (the unlink becomes durable BEFORE the key goes)
//	  -> delete key           (batched)
//
// This gives the exit-side invariant "key absent => file gone": because the
// key outlives the durable unlink, a crash anywhere leaves the key in place
// and the sweep re-runs. Deleting the key first would, on a crash, leave a
// file with no key — the one orphan class this design cannot find.

// SweepChunkArtifacts deletes the files for a batch of per-chunk artifact refs
// and removes their keys. Refs already past "frozen" (i.e. "freezing" or
// "pruning") are unlinked directly; a still-"frozen" ref is demoted to
// "pruning" first, in one atomic batch, so no unlink ever happens under a
// frozen key.
//
// The whole batch shares three barriers: one demote batch, one fsync pass over
// the affected parent dirs, one key-delete batch — so sweeping many refs at
// once pays a single round of each.
func (c *Catalog) SweepChunkArtifacts(refs []ArtifactRef) error {
	if len(refs) == 0 {
		return nil
	}

	// Demote first — never unlink under a "frozen" key. A crash after this
	// batch but before the unlinks leaves "pruning" keys the next sweep
	// finishes.
	if err := c.store.Batch(func(w *metastore.BatchWriter) error {
		for _, ref := range refs {
			if ref.State == StateFrozen {
				w.Put(ref.Key(), string(StatePruning))
			}
		}
		return nil
	}); err != nil {
		return err
	}

	// Between the demote and the unlink: every "frozen" ref must now read
	// "pruning". Dropping the demote above would leave it "frozen" here.
	c.hooks.fireBeforeUnlink()

	// Unlink every file (idempotent on already-gone paths), collecting parents
	// for the durability barrier.
	var paths []string
	for _, ref := range refs {
		for _, p := range c.layout.ArtifactPaths(ref.Chunk, ref.Kind) {
			if err := deleteFileIfExists(p); err != nil {
				return err
			}
			paths = append(paths, p)
		}
	}
	if err := fsyncParentDirs(paths); err != nil { // unlinks durable BEFORE keys
		return err
	}

	// Between the durable unlink and the key delete: the files are gone but the
	// keys still exist. Reordering the delete ahead of the unlink would leave a
	// file present here under no key — the one orphan class this order forbids.
	c.hooks.fireBeforeKeyDelete()

	// Delete the keys — only now that the unlinks are durable.
	return c.store.Batch(func(w *metastore.BatchWriter) error {
		for _, ref := range refs {
			w.Delete(ref.Key())
		}
		return nil
	})
}
