package catalog

import (
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/metastore"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/streaming/geometry"
)

// Key-driven sweeps — the ONLY two deletion bodies in the system, one per key
// family. Both follow the same load-bearing order:
//
//	demote-if-still-"frozen" -> unlink file(s) -> fsyncDir(parent) -> delete key
//
// The key outlives the durable unlink, giving the exit invariant
// "key absent => file gone": a crash anywhere leaves the key in place and the
// sweep re-runs. Deleting the key first would orphan a file with no key — the
// one class this design cannot find.

// SweepChunkArtifacts deletes the files and keys for a batch of per-chunk refs.
// Still-"frozen" refs are demoted to "pruning" first so no unlink happens under
// a frozen key; "freezing"/"pruning" refs unlink directly. The batch shares one
// demote, one fsync pass, and one key-delete across all refs.
func (c *Catalog) SweepChunkArtifacts(refs []ArtifactRef) error {
	if len(refs) == 0 {
		return nil
	}

	// Demote first — never unlink under a "frozen" key.
	if err := c.store.Batch(func(w *metastore.BatchWriter) error {
		for _, ref := range refs {
			if ref.State == geometry.StateFrozen {
				w.Put(ref.Key(), string(geometry.StatePruning))
			}
		}
		return nil
	}); err != nil {
		return err
	}

	// Unlink every file (idempotent), collecting parents for the barrier.
	var paths []string
	for _, ref := range refs {
		for _, p := range c.layout.ArtifactPaths(ref.Chunk, ref.Kind) {
			if err := geometry.DeleteFileIfExists(p); err != nil {
				return err
			}
			paths = append(paths, p)
		}
	}
	if err := geometry.FsyncParentDirs(paths); err != nil { // unlinks durable BEFORE keys
		return err
	}

	// Delete the keys — only now that the unlinks are durable.
	return c.store.Batch(func(w *metastore.BatchWriter) error {
		for _, ref := range refs {
			w.Delete(ref.Key())
		}
		return nil
	})
}

// SweepTxHashIndexKey deletes one index coverage's file and key, in the same order as
// SweepChunkArtifacts. A "frozen" coverage is demoted first; "freezing" debris
// (a crashed attempt, never salvaged) and "pruning" coverages take the same
// path from here.
//
// The demote decision is made on the CURRENT durable value of the key, NOT on
// cov.State, which the caller may have snapshotted before a concurrent
// CommitTxHashIndex promoted that same key to "frozen". Trusting a stale
// "freezing" snapshot would skip the frozen->pruning write and unlink the .idx
// under a key still durably "frozen"; a crash before the key delete would then
// leave a frozen catalog key pointing at a missing file. An absent key means a
// prior sweep already finished — nothing to do.
func (c *Catalog) SweepTxHashIndexKey(cov geometry.TxHashIndexCoverage) error {
	cur, ok, err := c.get(cov.Key)
	if err != nil {
		return err
	}
	if !ok {
		return nil // key already gone — a prior sweep completed
	}
	if geometry.State(cur) == geometry.StateFrozen { // never unlink under a "frozen" key
		if err := c.store.Put(cov.Key, string(geometry.StatePruning)); err != nil {
			return err
		}
	}
	path := c.layout.TxHashIndexFilePath(cov)
	if err := geometry.DeleteFileIfExists(path); err != nil {
		return err
	}
	dir := c.layout.TxHashIndexDir(cov.Index)
	if err := geometry.FsyncDir(dir); err != nil { // unlink durable BEFORE key delete
		return err
	}
	if err := c.store.Delete(cov.Key); err != nil {
		return err
	}
	geometry.RmdirIfEmpty(dir) // best-effort; an empty dir is not an artifact
	return nil
}
