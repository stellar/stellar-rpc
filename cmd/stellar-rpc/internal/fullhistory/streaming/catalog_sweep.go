package streaming

import (
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/metastore"
)

// Key-driven sweeps — the ONLY two deletion bodies in the system, one per key
// family. Both follow the same load-bearing order:
//
//	demote-if-still-"frozen" -> unlink file(s) -> fsyncDir(parent) -> delete key
//
// The key outlives the durable unlink, giving the exit invariant
// "key absent => file gone": a crash anywhere leaves the key in place and the
// sweep re-runs. Deleting the key first would orphan a file with no key — the
// one class this design cannot find. The fireBefore* hooks mark the two crash
// windows the order protects (no-op in production).

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
			if ref.State == StateFrozen {
				w.Put(ref.Key(), string(StatePruning))
			}
		}
		return nil
	}); err != nil {
		return err
	}
	c.hooks.fireBeforeUnlink() // crash window: every frozen ref now reads "pruning"

	// Unlink every file (idempotent), collecting parents for the barrier.
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
	c.hooks.fireBeforeKeyDelete() // crash window: files gone, keys not yet

	// Delete the keys — only now that the unlinks are durable.
	return c.store.Batch(func(w *metastore.BatchWriter) error {
		for _, ref := range refs {
			w.Delete(ref.Key())
		}
		return nil
	})
}

// SweepIndexKey deletes one index coverage's file and key, in the same order as
// SweepChunkArtifacts. A "frozen" coverage is demoted first; "freezing" debris
// (a crashed attempt, never salvaged) and "pruning" coverages take the same
// path from here.
func (c *Catalog) SweepIndexKey(cov IndexCoverage) error {
	if cov.State == StateFrozen { // never unlink under a "frozen" key
		if err := c.store.Put(cov.Key, string(StatePruning)); err != nil {
			return err
		}
	}
	c.hooks.fireBeforeUnlink() // crash window: key now reads "pruning"
	path := c.layout.IndexFilePath(cov)
	if err := deleteFileIfExists(path); err != nil {
		return err
	}
	dir := c.layout.IndexWindowDir(cov.Window)
	if err := fsyncDir(dir); err != nil { // unlink durable BEFORE key delete
		return err
	}
	c.hooks.fireBeforeKeyDelete() // crash window: file gone, key not yet
	if err := c.store.Delete(cov.Key); err != nil {
		return err
	}
	rmdirIfEmpty(dir) // best-effort; an empty dir is not an artifact
	return nil
}
