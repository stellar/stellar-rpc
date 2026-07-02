package catalog

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
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
// path from here. cov.State is the caller's observation; the one-writer-per-key
// invariant (see catalog_protocol.go) means no concurrent writer can have changed
// the durable value under it.
func (c *Catalog) SweepTxHashIndexKey(cov geometry.TxHashIndexCoverage) error {
	if cov.State == geometry.StateFrozen { // never unlink under a "frozen" key
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

// DiscardHotChunk retires a chunk's hot DB once its cold artifacts are durable
// (or it fell past retention), following the same crash order as the two sweeps
// above: mark "transient" -> rmdir -> fsync(parent) -> delete key. The key
// outlives the durable rmdir, so a crash anywhere leaves the key "transient" for
// the next scan to finish — idempotent, and an absent key is a no-op. The caller
// MUST have closed the chunk's hot write handle (discard runs after the freeze).
func (c *Catalog) DiscardHotChunk(chunkID chunk.ID) error {
	state, err := c.HotState(chunkID)
	if err != nil {
		return fmt.Errorf("read hot key chunk %s: %w", chunkID, err)
	}
	if state == "" {
		return nil
	}
	if err := c.PutHotTransient(chunkID); err != nil {
		return fmt.Errorf("mark hot transient chunk %s: %w", chunkID, err)
	}
	dir := c.layout.HotChunkPath(chunkID)
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("rmdir hot dir %s: %w", dir, err)
	}
	// rmdir durable BEFORE the key delete: the key outlives the dir, so a crash
	// re-runs the discard rather than leaving a key-less dir.
	if err := geometry.FsyncDir(filepath.Dir(dir)); err != nil {
		return fmt.Errorf("fsync hot parent dir %s: %w", filepath.Dir(dir), err)
	}
	if err := c.DeleteHotKey(chunkID); err != nil {
		return fmt.Errorf("delete hot key chunk %s: %w", chunkID, err)
	}
	return nil
}
