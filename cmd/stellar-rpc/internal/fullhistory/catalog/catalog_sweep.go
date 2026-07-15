package catalog

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/durable"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
)

// Key-driven sweeps — the ONLY deletion bodies in the system, one per key
// family. Each sweep is split into its two halves so the caller can run them
// at different times (the #772 registry defers the destructive half behind the
// reaper's grace period, while tests and startup backfill run both inline):
//
//	demote:  if the key is still "frozen", flip it to "pruning"  (runs NOW)
//	destroy: unlink file(s) -> fsyncDir(parent) -> delete key    (may run LATER)
//
// The key outlives the durable unlink, giving the exit invariant
// "key absent => file gone": a crash anywhere leaves the key in place and the
// sweep re-runs. Deleting the key first would orphan a file with no key — the
// one class this design cannot find.
//
// Because a destroy may run long after its demote, every destroy re-reads the
// key first and SKIPS if the state went back to "frozen" (or "ready" for hot
// chunks) in the meantime. No current flow re-freezes a demoted key before its
// destroy runs — floors and index coverages only move forward — so the guard
// is defense-in-depth: if a future flow ever does, the destroy declines to
// delete a serving artifact instead of silently gutting it.

// SweepChunkArtifacts deletes the files and keys for a batch of per-chunk refs
// in one pass: DemoteChunkArtifacts then DestroyChunkArtifacts.
func (c *Catalog) SweepChunkArtifacts(refs []ArtifactRef) error {
	if err := c.DemoteChunkArtifacts(refs); err != nil {
		return err
	}
	return c.DestroyChunkArtifacts(refs)
}

// DemoteChunkArtifacts flips every still-"frozen" ref to "pruning" in one
// batch, so no later unlink happens under a "frozen" key. Refs already
// "freezing"/"pruning" are left as they are.
func (c *Catalog) DemoteChunkArtifacts(refs []ArtifactRef) error {
	if len(refs) == 0 {
		return nil
	}
	return c.batch(func(w batchWriter) error {
		for _, ref := range refs {
			if ref.State == geometry.StateFrozen {
				w.Put(ref.Key(), string(geometry.StatePruning))
			}
		}
		return nil
	})
}

// DestroyChunkArtifacts unlinks every ref's files and deletes their keys:
// unlink -> fsync parents -> delete keys. The caller must have demoted the
// refs first (DemoteChunkArtifacts) — possibly much earlier, when the destroy
// is deferred through the registry's reaper.
//
// Each ref's durable state is re-read first: an absent key means an earlier
// destroy already finished (nothing to do), and a "frozen" key means the
// artifact was re-frozen since the demote — it is serving again, so it is
// skipped, never unlinked.
func (c *Catalog) DestroyChunkArtifacts(refs []ArtifactRef) error {
	if len(refs) == 0 {
		return nil
	}

	// Re-read each key and keep only the refs still awaiting destruction.
	live := make([]ArtifactRef, 0, len(refs))
	for _, ref := range refs {
		state, err := c.State(ref.Chunk, ref.Kind)
		if err != nil {
			return err
		}
		switch state {
		case geometry.State(""):
			continue // already destroyed; "key absent => file gone"
		case geometry.StateFrozen:
			c.logger.WithField("key", ref.Key()).
				Warn("catalog: destroy found the key re-frozen; leaving the artifact alone")
			continue
		default:
			live = append(live, ref)
		}
	}
	if len(live) == 0 {
		return nil
	}

	// Unlink every file (idempotent), collecting parents for the barrier.
	var paths []string
	for _, ref := range live {
		for _, p := range c.layout.ArtifactPaths(ref.Chunk, ref.Kind) {
			if err := durable.DeleteFileIfExists(p); err != nil {
				return err
			}
			paths = append(paths, p)
		}
	}
	if err := durable.FsyncParentDirs(paths); err != nil { // unlinks durable BEFORE keys
		return err
	}

	// Delete the keys — only now that the unlinks are durable.
	return c.batch(func(w batchWriter) error {
		for _, ref := range live {
			w.Delete(ref.Key())
		}
		return nil
	})
}

// SweepTxHashIndexKey deletes one index coverage's file and key in one pass:
// DemoteTxHashIndexKey then DestroyTxHashIndexKey. cov.State is the caller's
// observation; the one-writer-per-key invariant (see catalog_protocol.go)
// means no concurrent writer can have changed the durable value under it.
func (c *Catalog) SweepTxHashIndexKey(cov geometry.TxHashIndexCoverage) error {
	if err := c.DemoteTxHashIndexKey(cov); err != nil {
		return err
	}
	return c.DestroyTxHashIndexKey(cov)
}

// DemoteTxHashIndexKey flips a still-"frozen" coverage to "pruning" so no
// later unlink happens under a "frozen" key. "Freezing" debris (a crashed
// build) and already-"pruning" coverages need no demotion.
func (c *Catalog) DemoteTxHashIndexKey(cov geometry.TxHashIndexCoverage) error {
	if cov.State != geometry.StateFrozen {
		return nil
	}
	return c.put(cov.Key, string(geometry.StatePruning))
}

// DestroyTxHashIndexKey unlinks one coverage's .idx file and deletes its key:
// unlink -> fsyncDir -> delete key -> best-effort rmdir. The caller must have
// demoted the coverage first (DemoteTxHashIndexKey) — possibly much earlier,
// when the destroy is deferred through the registry's reaper.
//
// The durable key is re-read first: absent means an earlier destroy already
// finished, and "frozen" means the coverage was re-frozen since the demote —
// it is serving again, so it is skipped, never unlinked.
func (c *Catalog) DestroyTxHashIndexKey(cov geometry.TxHashIndexCoverage) error {
	state, present, err := c.get(cov.Key)
	if err != nil {
		return err
	}
	if !present {
		return nil // already destroyed; "key absent => file gone"
	}
	if geometry.State(state) == geometry.StateFrozen {
		c.logger.WithField("key", cov.Key).
			Warn("catalog: destroy found the coverage re-frozen; leaving the index alone")
		return nil
	}

	path := c.layout.TxHashIndexFilePath(cov)
	if err := durable.DeleteFileIfExists(path); err != nil {
		return err
	}
	dir := c.layout.TxHashIndexDir(cov.Index)
	if err := durable.FsyncDir(dir); err != nil { // unlink durable BEFORE key delete
		return err
	}
	if err := c.del(cov.Key); err != nil {
		return err
	}
	durable.RmdirIfEmpty(dir) // best-effort; an empty dir is not an artifact
	return nil
}

// DiscardHotChunk retires a chunk's hot DB once its cold artifacts are durable
// (or it fell past retention) in one pass: mark "transient" then
// DestroyHotChunk. The caller MUST have closed the chunk's hot write handle.
// Idempotent; an absent key is a no-op.
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
	return c.DestroyHotChunk(chunkID)
}

// DestroyHotChunk removes a hot chunk's directory and deletes its key:
// rmdir -> fsync(parent) -> delete key. The caller must have demoted the key
// to "transient" first (PutHotTransient) and closed the chunk's write handle
// — when the destroy is deferred through the registry's reaper, the reaper
// closes the handle immediately before running this.
//
// The key outlives the durable rmdir, so a crash anywhere leaves it
// "transient" for the next discard scan to finish. The durable key is re-read
// first: absent means an earlier destroy already finished, and "ready" means
// the chunk was re-created since the demote — it is serving again, so it is
// skipped, never removed.
func (c *Catalog) DestroyHotChunk(chunkID chunk.ID) error {
	state, err := c.HotState(chunkID)
	if err != nil {
		return fmt.Errorf("read hot key chunk %s: %w", chunkID, err)
	}
	if state == "" {
		return nil // already destroyed
	}
	if state == geometry.HotReady {
		c.logger.WithField("chunk", chunkID.String()).
			Warn("catalog: destroy found the hot key ready again; leaving the DB alone")
		return nil
	}
	dir := c.layout.HotChunkPath(chunkID)
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("rmdir hot dir %s: %w", dir, err)
	}
	// rmdir durable BEFORE the key delete: the key outlives the dir, so a crash
	// re-runs the discard rather than leaving a key-less dir.
	if err := durable.FsyncDir(filepath.Dir(dir)); err != nil {
		return fmt.Errorf("fsync hot parent dir %s: %w", filepath.Dir(dir), err)
	}
	if err := c.deleteHotKey(chunkID); err != nil {
		return fmt.Errorf("delete hot key chunk %s: %w", chunkID, err)
	}
	return nil
}
