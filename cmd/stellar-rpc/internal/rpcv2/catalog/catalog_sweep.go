package catalog

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/durable"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
)

// Key-driven deletion bodies, one per key family (chunk artifacts, index
// coverages, hot chunks). Every file deleter in the system goes through here.
// All follow the same load-bearing order:
//
//	demote-if-still-"frozen" -> unlink file(s) -> fsyncDir(parent) -> delete key
//
// The key outlives the durable unlink, giving the exit invariant
// "key absent => file gone": a crash anywhere leaves the key in place and the
// sweep re-runs. Deleting the key first would orphan a file with no key — the
// one class this design cannot find.

// SweepChunkArtifacts deletes a batch of per-chunk refs — their files and keys.
func (c *Catalog) SweepChunkArtifacts(refs []ArtifactRef) error {
	if err := c.DemoteChunkArtifacts(refs); err != nil {
		return err
	}
	return c.DestroyChunkArtifacts(refs)
}

// DemoteChunkArtifacts marks every "frozen" ref in the batch "pruning", so the
// later unlink never runs under a "frozen" key. Refs already "freezing" or
// "pruning" are left as-is. Idempotent.
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

// DestroyChunkArtifacts unlinks the files and deletes the keys for a batch of refs
// the caller has demoted. It re-reads each ref's current state and destroys only a
// demoted ("pruning") or debris ("freezing") ref: a still-"frozen" ref is skipped
// with a warning so an un-demoted ref cannot delete a live artifact, and an absent
// key skips silently (already destroyed). Files are unlinked (durably) before their
// keys, so a crash in between leaves the key for the next run to finish — never a
// file with no key. Idempotent.
func (c *Catalog) DestroyChunkArtifacts(refs []ArtifactRef) error {
	if len(refs) == 0 {
		return nil
	}
	// Re-read live state; destroy only demoted refs, collecting them for the
	// key-delete batch and their files' parents for the fsync barrier.
	var paths []string
	var demoted []ArtifactRef
	for _, ref := range refs {
		st, err := c.State(ref.Chunk, ref.Kind)
		if err != nil {
			return err
		}
		if st == geometry.StateFrozen {
			c.logger.Warnf("catalog: destroy skipped still-frozen artifact %s — not demoted (caller bug)", ref.Key())
			continue
		}
		if st != geometry.StatePruning && st != geometry.StateFreezing {
			continue // absent: already destroyed
		}
		for _, p := range c.layout.ArtifactPaths(ref.Chunk, ref.Kind) {
			if err := durable.DeleteFileIfExists(p); err != nil {
				return err
			}
			paths = append(paths, p)
		}
		demoted = append(demoted, ref)
	}
	if err := durable.FsyncParentDirs(paths); err != nil { // unlinks durable BEFORE keys
		return err
	}
	// Delete the keys — only now that the unlinks are durable.
	return c.batch(func(w batchWriter) error {
		for _, ref := range demoted {
			w.Delete(ref.Key())
		}
		return nil
	})
}

// DemoteTxHashIndexKey demotes a "frozen" coverage to "pruning" so no unlink later
// happens under a frozen key; "freezing"/"pruning" debris is left as-is. cov.State
// is the caller's observation; the one-writer-per-key invariant (catalog_protocol.go)
// means no concurrent writer changed the durable value under it. Idempotent.
func (c *Catalog) DemoteTxHashIndexKey(cov geometry.TxHashIndexCoverage) error {
	if cov.State == geometry.StateFrozen {
		return c.put(cov.Key, string(geometry.StatePruning))
	}
	return nil
}

// DestroyTxHashIndexKey unlinks one coverage's file and deletes its key, for a
// coverage the caller has already demoted. It re-reads the key's current state
// first (cov.State is the caller's observation, taken before the deferred-destroy
// wait): a still-"frozen" coverage is skipped with a warning so an un-demoted key
// cannot delete a live index, and an absent key is a no-op (already destroyed).
// Unlink is made durable BEFORE the key delete, so the key outlives the file and
// a crash re-runs. Idempotent.
func (c *Catalog) DestroyTxHashIndexKey(cov geometry.TxHashIndexCoverage) error {
	st, ok, err := c.get(cov.Key)
	if err != nil {
		return err
	}
	if !ok {
		return nil // already destroyed
	}
	if geometry.State(st) == geometry.StateFrozen {
		c.logger.Warnf("catalog: destroy skipped still-frozen index coverage %s — not demoted (caller bug)", cov.Key)
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

// DestroyHotChunk removes a hot chunk's dir and key — the destroy half of the
// discard (deferred deletion marks the chunk "transient" during a stage, then
// destroys at end of run), following the same crash order as the two sweeps
// above: rmdir -> fsync(parent) -> delete key. It re-reads the hot key first: a chunk not marked
// "transient" is skipped with a warning so an un-demoted key cannot delete a live
// hot DB, and an absent key is a no-op (already destroyed). The caller MUST have
// closed the chunk's hot handle. rmdir is made durable BEFORE the key delete, so
// the key outlives the dir and a crash re-runs the destroy rather than leaving a
// key-less dir. Idempotent.
func (c *Catalog) DestroyHotChunk(chunkID chunk.ID) error {
	st, err := c.HotState(chunkID)
	if err != nil {
		return fmt.Errorf("read hot key chunk %s: %w", chunkID, err)
	}
	if st == "" {
		return nil // already destroyed
	}
	if st != geometry.HotTransient {
		c.logger.Warnf("catalog: destroy skipped hot chunk %s in state %q — not marked transient (caller bug)", chunkID, st)
		return nil
	}
	dir := c.layout.HotChunkPath(chunkID)
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("rmdir hot dir %s: %w", dir, err)
	}
	if err := durable.FsyncDir(filepath.Dir(dir)); err != nil {
		return fmt.Errorf("fsync hot parent dir %s: %w", filepath.Dir(dir), err)
	}
	if err := c.deleteHotKey(chunkID); err != nil {
		return fmt.Errorf("delete hot key chunk %s: %w", chunkID, err)
	}
	return nil
}
