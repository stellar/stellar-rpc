package catalog

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/durable"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
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

// SweepChunkArtifacts deletes the files and keys for a batch of per-chunk refs,
// as one immediate demote-then-destroy. Deferred deletion instead calls
// DemoteChunkArtifacts during a stage and DestroyChunkArtifacts at end of run.
func (c *Catalog) SweepChunkArtifacts(refs []ArtifactRef) error {
	if err := c.DemoteChunkArtifacts(refs); err != nil {
		return err
	}
	return c.DestroyChunkArtifacts(refs)
}

// DemoteChunkArtifacts demotes every still-"frozen" ref in the batch to "pruning"
// (in one write), so no unlink later happens under a frozen key. "freezing"/
// "pruning" refs are already past frozen and left as-is. Idempotent.
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

// DestroyChunkArtifacts unlinks the files and deletes the keys for a batch of
// refs the caller has already demoted. The batch shares one fsync pass and one
// key-delete; unlinks are made durable BEFORE the keys, so the key outlives the
// file and a crash re-runs the destroy. Idempotent: absent files/keys are no-ops.
func (c *Catalog) DestroyChunkArtifacts(refs []ArtifactRef) error {
	if len(refs) == 0 {
		return nil
	}
	// Unlink every file (idempotent), collecting parents for the barrier.
	var paths []string
	for _, ref := range refs {
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
		for _, ref := range refs {
			w.Delete(ref.Key())
		}
		return nil
	})
}

// SweepTxHashIndexKey deletes one index coverage's file and key, as one immediate
// demote-then-destroy. Deferred deletion instead calls DemoteTxHashIndexKey during
// a stage and DestroyTxHashIndexKey at end of run.
func (c *Catalog) SweepTxHashIndexKey(cov geometry.TxHashIndexCoverage) error {
	if err := c.DemoteTxHashIndexKey(cov); err != nil {
		return err
	}
	return c.DestroyTxHashIndexKey(cov)
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
// coverage the caller has already demoted. Unlink is made durable BEFORE the key
// delete, so the key outlives the file and a crash re-runs. Idempotent.
func (c *Catalog) DestroyTxHashIndexKey(cov geometry.TxHashIndexCoverage) error {
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
	return c.DestroyHotChunk(chunkID)
}

// DestroyHotChunk removes a hot chunk's dir and key — the destroy half of the
// discard, split out for deferred deletion (which demotes during a stage, then
// destroys at end of run). The caller MUST have marked the chunk transient and
// closed its hot handle. rmdir is made durable BEFORE the key delete, so the key
// outlives the dir and a crash re-runs the destroy rather than leaving a key-less
// dir. Idempotent: an absent dir/key is a no-op.
func (c *Catalog) DestroyHotChunk(chunkID chunk.ID) error {
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
