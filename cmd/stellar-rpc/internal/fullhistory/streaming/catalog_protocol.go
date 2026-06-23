package streaming

import (
	"errors"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/metastore"
)

// The one write protocol — mark-then-write. Every durable per-chunk artifact
// flows through here:
//
//  1. Put the key "freezing" via metastore BEFORE any I/O.
//  2. The caller writes the file.
//  3. The caller fsyncs the FILE + its PARENT dirent (+ the GRANDPARENT dirent
//     when the parent dir was just created) — barrierNewFile in paths.go.
//  4. Flip to "frozen": a single Put for per-chunk artifacts.
//
// The pre-mark gives "every file on disk has its meta key"; the dirent
// barriers guarantee the key never outlives the file's creation; the frozen
// flip is the only transition readers trust. The catalog owns steps 1 and 4
// (meta-store writes); the caller owns steps 2 and 3 (I/O), calling
// MarkChunkFreezing before and FlipChunkFrozen after.

// MarkChunkFreezing puts every requested kind's key to "freezing" in one
// atomic synced batch, BEFORE any file I/O. Re-marking a "freezing"/"pruning"/
// absent key is the idempotent re-materialization entry; a "frozen" kind is
// the caller's to skip (rule 1's per-kind idempotency), not this helper's.
func (c *Catalog) MarkChunkFreezing(chunkID chunk.ID, kinds ...Kind) error {
	if len(kinds) == 0 {
		return errors.New("streaming: MarkChunkFreezing requires at least one kind")
	}
	return c.store.Batch(func(w *metastore.BatchWriter) error {
		for _, kind := range kinds {
			w.Put(chunkKey(chunkID, kind), string(StateFreezing))
		}
		return nil
	})
}

// FlipChunkFrozen flips every requested kind's key to "frozen" in one atomic
// synced batch. The caller MUST have completed barrierNewFile for every file
// first — "frozen" means durable and complete, trusted blindly downstream.
func (c *Catalog) FlipChunkFrozen(chunkID chunk.ID, kinds ...Kind) error {
	if len(kinds) == 0 {
		return errors.New("streaming: FlipChunkFrozen requires at least one kind")
	}
	return c.store.Batch(func(w *metastore.BatchWriter) error {
		for _, kind := range kinds {
			w.Put(chunkKey(chunkID, kind), string(StateFrozen))
		}
		return nil
	})
}

// ---------------------------------------------------------------------------
// Hot-DB key bracket. The directory operation's two ends: PutHotTransient
// before the dir is created (or before a discard rmdirs it), FlipHotReady
// after the dir is durable, DeleteHotKey after the rmdir completes. The
// "transient"/"ready" bracket is the same two ideas the file protocol uses,
// applied to a directory.
// ---------------------------------------------------------------------------

// PutHotTransient marks a hot-DB key "transient" — the bracket's open end,
// written before the directory is created or before a discard begins removing
// it. A crash mid-operation is detectable from this value alone.
func (c *Catalog) PutHotTransient(chunkID chunk.ID) error {
	// Test-only observation point at the exact instant a hot key is about to be
	// created (a no-op in production). At a boundary handoff this is when the
	// next chunk's key appears — the ingestion loop guarantees the predecessor's
	// write handle is already closed here (close-before-create-key).
	c.hooks.fireBeforeHotTransient(chunkID)
	return c.store.Put(hotChunkKey(chunkID), string(HotTransient))
}

// FlipHotReady marks a hot-DB key "ready" — the dir exists and is usable. The
// caller MUST have fsynced the dir (and its parent on creation) first.
func (c *Catalog) FlipHotReady(chunkID chunk.ID) error {
	return c.store.Put(hotChunkKey(chunkID), string(HotReady))
}

// DeleteHotKey removes a hot-DB key — the bracket's close end, after rmdir
// completes. Idempotent on a missing key.
func (c *Catalog) DeleteHotKey(chunkID chunk.ID) error {
	return c.store.Delete(hotChunkKey(chunkID))
}
