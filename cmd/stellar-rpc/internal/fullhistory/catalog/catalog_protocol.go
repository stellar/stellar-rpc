package catalog

import (
	"errors"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/metastore"
)

// The one write protocol — mark-then-write. Every durable artifact (per-chunk
// file or index coverage) flows through here:
//
//  1. Put the key "freezing" BEFORE any I/O.
//  2. Caller writes the file.
//  3. Caller fsyncs the file + parent dirent (+ grandparent on a new parent dir)
//     — geometry.BarrierNewFile.
//  4. Flip to "frozen" — one Put per-chunk, or one atomic Batch for the index.
//
// "frozen" is the only transition readers trust. The catalog owns steps 1 and 4
// (meta writes); the caller owns 2 and 3 (I/O).
//
// One writer per key: the read-then-act sequences here and in the sweeps are
// UNGUARDED against a racing second writer because the design rules it out
// (ingest and lifecycle never write the same key; resolve emits one build per
// window; one lifecycle run at a time). Crash re-runs stay safe because every
// step is idempotent and resolve re-plans from durable state.

// MarkChunkFreezing is step 1 for every requested kind. Re-marking a
// freezing/pruning/absent key is idempotent; skipping an already-frozen kind is
// the caller's job.
func (c *Catalog) MarkChunkFreezing(chunkID chunk.ID, kinds ...geometry.Kind) error {
	if len(kinds) == 0 {
		return errors.New("streaming: MarkChunkFreezing requires at least one kind")
	}
	return c.store.Batch(func(w *metastore.BatchWriter) error {
		for _, kind := range kinds {
			w.Put(geometry.ChunkKey(chunkID, kind), string(geometry.StateFreezing))
		}
		return nil
	})
}

// FlipChunkFrozen is step 4 for per-chunk artifacts: flips every kind to
// "frozen". The caller MUST have completed BarrierNewFile for every file first.
func (c *Catalog) FlipChunkFrozen(chunkID chunk.ID, kinds ...geometry.Kind) error {
	if len(kinds) == 0 {
		return errors.New("streaming: FlipChunkFrozen requires at least one kind")
	}
	return c.store.Batch(func(w *metastore.BatchWriter) error {
		for _, kind := range kinds {
			w.Put(geometry.ChunkKey(chunkID, kind), string(geometry.StateFrozen))
		}
		return nil
	})
}

// MarkTxHashIndexFreezing is step 1 for the index, returning the coverage for
// CommitTxHashIndex. lo > hi panics (TxHashIndexKey enforces it).
func (c *Catalog) MarkTxHashIndexFreezing(
	w geometry.TxHashIndexID, lo, hi chunk.ID,
) (geometry.TxHashIndexCoverage, error) {
	cov := geometry.TxHashIndexCoverage{
		Index: w,
		Lo:    lo,
		Hi:    hi,
		Key:   geometry.TxHashIndexKey(w, lo, hi),
		State: geometry.StateFreezing,
	}
	if err := c.store.Put(cov.Key, string(geometry.StateFreezing)); err != nil {
		return geometry.TxHashIndexCoverage{}, err
	}
	return cov, nil
}

// CommitTxHashIndex is step 4 for the index. In one atomic batch it:
//
//   - promotes cov ("freezing" -> "frozen");
//   - demotes the predecessor frozen coverage (if any) to "pruning";
//   - iff terminal (cov.Hi == index's last chunk), demotes every chunk:{c}:txhash
//     key in cov's [Lo, Hi] range to "pruning".
//
// The batch only DEMOTES (file deletion is the sweeps' job), so there is never an
// instant with two frozen coverages, an unreachable live index, or a "frozen"
// chunk:c:txhash whose .bin was deleted. A re-commit is an idempotent overwrite
// (crash re-run). The caller MUST have fsynced the .idx and its dir first; the
// predecessor is re-read from durable state, so this is crash-safe.
func (c *Catalog) CommitTxHashIndex(cov geometry.TxHashIndexCoverage) error {
	// Compose demotions against durable state BEFORE the batch, so the batch body
	// is a pure sequence of puts.
	prev, hasPrev, err := c.FrozenTxHashIndex(cov.Index)
	if err != nil {
		return err
	}
	if hasPrev && prev.Key == cov.Key {
		// Re-commit of an already-landed batch: nothing to demote against itself.
		hasPrev = false
	}

	var txhashKeys []string
	if c.txhashIndex.IsTerminalCoverage(cov) {
		txhashKeys, err = c.txhashIndexChunkKeysPresent(cov.Lo, cov.Hi)
		if err != nil {
			return err
		}
	}

	return c.store.Batch(func(bw *metastore.BatchWriter) error {
		bw.Put(cov.Key, string(geometry.StateFrozen))
		if hasPrev {
			bw.Put(prev.Key, string(geometry.StatePruning))
		}
		for _, k := range txhashKeys {
			bw.Put(k, string(geometry.StatePruning))
		}
		return nil
	})
}

// txhashIndexChunkKeysPresent returns the chunk:{c}:txhash keys that EXIST in
// [lo, hi]. A terminal commit passes cov's own range, so it demotes only the .bin
// inputs the new .idx covers — never a key below cov.Lo (whose .bin must survive
// for its own index) nor a chunk whose .bin was never produced.
func (c *Catalog) txhashIndexChunkKeysPresent(lo, hi chunk.ID) ([]string, error) {
	var keys []string
	for cid := lo; cid <= hi; cid++ {
		key := geometry.ChunkKey(cid, geometry.KindTxHash)
		ok, err := c.has(key)
		if err != nil {
			return nil, err
		}
		if ok {
			keys = append(keys, key)
		}
	}
	return keys, nil
}

// --- Hot-DB key bracket: the file protocol's transient/ready bracket applied to
// the chunk's hot directory. ---

// PutHotTransient marks a hot-DB key "transient" — the open end, written before
// the dir is created or a discard begins removing it. A crash mid-operation is
// detectable from this value alone.
func (c *Catalog) PutHotTransient(chunkID chunk.ID) error {
	return c.store.Put(geometry.HotChunkKey(chunkID), string(geometry.HotTransient))
}

// FlipHotReady marks a hot-DB key "ready" (dir exists and usable). The caller
// MUST have fsynced the dir (and its parent on creation) first.
func (c *Catalog) FlipHotReady(chunkID chunk.ID) error {
	return c.store.Put(geometry.HotChunkKey(chunkID), string(geometry.HotReady))
}

// DeleteHotKey removes a hot-DB key — the close end, after rmdir. Idempotent.
func (c *Catalog) DeleteHotKey(chunkID chunk.ID) error {
	return c.store.Delete(geometry.HotChunkKey(chunkID))
}
