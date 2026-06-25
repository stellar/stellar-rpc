package catalog

import (
	"errors"
	"fmt"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/metastore"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/streaming/geometry"
)

// The one write protocol — mark-then-write. Every durable artifact (per-chunk
// file or index coverage) flows through here:
//
//  1. Put the key "freezing" via metastore BEFORE any I/O.
//  2. The caller writes the file.
//  3. The caller fsyncs the FILE + its PARENT dirent (+ the GRANDPARENT dirent
//     when the parent dir was just created) — geometry.BarrierNewFile.
//  4. Flip to "frozen": a single Put for per-chunk artifacts, or one atomic
//     Batch for the index (see CommitTxHashIndex).
//
// "frozen" is the only transition readers trust. The catalog owns steps 1 and 4
// (meta writes); the caller owns 2 and 3 (I/O).

// MarkChunkFreezing is step 1 for every requested kind. Re-marking a
// "freezing"/absent key is idempotent re-materialization; skipping an
// already-"frozen" kind (per-kind idempotency) is the caller's job.
//
// It REFUSES to re-materialize a "pruning" key. "pruning" means a sweep has
// already claimed that artifact for deletion (a terminal index commit demoted
// its .bin, or prune demoted it below the retention floor). Re-marking it
// "freezing" here would let a writer fsync a fresh file that the in-flight sweep
// still unlinks, after which FlipChunkFrozen would leave a "frozen" key pointing
// at a deleted file. The rebuild must wait until the sweep has removed the key.
// (The single-step lifecycle never overlaps a sweep with a rebuild today; the
// guard keeps the method safe if that ever changes, paired with the sweeps'
// current-state re-read in catalog_sweep.go.)
func (c *Catalog) MarkChunkFreezing(chunkID chunk.ID, kinds ...geometry.Kind) error {
	if len(kinds) == 0 {
		return errors.New("streaming: MarkChunkFreezing requires at least one kind")
	}
	for _, kind := range kinds {
		s, err := c.State(chunkID, kind)
		if err != nil {
			return err
		}
		if s == geometry.StatePruning {
			return fmt.Errorf(
				"streaming: refuse to re-materialize %q: key is pruning (a sweep owns it)",
				geometry.ChunkKey(chunkID, kind),
			)
		}
	}
	return c.store.Batch(func(w *metastore.BatchWriter) error {
		for _, kind := range kinds {
			w.Put(geometry.ChunkKey(chunkID, kind), string(geometry.StateFreezing))
		}
		return nil
	})
}

// FlipChunkFrozen is step 4 for per-chunk artifacts: flips every requested kind
// to "frozen". The caller MUST have completed geometry.BarrierNewFile for every
// file first.
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

// MarkTxHashIndexFreezing is step 1 for the index, returning the TxHashIndexCoverage for
// CommitTxHashIndex. lo > hi panics (geometry.TxHashIndexKey enforces it).
func (c *Catalog) MarkTxHashIndexFreezing(w geometry.TxHashIndexID, lo, hi chunk.ID) (geometry.TxHashIndexCoverage, error) {
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
//   - demotes the index's predecessor frozen coverage (if any) to "pruning";
//   - iff this build is terminal (cov.Hi == index's last chunk), demotes the
//     chunk:{c}:txhash key of every chunk in cov's [Lo, Hi] range to "pruning".
//
// The batch only DEMOTES keys — file deletion is the sweeps' job. So there is no
// instant with two frozen coverages, no live index unreachable, and no "frozen"
// chunk:c:txhash whose .bin was deleted.
//
// Two guards keep it safe against retried or out-of-order builds for the same
// index (which the single-step lifecycle never produces today, but a crash
// re-run or a future concurrent builder could):
//
//   - A re-commit of the already-frozen coverage is an idempotent overwrite.
//   - A commit whose range the frozen coverage already spans (a superset) is
//     REFUSED: promoting it would regress FrozenTxHashIndex to a shorter index,
//     and a terminal predecessor may already have demoted the .bin inputs needed
//     to rebuild the longer one. The stale "freezing" key is left for a sweep.
//
// The caller MUST have fsynced the .idx file and its dir first. The predecessor
// is re-read from durable state, so this is safe to call after a crash.
func (c *Catalog) CommitTxHashIndex(cov geometry.TxHashIndexCoverage) error {
	// Compose demotions against durable state BEFORE opening the batch, so the
	// batch body is a pure sequence of puts.
	prev, hasPrev, err := c.FrozenTxHashIndex(cov.Index)
	if err != nil {
		return err
	}
	switch {
	case hasPrev && prev.Key == cov.Key:
		// Re-commit of an already-landed batch: nothing to demote against itself;
		// the promote below is an idempotent overwrite.
		hasPrev = false
	case hasPrev && prev.Lo <= cov.Lo && cov.Hi <= prev.Hi:
		// Stale/out-of-order commit: the frozen coverage already spans a superset
		// of cov's range, so promoting cov would regress to a shorter index.
		return fmt.Errorf(
			"streaming: refuse stale tx-hash index commit %q: frozen coverage %q already spans [%s,%s]",
			cov.Key, prev.Key, prev.Lo, prev.Hi,
		)
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
// the inclusive chunk range [lo, hi]. A terminal commit passes cov's own range,
// so it demotes only the .bin inputs the new .idx actually covers — never a key
// below cov.Lo (whose ledgers the new index cannot answer, and whose .bin must
// survive for its own index's build) and never a chunk whose .bin was never
// produced (the spec's cat.Has guard).
func (c *Catalog) txhashIndexChunkKeysPresent(lo, hi chunk.ID) ([]string, error) {
	var keys []string
	for cid := lo; ; cid++ {
		key := geometry.ChunkKey(cid, geometry.KindTxHash)
		ok, err := c.has(key)
		if err != nil {
			return nil, err
		}
		if ok {
			keys = append(keys, key)
		}
		if cid == hi { // inclusive upper bound; also guards chunk.ID wraparound
			break
		}
	}
	return keys, nil
}
