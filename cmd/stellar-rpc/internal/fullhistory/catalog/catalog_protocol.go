package catalog

import (
	"errors"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/metastore"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
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
//
// One writer per key. The read-then-act sequences here and in the sweeps
// (catalog_sweep.go) — read a key/coverage, then Put or unlink based on it — are
// deliberately UNGUARDED against a second writer racing the same key, because the
// design's Concurrency model rules that out: the ingest thread and the lifecycle
// thread write the catalog at the same time but NEVER the same key; resolve emits
// exactly one index build per window (so even concurrent backfill never has two
// builds for one index); and only one lifecycle run executes at a time. Crash
// re-runs stay safe not because of any in-method guard but because every step is
// idempotent and resolve re-plans from durable state. See the design's
// "Concurrency model" section.

// OneWrite runs the protocol's fixed step order for a single freeze, returning on
// the first step that errors: mark the target keys "freezing" (step 1), create/
// write the artifact(s) (step 2), fsync the durability barrier (step 3), then flip
// the keys "frozen" (step 4 / commit). The states and the create/barrier steps
// differ per freeze site; the order does not. Centralizing it keeps the freeze
// sites — cold-chunk materialization, tx-hash index rebuild, and (Phase 2 / #820)
// the hot-tier open — from drifting out of the crash-safe order. Callers supply
// the catalog's mark/flip as the step-1/4 closures and their own I/O as step 2/3.
func OneWrite(mark, create, barrier, flip func() error) error {
	if err := mark(); err != nil {
		return err
	}
	if err := create(); err != nil {
		return err
	}
	if err := barrier(); err != nil {
		return err
	}
	return flip()
}

// MarkChunkFreezing is step 1 for every requested kind. Re-marking a
// "freezing"/"pruning"/absent key is idempotent re-materialization; skipping an
// already-"frozen" kind (per-kind idempotency) is the caller's job.
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
//   - demotes the index's predecessor frozen coverage (if any) to "pruning";
//   - iff this build is terminal (cov.Hi == index's last chunk), demotes the
//     chunk:{c}:txhash key of every chunk in cov's [Lo, Hi] range to "pruning".
//
// The batch only DEMOTES keys — file deletion is the sweeps' job. So there is no
// instant with two frozen coverages, no live index unreachable, and no "frozen"
// chunk:c:txhash whose .bin was deleted.
//
// A re-commit of the already-frozen coverage is an idempotent overwrite — the
// crash-re-run case. There is no guard against an out-of-order or duplicate build
// for the same index: the design's Concurrency model precludes it (resolve emits
// one build per window; one lifecycle run at a time — see the header note).
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
	if hasPrev && prev.Key == cov.Key {
		// Re-commit of an already-landed batch: nothing to demote against itself;
		// the promote below is an idempotent overwrite.
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
