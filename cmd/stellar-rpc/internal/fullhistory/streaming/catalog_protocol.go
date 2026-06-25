package streaming

import (
	"errors"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/metastore"
)

// errCommitBatchFaultInjected forces CommitIndex's batch to be dropped; only the
// test-only failCommitBatch hook (hooks.go) returns it. nil hook in production.
var errCommitBatchFaultInjected = errors.New("streaming: commit batch fault-injected (test only)")

// The one write protocol — mark-then-write. Every durable artifact (per-chunk
// file or index coverage) flows through here:
//
//  1. Put the key "freezing" via metastore BEFORE any I/O.
//  2. The caller writes the file.
//  3. The caller fsyncs the FILE + its PARENT dirent (+ the GRANDPARENT dirent
//     when the parent dir was just created) — barrierNewFile in paths.go.
//  4. Flip to "frozen": a single Put for per-chunk artifacts, or one atomic
//     Batch for the index (see CommitIndex).
//
// "frozen" is the only transition readers trust. The catalog owns steps 1 and 4
// (meta writes); the caller owns 2 and 3 (I/O).

// MarkChunkFreezing is step 1 for every requested kind. Re-marking a
// "freezing"/"pruning"/absent key is idempotent re-materialization; skipping a
// "frozen" kind (rule 1's per-kind idempotency) is the caller's job.
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

// FlipChunkFrozen is step 4 for per-chunk artifacts: flips every requested kind
// to "frozen". The caller MUST have completed barrierNewFile for every file first.
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

// MarkIndexFreezing is step 1 for the index, returning the IndexCoverage for
// CommitIndex. lo > hi panics (indexKey enforces it).
func (c *Catalog) MarkIndexFreezing(w WindowID, lo, hi chunk.ID) (IndexCoverage, error) {
	cov := IndexCoverage{
		Window: w,
		Lo:     lo,
		Hi:     hi,
		Key:    indexKey(w, lo, hi),
		State:  StateFreezing,
	}
	if err := c.store.Put(cov.Key, string(StateFreezing)); err != nil {
		return IndexCoverage{}, err
	}
	return cov, nil
}

// CommitIndex is step 4 for the index. In one atomic batch it:
//
//   - promotes cov ("freezing" -> "frozen");
//   - demotes the window's predecessor frozen coverage (if any) to "pruning";
//   - iff this build is terminal (cov.Hi == window's last chunk), demotes
//     every chunk:{c}:txhash key in the window to "pruning".
//
// The batch only DEMOTES keys — file deletion is the sweeps' job. So there is no
// instant with two frozen coverages, no live index unreachable, and no "frozen"
// chunk:c:txhash whose .bin was deleted.
//
// The caller MUST have fsynced the .idx file and its dir first. The predecessor
// is re-read from durable state, so this is safe to call after a crash.
func (c *Catalog) CommitIndex(cov IndexCoverage) error {
	// Compose demotions against durable state BEFORE opening the batch, so the
	// batch body is a pure sequence of puts.
	prev, hasPrev, err := c.FrozenCoverage(cov.Window)
	if err != nil {
		return err
	}
	if hasPrev && prev.Key == cov.Key {
		// Re-commit of an already-landed batch: nothing to demote against itself;
		// the promote below is an idempotent overwrite.
		hasPrev = false
	}

	terminal := c.windows.IsTerminalCoverage(cov)
	var txhashKeys []string
	if terminal {
		txhashKeys, err = c.windowTxhashKeysPresent(cov.Window)
		if err != nil {
			return err
		}
	}

	return c.store.Batch(func(bw *metastore.BatchWriter) error {
		bw.Put(cov.Key, string(StateFrozen))
		if hasPrev {
			bw.Put(prev.Key, string(StatePruning))
		}
		for _, k := range txhashKeys {
			bw.Put(k, string(StatePruning))
		}
		// Fault injection: lets a test assert the all-or-nothing property — none
		// of the puts above land.
		if c.hooks.commitBatchShouldFail() {
			return errCommitBatchFaultInjected
		}
		return nil
	})
}

// windowTxhashKeysPresent returns the chunk:{c}:txhash keys that EXIST in
// window [firstChunk, lastChunk], so the terminal commit demotes only present
// keys (the spec's cat.Has guard), never chunks whose .bin was never produced.
func (c *Catalog) windowTxhashKeysPresent(w WindowID) ([]string, error) {
	first := c.windows.FirstChunk(w)
	last := c.windows.LastChunk(w)
	var keys []string
	for cid := first; ; cid++ {
		key := chunkKey(cid, KindTxHash)
		ok, err := c.Has(key)
		if err != nil {
			return nil, err
		}
		if ok {
			keys = append(keys, key)
		}
		if cid == last { // inclusive upper bound; also guards chunk.ID wraparound
			break
		}
	}
	return keys, nil
}
