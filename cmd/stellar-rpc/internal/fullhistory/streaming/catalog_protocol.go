package streaming

import (
	"errors"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/metastore"
)

// errCommitBatchFaultInjected is returned only by the test-only
// failCommitBatch hook (hooks.go) to force CommitIndex's batch to be dropped.
// It never surfaces in production, where the hook is nil.
var errCommitBatchFaultInjected = errors.New("streaming: commit batch fault-injected (test only)")

// The one write protocol — mark-then-write. Every durable artifact (per-chunk
// file or index coverage) flows through here:
//
//  1. Put the key "freezing" via metastore BEFORE any I/O.
//  2. The caller writes the file.
//  3. The caller fsyncs the FILE + its PARENT dirent (+ the GRANDPARENT dirent
//     when the parent dir was just created) — barrierNewFile in paths.go.
//  4. Flip to "frozen": a single Put for per-chunk artifacts, or one atomic
//     Batch for the index (promote new coverage + demote predecessor + on a
//     terminal build demote every in-window chunk:{c}:txhash key).
//
// The pre-mark gives "every file on disk has its meta key"; the dirent
// barriers guarantee the key never outlives the file's creation; the frozen
// flip is the only transition readers trust. The catalog owns steps 1 and 4
// (meta-store writes); the caller owns steps 2 and 3 (I/O), calling
// MarkChunkFreezing/MarkIndexFreezing before and FlipChunkFrozen/CommitIndex
// after.

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

// MarkIndexFreezing puts the coverage's key to "freezing" before any index
// I/O. It returns the IndexCoverage (with State set) the caller threads into
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

// CommitIndex is the index's frozen flip — the batch extension of the one
// write protocol and the ENTIRE finalization protocol. In one atomic synced
// batch it:
//
//   - promotes cov ("freezing" -> "frozen");
//   - demotes the window's predecessor frozen coverage (if any) to "pruning";
//   - iff this build is terminal (cov.Hi == window's last chunk), demotes
//     every chunk:{c}:txhash key in the window to "pruning".
//
// The batch only ever DEMOTES keys and unlinks nothing — file deletion is
// exclusively the sweeps' job. A crash before this lands leaves the
// predecessor frozen and cov as "freezing" debris; a crash after leaves cov
// frozen and the demoted keys as "pruning" sweep work. There is no instant
// with two frozen coverages, no live index unreachable, and no "frozen"
// chunk:c:txhash whose .bin was deleted.
//
// The caller MUST have fsynced the .idx file and its dir first. CommitIndex
// re-reads the predecessor inside the batch-composition phase from durable
// state, so it is safe to call after a crash without external bookkeeping.
func (c *Catalog) CommitIndex(cov IndexCoverage) error {
	// Compose the demotions against durable state BEFORE opening the batch, so
	// the batch body is a pure sequence of puts (the scans below read the same
	// store the batch will write, but only keys this batch does not also
	// write — the predecessor differs from cov, and the txhash keys are a
	// different family).
	prev, hasPrev, err := c.FrozenCoverage(cov.Window)
	if err != nil {
		return err
	}
	if hasPrev && prev.Key == cov.Key {
		// The predecessor IS this coverage already frozen — a re-commit of an
		// already-landed batch. Nothing to demote against itself; the promote
		// below is an idempotent overwrite.
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
		// Fault injection: returning an error here makes metastore drop the
		// whole batch, so a test can assert none of the puts above became
		// observable — the all-or-nothing property the protocol depends on.
		if c.hooks.commitBatchShouldFail() {
			return errCommitBatchFaultInjected
		}
		return nil
	})
}

// windowTxhashKeysPresent returns the chunk:{c}:txhash keys that EXIST in the
// window [firstChunk, lastChunk], so the terminal commit demotes only present
// keys (matching the spec's cat.Has guard) rather than conjuring keys for
// chunks whose .bin was never produced.
func (c *Catalog) windowTxhashKeysPresent(w WindowID) ([]string, error) {
	first := c.windows.FirstChunk(w)
	last := c.windows.LastChunk(w)
	var keys []string
	for cid := first; cid <= last; cid++ {
		ok, err := c.Has(chunkKey(cid, KindTxHash))
		if err != nil {
			return nil, err
		}
		if ok {
			keys = append(keys, chunkKey(cid, KindTxHash))
		}
		if cid == last { // guard against chunk.ID wraparound at the top
			break
		}
	}
	return keys, nil
}
