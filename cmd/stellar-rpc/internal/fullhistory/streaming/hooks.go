package streaming

import "github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"

// crashHooks are test-only fault-injection points at the load-bearing instants
// of the one-write protocol and the sweeps. In production every field is nil
// and every call site is a no-op nil-check.
//
// They exist because the crash-safety invariants are properties of the ORDER of
// operations inside the real methods (catalog_sweep.go, catalog_protocol.go): a
// hand-replayed sweep can stay green after the production order breaks, but a
// hook fired from INSIDE the real method cannot. Each fires at the exact instant
// between two steps so a test can assert the invariant that order guarantees:
//
//   - beforeKeyDelete fires AFTER unlink+fsync, BEFORE the key delete. Asserts
//     file-gone-implies-key-present: reorder the delete ahead of the unlink and
//     the file would still be on disk here.
//   - beforeUnlink fires AFTER the frozen→pruning demote, BEFORE the unlink.
//     Asserts never-unlink-under-a-frozen-key: the value must read "pruning";
//     drop the demote and it would still be "frozen".
//   - failCommitBatch, when true, forces a recovery batch callback to error so
//     the batch is dropped wholesale. Asserts all-or-nothing: nothing it would
//     have written is observable.
//   - afterMarkFreezing fires INSIDE processChunk, AFTER MarkChunkFreezing and
//     BEFORE any file I/O. Asserts mark-then-write: every requested kind reads
//     "freezing" and no artifact file exists yet, so every file on disk stays
//     reachable from a key.
//   - beforeHotTransient fires INSIDE PutHotTransient, BEFORE the hot:chunk key
//     is written, carrying the chunk about to appear. At a boundary handoff this
//     is when the next chunk's key is created: the ingestion loop guarantees the
//     predecessor's write handle is already CLOSED (close-before-create-key), so
//     a test can assert that at the one instant the partition moves.
type crashHooks struct {
	beforeKeyDelete    func()
	beforeUnlink       func()
	failCommitBatch    func() bool //nolint:unused // fired from a later layer (recovery/CommitIndex)
	afterMarkFreezing  func()      //nolint:unused // fired from a later layer (processChunk)
	beforeHotTransient func(chunkID chunk.ID)
}

func (h crashHooks) fireBeforeKeyDelete() {
	if h.beforeKeyDelete != nil {
		h.beforeKeyDelete()
	}
}

func (h crashHooks) fireBeforeUnlink() {
	if h.beforeUnlink != nil {
		h.beforeUnlink()
	}
}

//nolint:unused // called from a later layer (catalog_protocol/recovery)
func (h crashHooks) commitBatchShouldFail() bool {
	return h.failCommitBatch != nil && h.failCommitBatch()
}

//nolint:unused // called from a later layer (processChunk)
func (h crashHooks) fireAfterMarkFreezing() {
	if h.afterMarkFreezing != nil {
		h.afterMarkFreezing()
	}
}

func (h crashHooks) fireBeforeHotTransient(chunkID chunk.ID) {
	if h.beforeHotTransient != nil {
		h.beforeHotTransient(chunkID)
	}
}
