package streaming

import "github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"

// crashHooks are test-only fault-injection points interposed at the
// load-bearing instants of the one-write protocol and the sweeps. In
// production every field is nil and every call site is a no-op, so the hooks
// add one nil-check per protected step and nothing else.
//
// They exist because the crash-safety invariants are properties of the ORDER
// of operations inside the real catalog methods (sweep.go, protocol.go), not
// of a test that hand-replays those steps. A hand-inlined sweep can stay green
// even after the production order is broken; a hook fired from INSIDE the real
// method cannot. Each hook observes durable state at the exact instant between
// two steps and lets the test assert the invariant that the step ORDER is
// meant to guarantee:
//
//   - beforeKeyDelete fires AFTER the unlink+fsync and BEFORE the key delete.
//     Asserts file-gone-implies-key-present: if the key delete were reordered
//     ahead of the unlink, the file would still be on disk here.
//   - beforeUnlink fires AFTER the frozen->pruning demote and BEFORE the
//     unlink. Asserts never-unlink-under-a-frozen-key: the value must already
//     be "pruning"; if the demote were dropped, it would still be "frozen".
//   - failCommitBatch, when it returns true, forces a recovery batch callback to
//     return an error so the batch is dropped wholesale. Asserts all-or-nothing:
//     nothing the batch would have written may be observable.
//   - afterMarkFreezing fires INSIDE processChunk, AFTER MarkChunkFreezing has
//     put every requested kind's key to "freezing" and BEFORE any file I/O.
//     Asserts mark-then-write: at this instant every requested kind reads
//     "freezing" and no artifact file exists yet. Dropping the mark (or
//     reordering the write ahead of it) would leave the keys absent (or a file
//     on disk) here — defeating "every file on disk is reachable from a key"
//     and crash detectability.
//   - beforeHotTransient fires INSIDE PutHotTransient, BEFORE the hot:chunk key
//     is written "transient", carrying the chunk whose key is about to appear.
//     At a boundary handoff this is the exact instant the next chunk's key is
//     created: the ingestion loop guarantees the just-completed chunk's write
//     handle is already CLOSED here (close-before-create-key), so a test can
//     assert the closed-ness of the predecessor's DB at the one instant the
//     partition moves. Dropping the close-before-open order would leave the
//     predecessor's DB open under a live writer here.
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
