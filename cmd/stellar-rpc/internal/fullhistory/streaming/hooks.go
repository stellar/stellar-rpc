package streaming

// crashHooks are test-only fault-injection points interposed at the
// load-bearing instants of the one-write protocol and the sweeps. In
// production every field is nil and every call site is a no-op (one nil-check
// per protected step).
//
// They exist because the crash-safety invariants are properties of the ORDER
// of operations inside the real catalog methods (catalog_sweep.go,
// catalog_protocol.go): a hand-inlined sweep can stay green after the
// production order breaks, but a hook fired from INSIDE the real method cannot.
// Each hook observes durable state between two steps so the test can assert the
// invariant that the step ORDER guarantees:
//
//   - beforeKeyDelete fires after unlink+fsync, before the key delete. Asserts
//     file-gone-implies-key-present: reordering the delete ahead of the unlink
//     would leave the file on disk here.
//   - beforeUnlink fires after the frozen->pruning demote, before the unlink.
//     Asserts never-unlink-under-a-frozen-key: the value must already be
//     "pruning"; a dropped demote would leave it "frozen".
//   - failCommitBatch, returning true, forces CommitIndex's batch callback to
//     error so the batch is dropped wholesale. Asserts all-or-nothing: nothing
//     the batch would have written may be observable.
//
// The primitives layer (processChunk, buildTxhashIndex) adds its own hooks to
// this struct.
type crashHooks struct {
	beforeKeyDelete func()
	beforeUnlink    func()
	failCommitBatch func() bool
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

func (h crashHooks) commitBatchShouldFail() bool {
	return h.failCommitBatch != nil && h.failCommitBatch()
}
