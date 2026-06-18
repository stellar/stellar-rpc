package streaming

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
//   - failCommitBatch, when it returns true, forces CommitIndex's batch
//     callback to return an error so the batch is dropped wholesale. Asserts
//     all-or-nothing: nothing the batch would have written may be observable.
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
