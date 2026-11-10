package preflight

/*
#include "../../lib/preflight.h"
*/
import "C"

// memoryLimitExitCode is the exit code of a worker that passed its memory
// limit. A worker exits with 0 or 1 itself, a Go crash exits with 2 and the
// race detector with 66.
const memoryLimitExitCode = 3

// limitMemory starts counting the heap the Rust libraries hold in this
// process, and limits it: to bytes from now on, or when bytes is 0, to 8 times
// the per-transaction memory limit of the ledger each simulation reads. An
// allocation past the limit ends the process with memoryLimitExitCode, so
// only a worker calls it, before it allocates anything through Rust.
func limitMemory(bytes uint64) {
	C.preflight_limit_memory(C.uint64_t(bytes), C.int(memoryLimitExitCode))
}

// memoryLimit is the heap limit in force, 0 when there is none.
func memoryLimit() uint64 {
	return uint64(C.preflight_memory_limit())
}

// resetMemoryPeak restarts the measurement memoryPeak returns.
func resetMemoryPeak() {
	C.preflight_reset_memory_peak()
}

// memoryPeak is the most heap the Rust libraries held since resetMemoryPeak,
// 0 in a process that does not count it.
func memoryPeak() uint64 {
	return uint64(C.preflight_memory_peak())
}
