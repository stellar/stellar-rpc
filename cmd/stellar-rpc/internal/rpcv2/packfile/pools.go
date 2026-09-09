package packfile

// Open-path allocation pools. The v2 read path opens cold packfiles per
// request, and every open allocates the decoded offset index, its FOR-decode
// scratch and the open-time read buffers. These pools recycle the backing
// memory only: contents are always fully rewritten before use, and every
// pooled buffer is either dead before its function returns or reader-private
// until Close hands it back.
//
// Puts are capacity-capped so one pathological file cannot pin an arbitrarily
// large array in a pool slot; larger buffers fall to the garbage collector.
// Every cap must therefore sit above what a legitimate packfile needs, because
// crossing one is silent: the Put is skipped, the pool drains, and every open
// allocates afresh — still correct, but without the allocation win these pools
// exist for.

import "sync"

// maxPooledOffsets caps the decoded offset table (recordCount+1 int64s, so an
// 8 MiB array at the cap) a Put may retain. What it has to exceed is fixed by
// chunk geometry, which this package cannot reach — packfile is a container
// format with no domain dependencies — so the bound is a constant carrying
// headroom rather than a derived one:
//
//   - the ledger cold pack stores one ledger per record, so its table is
//     chunk.LedgersPerChunk+1 entries: 10,001;
//   - events.pack and index.pack store 128 items per record, so 1<<20 entries
//     covers ~134M events, or ~134M distinct index terms, within a single
//     chunk — against the ~600K terms a production chunk carries today.
//
// Raise it if chunk geometry grows past that; see the file comment for what
// happens silently if it isn't raised.
const maxPooledOffsets = 1 << 20 // entries (8 MiB backing array)

const (
	// maxPooledScratch tracks maxPooledOffsets: the FOR-decode scratch is
	// recordCount uint32s to the offset table's recordCount+1 int64s.
	maxPooledScratch = 1 << 20 // entries (4 MiB backing array)
	maxPooledOpenBuf = 4 << 20 // bytes
)

//nolint:gochecknoglobals // process-wide pools, like recordWorkspacePool
var (
	offsetsPool sync.Pool // *[]int64
	scratchPool sync.Pool // *[]uint32
	openBufPool sync.Pool // *[]byte
)

// A size miss hands the pooled buffer back before allocating: Get has already
// removed it from the pool, so returning it is the only thing that keeps a run
// of growing opens from draining the pool one buffer per open.

func getOffsets(n int) []int64 {
	if p, _ := offsetsPool.Get().(*[]int64); p != nil {
		if cap(*p) >= n {
			return (*p)[:n]
		}
		putOffsets(*p)
	}
	return make([]int64, n)
}

// putOffsets recycles a decoded offset table. The caller must guarantee no
// live reference remains; see Reader.Close for the in-flight handshake.
func putOffsets(s []int64) {
	if cap(s) == 0 || cap(s) > maxPooledOffsets {
		return
	}
	s = s[:0]
	offsetsPool.Put(&s)
}

func getScratch(n int) []uint32 {
	if p, _ := scratchPool.Get().(*[]uint32); p != nil {
		if cap(*p) >= n {
			return (*p)[:n]
		}
		putScratch(*p)
	}
	return make([]uint32, n)
}

func putScratch(s []uint32) {
	if cap(s) == 0 || cap(s) > maxPooledScratch {
		return
	}
	s = s[:0]
	scratchPool.Put(&s)
}

func getOpenBuf(n int) []byte {
	if p, _ := openBufPool.Get().(*[]byte); p != nil {
		if cap(*p) >= n {
			return (*p)[:n]
		}
		putOpenBuf(*p)
	}
	return make([]byte, n)
}

func putOpenBuf(s []byte) {
	if cap(s) == 0 || cap(s) > maxPooledOpenBuf {
		return
	}
	s = s[:0]
	openBufPool.Put(&s)
}
