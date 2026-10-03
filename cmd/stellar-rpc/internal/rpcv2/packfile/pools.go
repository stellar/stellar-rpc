package packfile

// Pools for the open path. Cold packfiles are opened per request, and every
// open decodes an offset table, with FOR-decode scratch, out of read buffers;
// these pools recycle that memory. Contents are fully rewritten before use,
// and a pooled buffer is either dead before its function returns or
// reader-private until Close returns it.
//
// Puts are capped so one pathological file cannot pin a huge array. A cap
// must sit above what a legitimate packfile needs: crossing it skips the Put,
// the pool drains, and every open allocates afresh. capSkips counts those
// skips so the drain is visible.

import (
	"sync"
	"sync/atomic"
)

// maxPooledOffsets caps the decoded offset table (recordCount+1 int64s) a Put
// may retain. Chunk geometry, which this package cannot import, sets what it
// must exceed: the ledger pack holds one ledger per record, 10,001 entries;
// events.pack and index.pack hold 128 items per record, so 1<<20 entries
// covers about 134M events or index terms per chunk, against roughly 600K
// terms in a production chunk today. Raise it if the geometry grows.
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

// capSkips counts Puts dropped for exceeding a cap, across all three pools.
// A count that climbs with the open rate means the pools have stopped
// recycling. Exported through PoolCapSkips.
//
//nolint:gochecknoglobals // one tally across process-wide pools; read-only outside this file
var capSkips atomic.Uint64

// PoolCapSkips returns the process-wide count of pooled buffers dropped for
// exceeding a capacity cap. Zero is the only healthy value; a climbing count
// means a cap wants raising.
func PoolCapSkips() uint64 { return capSkips.Load() }

// On a size miss the pooled buffer is returned before a larger one is
// allocated: Get already removed it, and returning it is what keeps a run of
// growing opens from draining the pool. It is under its cap, so it never
// counts as a skip.

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
	if cap(s) == 0 {
		return
	}
	if cap(s) > maxPooledOffsets {
		capSkips.Add(1)
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
	if cap(s) == 0 {
		return
	}
	if cap(s) > maxPooledScratch {
		capSkips.Add(1)
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
	if cap(s) == 0 {
		return
	}
	if cap(s) > maxPooledOpenBuf {
		capSkips.Add(1)
		return
	}
	s = s[:0]
	openBufPool.Put(&s)
}
