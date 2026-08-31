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

import "sync"

const (
	maxPooledOffsets = 1 << 20 // entries (8 MiB backing array)
	maxPooledScratch = 1 << 20 // entries (4 MiB backing array)
	maxPooledOpenBuf = 4 << 20 // bytes
)

//nolint:gochecknoglobals // process-wide pools, like recordWorkspacePool
var (
	offsetsPool sync.Pool // *[]int64
	scratchPool sync.Pool // *[]uint32
	openBufPool sync.Pool // *[]byte
)

func getOffsets(n int) []int64 {
	if p, _ := offsetsPool.Get().(*[]int64); p != nil && cap(*p) >= n {
		return (*p)[:n]
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
	if p, _ := scratchPool.Get().(*[]uint32); p != nil && cap(*p) >= n {
		return (*p)[:n]
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
	if p, _ := openBufPool.Get().(*[]byte); p != nil && cap(*p) >= n {
		return (*p)[:n]
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
