package event

// byteArena hands out stable copies of transient byte slices from large
// chunked allocations, so a fetch that copies hundreds of small payloads
// costs a handful of allocations instead of one per payload. Chunks are
// only ever appended within capacity, so previously returned copies never
// move. Zero value is ready.
//
// NOT safe for concurrent use: copy appends to one buffer. A caller whose
// copies come from several goroutines — ColdReader.FetchEvents, once its
// packfile reads fan out — must serialize them.
type byteArena struct {
	buf []byte
}

// arenaChunkSize is the arena's allocation unit. Big enough that a
// 512-candidate fetch of ~250B payloads fits in one or two chunks, small
// enough that a mostly-idle arena wastes little.
const arenaChunkSize = 64 << 10

func (a *byteArena) copy(b []byte) []byte {
	if len(b) > cap(a.buf)-len(a.buf) {
		a.buf = make([]byte, 0, max(arenaChunkSize, len(b)))
	}
	n := len(a.buf)
	a.buf = append(a.buf, b...)
	return a.buf[n : n+len(b) : n+len(b)]
}
