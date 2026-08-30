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

// The arena's allocation unit ramps: the first chunk is small so a
// limit=1 page fetching a few hundred bytes does not pay 64 KiB for
// them, and each subsequent chunk doubles up to arenaChunkSize so a
// 512-candidate fetch of ~250B payloads still lands in a handful of
// allocations.
const (
	arenaFirstChunkSize = 4 << 10
	arenaChunkSize      = 64 << 10
)

func (a *byteArena) copy(b []byte) []byte {
	if len(b) > cap(a.buf)-len(a.buf) {
		next := arenaFirstChunkSize
		if c := 2 * cap(a.buf); c > next {
			next = min(c, arenaChunkSize)
		}
		a.buf = make([]byte, 0, max(next, len(b)))
	}
	n := len(a.buf)
	a.buf = append(a.buf, b...)
	return a.buf[n : n+len(b) : n+len(b)]
}
