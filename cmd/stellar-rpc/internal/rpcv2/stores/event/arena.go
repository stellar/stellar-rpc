package event

// byteArena hands out stable copies of transient byte slices, carved from
// larger chunks so a fetch copying hundreds of payloads costs a few
// allocations rather than one each. A chunk is only appended within its
// capacity, so returned copies never move. The zero value is ready.
//
// Not safe for concurrent use.
type byteArena struct {
	buf []byte
}

// The first chunk is small so a one-event page does not pay 64 KiB; each
// later chunk doubles up to arenaChunkSize.
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
