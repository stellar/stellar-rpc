package packfile

import (
	"crypto/sha256"
	"encoding/binary"
	"hash"
)

// writeLenPrefixed writes [4-byte little-endian length][item bytes] into h.
// This is the wire format for one item in a chunked content hash; both the
// writer's hashGoroutine and the reader's contentHasher emit identical bytes
// via this helper, so the format lives in exactly one place.
func writeLenPrefixed(h hash.Hash, item []byte) {
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(item))) //nolint:gosec // caller bounds-checks
	_, _ = h.Write(lenBuf[:])
	_, _ = h.Write(item)
}

// contentHasher computes the chunked SHA-256 content hash, one chunk per
// record:
//
//	digest_r = SHA-256([4B len][item] ... for each item of record r)
//	final    = SHA-256(digest_0 || digest_1 || ...)
//
// Items stream through Add, flushChunk ends a record's chunk, and Sum produces
// the final hash. Matches the writer's per-record digest scheme so the
// reader's Verify can replay it.
type contentHasher struct {
	chunk hash.Hash // current chunk's SHA-256 (length-prefixed items)
	final hash.Hash // SHA-256 over chunk digests, ready for Sum
}

func newContentHasher() *contentHasher {
	return &contentHasher{chunk: sha256.New(), final: sha256.New()}
}

// Add appends one logical item to the current chunk.
func (h *contentHasher) Add(item []byte) {
	writeLenPrefixed(h.chunk, item)
}

// Sum returns the final hash. After calling Sum the hasher must not be reused.
func (h *contentHasher) Sum() [sha256.Size]byte {
	var out [sha256.Size]byte
	h.final.Sum(out[:0])
	return out
}

// flushChunk finalizes the current chunk's digest, feeds it into the outer
// hasher, and resets chunk state for the next chunk.
func (h *contentHasher) flushChunk() {
	var d [sha256.Size]byte
	h.chunk.Sum(d[:0])
	_, _ = h.final.Write(d[:])
	h.chunk.Reset()
}
