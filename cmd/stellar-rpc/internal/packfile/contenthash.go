package packfile

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
)

// ErrContentHashMismatch is returned when a file's content hash does not match
// the hash stored in the trailer.
var ErrContentHashMismatch = errors.New("packfile: content hash mismatch")

// contentHasher computes a chunked SHA-256 content hash over a stream of items.
// Items are length-prefixed and grouped into fixed-size chunks; chunk digests
// are fed into a final hasher. chunkSize is typically the record size.
//
//	chunkDigest_i = SHA-256([4B len][item_{i*K}] ... [4B len][item_{i*K+K-1}])
//	finalHash     = SHA-256(chunkDigest_0 || ... || chunkDigest_M)
//
// Both hashers are streaming, so memory usage is O(1) regardless of item count.
type contentHasher struct {
	chunk     hash.Hash // per-chunk hasher; reset at each chunk boundary
	final     hash.Hash // running hasher over chunk digests
	count     int
	chunkSize int
	lenBuf    [4]byte
}

// newContentHasher creates a contentHasher with the given chunk size.
// Panics if chunkSize <= 0.
func newContentHasher(chunkSize int) *contentHasher {
	if chunkSize <= 0 {
		panic(fmt.Sprintf("packfile: newContentHasher chunkSize must be > 0, got %d", chunkSize))
	}
	return &contentHasher{
		chunk:     sha256.New(),
		final:     sha256.New(),
		chunkSize: chunkSize,
	}
}

// Add appends one logical item. Parts are concatenated under a single length prefix.
func (h *contentHasher) Add(parts ...[]byte) {
	total := 0
	for _, p := range parts {
		total += len(p)
	}
	binary.LittleEndian.PutUint32(h.lenBuf[:], uint32(total))
	h.chunk.Write(h.lenBuf[:])
	for _, p := range parts {
		h.chunk.Write(p)
	}
	h.count++
	if h.count == h.chunkSize {
		h.flushChunk()
	}
}

// Sum flushes any partial chunk and returns the final hash.
// After calling Sum, the hasher must not be reused (no further Add calls).
func (h *contentHasher) Sum() [32]byte {
	if h.count > 0 {
		h.flushChunk()
	}
	var out [32]byte
	h.final.Sum(out[:0])
	return out
}

// flushChunk computes the current chunk's digest, feeds it into the final
// hasher, and resets the chunk hasher for the next chunk.
func (h *contentHasher) flushChunk() {
	var digest [sha256.Size]byte
	h.chunk.Sum(digest[:0])
	h.final.Write(digest[:])
	h.chunk.Reset()
	h.count = 0
}
