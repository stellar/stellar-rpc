package packfile

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

// contentHasher computes a chunked SHA-256 content hash over a stream of items.
// Items are length-prefixed and grouped into fixed-size chunks; chunk digests
// are aggregated into a final hash. chunkSize is typically itemsPerRecord.
//
//	chunkDigest_i = SHA-256([4B len][item_{i*K}] ... [4B len][item_{i*K+K-1}])
//	finalHash     = SHA-256(chunkDigest_0 || ... || chunkDigest_M)
type contentHasher struct {
	digests   []byte
	buf       []byte
	count     int
	chunkSize int
}

// newContentHasher creates a contentHasher with the given chunk size.
// Panics if chunkSize <= 0.
func newContentHasher(chunkSize int) *contentHasher {
	if chunkSize <= 0 {
		panic(fmt.Sprintf("packfile: newContentHasher chunkSize must be > 0, got %d", chunkSize))
	}
	return &contentHasher{chunkSize: chunkSize}
}

// Add appends one logical item. Parts are concatenated under a single length prefix.
func (h *contentHasher) Add(parts ...[]byte) {
	total := 0
	for _, p := range parts {
		total += len(p)
	}
	h.buf = binary.LittleEndian.AppendUint32(h.buf, uint32(total))
	for _, p := range parts {
		h.buf = append(h.buf, p...)
	}
	h.count++
	if h.count == h.chunkSize {
		digest := sha256.Sum256(h.buf)
		h.digests = append(h.digests, digest[:]...)
		h.buf = h.buf[:0]
		h.count = 0
	}
}

// Sum flushes any partial chunk and returns the final hash.
// After calling Sum the hasher must not be reused.
func (h *contentHasher) Sum() [32]byte {
	if h.count > 0 {
		digest := sha256.Sum256(h.buf)
		h.digests = append(h.digests, digest[:]...)
		h.buf = h.buf[:0]
		h.count = 0
	}
	return sha256.Sum256(h.digests)
}
