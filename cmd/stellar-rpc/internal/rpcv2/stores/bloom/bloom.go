// Package bloom holds the run-routing bloom filter BOTH hot engines probe
// before they touch a sealed run file (stores/event's term runs,
// stores/txhash's packed-row runs). Geometry: ~10 bits/key, 7 probes
// double-hashed from a 64-bit fingerprint, bit count rounded up to a power of
// two so indexing is a mask. One []uint64 and no pointers — GC-invisible.
//
// The filter is key-agnostic: callers hash their own key (events xxhash the
// 16-byte term, txhash the 32-byte hash) and pass the fingerprint, so nothing
// here has to know either engine's key shape.
//
// Filters are RAM-only — never serialized, so bits/key and probe count carry
// no format compatibility. They are, however, a live RAM anchor: run blooms
// measured 241MB against the 120MB budgeted at stress-density term
// cardinality, and displaced generations' filters are freed only at chunk
// Close (design-docs/full-history-campaign-handoff.md, "RAM anchor"). Tune the
// geometry here, once, for both engines.
package bloom

// probes is how many bits one key sets and one probe tests.
const probes = 7

// Filter is a plain double-hashed bloom over 64-bit fingerprints. A copy
// aliases the original's bits (both see every later Add), and concurrent Add
// is unsynchronized — the engines build a filter on one goroutine and only
// read it once its run is published.
type Filter struct {
	bits []uint64
	mask uint64
}

// New sizes a filter for n keys at ~10 bits/key, rounded up to a power of two
// (mask-indexable). n must be at least 1.
func New(n int) Filter {
	bits := 1
	for bits < n*10 {
		bits <<= 1
	}
	return Filter{bits: make([]uint64, bits/64+1), mask: uint64(bits - 1)} //nolint:gosec // bits >= 1
}

// Add records the fingerprint fp.
func (b *Filter) Add(fp uint64) {
	h2 := fp>>33 | fp<<31 | 1 // odd second hash
	for i := range probes {
		bit := (fp + uint64(i)*h2) & b.mask
		b.bits[bit/64] |= 1 << (bit % 64)
	}
}

// MayContain reports whether fp may have been added: false is definitive, true
// carries the false-positive rate the sizing bought.
func (b *Filter) MayContain(fp uint64) bool {
	h2 := fp>>33 | fp<<31 | 1
	for i := range probes {
		bit := (fp + uint64(i)*h2) & b.mask
		if b.bits[bit/64]&(1<<(bit%64)) == 0 {
			return false
		}
	}
	return true
}
