package bloom

import (
	"math/bits"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testRNG(seed uint64) *rand.Rand {
	return rand.New(rand.NewPCG(seed, seed*7919+1))
}

// TestFilter_NoFalseNegatives pins the property every run lookup rests on: a
// negative MayContain skips the run's file entirely, so an added fingerprint
// must NEVER answer false.
func TestFilter_NoFalseNegatives(t *testing.T) {
	for _, n := range []int{1, 2, 63, 64, 65, 1000, 100_000} {
		rng := testRNG(uint64(n))
		fps := make([]uint64, n)
		f := New(n)
		for i := range fps {
			fps[i] = rng.Uint64()
			f.Add(fps[i])
		}
		for i, fp := range fps {
			require.Truef(t, f.MayContain(fp), "n=%d key %d", n, i)
		}
	}
}

// TestNew_SizesPowerOfTwoAtTenBitsPerKey pins the sizing geometry: a
// mask-indexable power of two at or just above 10 bits/key, with a word array
// long enough for the highest reachable bit.
func TestNew_SizesPowerOfTwoAtTenBitsPerKey(t *testing.T) {
	for _, n := range []int{1, 7, 100, 1024, 3_000_000} {
		f := New(n)
		size := f.mask + 1
		assert.Equalf(t, 1, bits.OnesCount64(size), "n=%d: %d bits is not a power of two", n, size)
		assert.GreaterOrEqualf(t, size, uint64(10*n), "n=%d: fewer than 10 bits/key", n)
		assert.Lessf(t, size, uint64(20*n), "n=%d: more than 20 bits/key", n)
		assert.GreaterOrEqualf(t, uint64(len(f.bits)), size/64, "n=%d: word array too short", n)
	}
}

// TestFilter_FalsePositiveRate guards the probe geometry against the sizing.
// The bound is deliberately loose — an honest re-tune of bits/key or probes
// should not have to touch it — but it catches a filter whose probes stop
// spreading: this shape measures 0.21% (420/200000), one probe measures 7.4%.
func TestFilter_FalsePositiveRate(t *testing.T) {
	const keys = 20_000
	rng := testRNG(42)
	f := New(keys)
	for range keys {
		f.Add(rng.Uint64())
	}
	positives := 0
	const probeCount = 200_000
	for range probeCount {
		// Absent with overwhelming probability: a 64-bit fingerprint drawn
		// from the same stream collides with an added one at ~2^-44 per pair.
		if f.MayContain(rng.Uint64()) {
			positives++
		}
	}
	assert.Lessf(t, positives, probeCount/50, "false-positive rate above 2%%: %d/%d", positives, probeCount)
}
