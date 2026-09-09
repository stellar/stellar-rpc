package event

// roaring_contract_test.go pins the library-level property the event index is
// built on: the aggregation entry points this package calls with shared
// bitmaps must treat those bitmaps as read-only.
//
// ConcurrentBitmaps.Get and denseState.snapshot publish one bitmap to every
// concurrent reader at once and never mutate it afterwards. Readers therefore
// hand the same *roaring.Bitmap to roaring.FastAnd, roaring.FastOr and
// Bitmap.AndAny from many goroutines at once. That is sound only while roaring
// writes exclusively through the receiver (And, AndAny) or into a freshly
// allocated answer (FastAnd, FastOr) — a property roaring documents by
// implication and this file pins by observation.
//
// The pin is on the dependency, not on any caller: it is written against
// roaring alone, so it holds however the match path is built, and it is the
// first thing to run on a roaring version bump. A failure here means the new
// version is unsafe to take, not that a caller is wrong.
//
// Pinned version: github.com/RoaringBitmap/roaring/v2 v2.26.0.

import (
	"math/rand"
	"sync"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/stretchr/testify/require"
)

// sharedBitmaps returns bitmaps in the shape the index publishes: a
// writer-private bitmap marked copy-on-write, and the Clone of it that
// denseState.snapshot hands to readers. The clone shares its containers with
// the writer's copy, so a write reaching either is visible in the other, which
// is what makes an unnoticed mutation here a live corruption bug rather than a
// style violation.
//
// The set spans all three container kinds and several high keys, because the
// aggregation paths branch on container type and on key advance: only a mixed
// corpus reaches all of them.
func sharedBitmaps(t *testing.T) []*roaring.Bitmap {
	t.Helper()
	rng := rand.New(rand.NewSource(20260909))

	// Array container: a few hundred scattered ids inside one high key.
	sparse := roaring.New()
	for range 400 {
		sparse.Add(uint32(rng.Intn(1 << 16)))
	}
	// Bitmap container: dense enough that roaring stores it as a bitset.
	dense := roaring.New()
	for range 40_000 {
		dense.Add(uint32(rng.Intn(1 << 16)))
	}
	// Run container: contiguous stretches, RunOptimize'd so the container
	// really is a run and not an array or a bitmap.
	runs := roaring.New()
	for i := range uint32(50) {
		runs.AddRange(uint64(i*1000), uint64(i*1000+600))
	}
	runs.RunOptimize()
	// Containers under several high keys, so the aggregation's key-advance
	// paths run too.
	wide := roaring.New()
	for key := range uint32(4) {
		for i := range uint32(5000) {
			wide.Add(key<<16 + i*7)
		}
	}

	out := make([]*roaring.Bitmap, 0, 4)
	for _, wbm := range []*roaring.Bitmap{sparse, dense, runs, wide} {
		wbm.SetCopyOnWrite(true)
		snap := wbm.Clone()
		require.True(t, snap.GetCopyOnWrite(),
			"fixture: the published snapshot must carry the copy-on-write mark")
		out = append(out, snap)
	}
	return out
}

func bitmapBytes(t *testing.T, bm *roaring.Bitmap) []byte {
	t.Helper()
	b, err := bm.ToBytes()
	require.NoError(t, err)
	return b
}

func bitmapImages(t *testing.T, bms []*roaring.Bitmap) [][]byte {
	t.Helper()
	out := make([][]byte, len(bms))
	for i, bm := range bms {
		out[i] = bitmapBytes(t, bm)
	}
	return out
}

func requireUnchanged(t *testing.T, bms []*roaring.Bitmap, before [][]byte, op string) {
	t.Helper()
	for i, bm := range bms {
		require.Equal(t, before[i], bitmapBytes(t, bm),
			"%s mutated shared argument %d: roaring can no longer be handed "+
				"denseState.snapshot bitmaps at this version", op, i)
	}
}

// freshRange is the receiver shape the match path hands to AndAny: a bitmap
// this call allocated, holding one contiguous id range, shared with nobody.
func freshRange(lo, hi uint64) *roaring.Bitmap {
	acc := roaring.New()
	acc.AddRange(lo, hi)
	return acc
}

// TestRoaringContract_AggregationDoesNotMutateInputs is the core pin: every
// aggregation this package calls with shared bitmaps leaves those bitmaps
// byte-identical.
func TestRoaringContract_AggregationDoesNotMutateInputs(t *testing.T) {
	t.Parallel()

	// Ranges chosen to hit the shapes that differ inside roaring: a whole
	// high key (AddRange produces a full run container, whose iand takes the
	// clone-the-other-side branch), a partial one, one the inputs do not
	// reach at all, and a span crossing several.
	ranges := []struct {
		name   string
		lo, hi uint64
	}{
		{"whole key", 0, 1 << 16},
		{"partial key", 1000, 40_000},
		{"key boundary", 65_000, 66_000},
		{"several keys", 0, 4 << 16},
		{"disjoint key", 40 << 16, 41 << 16},
	}

	for _, w := range ranges {
		t.Run("AndAny/"+w.name, func(t *testing.T) {
			t.Parallel()
			for n := 1; n <= 4; n++ {
				shared := sharedBitmaps(t)[:n]
				before := bitmapImages(t, shared)
				acc := freshRange(w.lo, w.hi)
				acc.AndAny(shared...)
				requireUnchanged(t, shared, before, "AndAny")

				// The accumulator must equal the definition AndAny documents,
				// so a silently wrong answer cannot pass as an unmutated one.
				// The leading empty bitmap keeps FastOr off its single-input
				// Clone shortcut, whose result would share containers with a
				// copy-on-write argument.
				want := roaring.FastOr(append([]*roaring.Bitmap{roaring.New()}, shared...)...)
				want.And(freshRange(w.lo, w.hi))
				require.Equal(t, bitmapBytes(t, want), bitmapBytes(t, acc),
					"AndAny must equal x.And(FastOr(args)) for %d args", n)
			}
		})
	}

	t.Run("FastAnd", func(t *testing.T) {
		t.Parallel()
		for n := 2; n <= 4; n++ {
			shared := sharedBitmaps(t)[:n]
			before := bitmapImages(t, shared)
			require.NotNil(t, roaring.FastAnd(shared...))
			requireUnchanged(t, shared, before, "FastAnd")
		}
	})

	t.Run("FastOr", func(t *testing.T) {
		t.Parallel()
		for n := 2; n <= 4; n++ {
			shared := sharedBitmaps(t)[:n]
			before := bitmapImages(t, shared)
			require.NotNil(t, roaring.FastOr(shared...))
			requireUnchanged(t, shared, before, "FastOr")
		}
	})

	// AndAny delegates a single argument to And, so And carries the same
	// obligation and is pinned separately.
	t.Run("And", func(t *testing.T) {
		t.Parallel()
		for _, shared := range sharedBitmaps(t) {
			before := bitmapBytes(t, shared)
			acc := freshRange(0, 1<<16)
			acc.And(shared)
			require.Equal(t, before, bitmapBytes(t, shared),
				"Bitmap.And mutated its argument")
		}
	})
}

// TestRoaringContract_AndAnyDoesNotRetainArguments pins the other half of the
// contract: AndAny copies out of its arguments rather than aliasing their
// storage into the receiver. A caller that hands it a reusable scratch bitmap
// depends on the answer surviving that scratch's next Clear.
func TestRoaringContract_AndAnyDoesNotRetainArguments(t *testing.T) {
	t.Parallel()

	for _, shared := range sharedBitmaps(t) {
		for _, nArgs := range []int{1, 2} {
			scratch := roaring.New()
			for i := range uint32(3000) {
				scratch.Add(i * 3)
			}
			acc := freshRange(0, 1<<16)
			if nArgs == 1 {
				acc.AndAny(scratch)
			} else {
				acc.AndAny(shared, scratch)
			}
			answer := bitmapBytes(t, acc)

			scratch.Clear()
			require.Equal(t, answer, bitmapBytes(t, acc),
				"AndAny with %d args aliased a scratch argument's storage into "+
					"the receiver: reusing a scratch bitmap is unsafe", nArgs)
		}
	}
}

// TestRoaringContract_ConcurrentReadersShareArguments is the race-detector
// gate. Eight goroutines aggregate over the same shared, copy-on-write-marked
// bitmaps at once, the way concurrent getEvents requests do against one
// denseState snapshot. Under -race any write reaching a shared bitmap fails
// the run; without it, the byte-identity check and the agreement between
// goroutines still catch a mutation.
func TestRoaringContract_ConcurrentReadersShareArguments(t *testing.T) {
	t.Parallel()

	const goroutines = 8
	const rounds = 32

	shared := sharedBitmaps(t)
	before := bitmapImages(t, shared)

	// The single-threaded answer every goroutine must reproduce.
	seq := freshRange(0, 4<<16)
	seq.AndAny(shared...)
	wantAndAny := bitmapBytes(t, seq)
	wantFastAnd := bitmapBytes(t, roaring.FastAnd(shared...))
	wantFastOr := bitmapBytes(t, roaring.FastOr(shared...))

	results := make([][3][]byte, goroutines)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for g := range goroutines {
		wg.Go(func() {
			<-start
			var last [3][]byte
			for range rounds {
				acc := freshRange(0, 4<<16)
				acc.AndAny(shared...)
				b, err := acc.ToBytes()
				if err != nil {
					panic(err)
				}
				last[0] = b
				if b, err = roaring.FastAnd(shared...).ToBytes(); err != nil {
					panic(err)
				}
				last[1] = b
				if b, err = roaring.FastOr(shared...).ToBytes(); err != nil {
					panic(err)
				}
				last[2] = b
			}
			results[g] = last
		})
	}
	close(start)
	wg.Wait()

	requireUnchanged(t, shared, before, "concurrent aggregation")
	for g := range goroutines {
		require.Equal(t, wantAndAny, results[g][0], "goroutine %d disagreed on AndAny", g)
		require.Equal(t, wantFastAnd, results[g][1], "goroutine %d disagreed on FastAnd", g)
		require.Equal(t, wantFastOr, results[g][2], "goroutine %d disagreed on FastOr", g)
	}
}
