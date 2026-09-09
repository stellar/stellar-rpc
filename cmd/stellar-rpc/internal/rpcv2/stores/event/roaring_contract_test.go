package event

// roaring_contract_test.go pins the roaring properties the event index rests
// on. ConcurrentBitmaps.Get publishes one bitmap to every concurrent reader
// and never mutates it, and the match path hands those bitmaps to AndAny,
// NextValue and PreviousValue from many goroutines at once. That is sound
// only while AndAny writes nothing but its receiver and shares no storage
// with its arguments, and while the searches are read-only, inclusive of the
// target, and return -1 for none. The tests are written against roaring
// alone, so they are the first thing to run on a version bump.
//
// Pinned version: github.com/RoaringBitmap/roaring/v2 v2.26.0.

import (
	"math/rand"
	"slices"
	"sync"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/stretchr/testify/require"
)

// sharedBitmaps returns bitmaps in the shape the index publishes: the
// copy-on-write Clone that denseState.snapshot hands to readers, sharing its
// containers with the writer's bitmap. The set spans all three container
// kinds and several high keys, since the roaring paths branch on both.
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

// TestRoaringContract_AggregationDoesNotMutateInputs pins that AndAny, and
// And, which AndAny delegates a single argument to, leave shared arguments
// byte-identical.
func TestRoaringContract_AggregationDoesNotMutateInputs(t *testing.T) {
	t.Parallel()

	// Ranges that take different paths inside roaring: a whole high key, a
	// partial one, one the inputs do not reach, and a span across several.
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

// TestRoaringContract_AndAnyDoesNotRetainArguments pins that AndAny copies
// out of its arguments rather than aliasing their storage into the receiver:
// the slab walk goes on to write the receiver, with further AndAny groups and
// the in-place union across filters.
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
				"AndAny with %d args aliased an argument's storage into the receiver", nArgs)
		}
	}
}

// TestRoaringContract_ConcurrentReaders is the race-detector gate: eight
// goroutines run AndAny, NextValue and PreviousValue over the same shared
// copy-on-write snapshots at once, as concurrent queries do. Under -race any
// write to a shared bitmap fails the run; without it, the byte-identity check
// and the agreement between goroutines still catch one.
func TestRoaringContract_ConcurrentReaders(t *testing.T) {
	t.Parallel()

	const goroutines = 8
	const rounds = 32
	targets := []uint32{0, 1, 1 << 15, 1 << 16, 1<<16 + 1, 3 << 16, 1<<20 - 1}

	shared := sharedBitmaps(t)
	before := bitmapImages(t, shared)

	// The single-threaded answers every goroutine must reproduce.
	seq := freshRange(0, 4<<16)
	seq.AndAny(shared...)
	wantAndAny := bitmapBytes(t, seq)
	wantSearch := valueSearches(shared, targets)

	gotAndAny := make([][]byte, goroutines)
	gotSearch := make([][]int64, goroutines)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for g := range goroutines {
		wg.Go(func() {
			<-start
			for range rounds {
				acc := freshRange(0, 4<<16)
				acc.AndAny(shared...)
				b, err := acc.ToBytes()
				if err != nil {
					panic(err)
				}
				gotAndAny[g] = b
				gotSearch[g] = valueSearches(shared, targets)
			}
		})
	}
	close(start)
	wg.Wait()

	requireUnchanged(t, shared, before, "concurrent reads")
	for g := range goroutines {
		require.Equal(t, wantAndAny, gotAndAny[g], "goroutine %d disagreed on AndAny", g)
		require.Equal(t, wantSearch, gotSearch[g], "goroutine %d disagreed on the value searches", g)
	}
}

// valueSearches runs NextValue and PreviousValue for every target on every
// bitmap.
func valueSearches(bms []*roaring.Bitmap, targets []uint32) []int64 {
	out := make([]int64, 0, 2*len(bms)*len(targets))
	for _, bm := range bms {
		for _, target := range targets {
			out = append(out, bm.NextValue(target), bm.PreviousValue(target))
		}
	}
	return out
}

// TestRoaringContract_ValueSearchIsInclusiveAndReadOnly pins NextValue and
// PreviousValue, which the slab walk proves its skips with: an answer past
// the target, or a -1 with ids still to come, would silently drop matches.
// The definition is checked against the bitmap's own ids.
func TestRoaringContract_ValueSearchIsInclusiveAndReadOnly(t *testing.T) {
	t.Parallel()

	for i, shared := range sharedBitmaps(t) {
		ids := shared.ToArray()
		require.NotEmpty(t, ids, "fixture: bitmap %d holds nothing", i)
		before := bitmapBytes(t, shared)

		for _, target := range searchTargets(ids) {
			// The definition, read off the ids: the first id at or after the
			// target, and the last id at or before it.
			at, _ := slices.BinarySearch(ids, target)
			wantNext, wantPrev := int64(-1), int64(-1)
			if at < len(ids) {
				wantNext = int64(ids[at])
			}
			if at < len(ids) && ids[at] == target {
				wantPrev = int64(target)
			} else if at > 0 {
				wantPrev = int64(ids[at-1])
			}

			require.Equal(t, wantNext, shared.NextValue(target),
				"bitmap %d: NextValue(%d) must be the first id at or above the target",
				i, target)
			require.Equal(t, wantPrev, shared.PreviousValue(target),
				"bitmap %d: PreviousValue(%d) must be the last id at or below the target",
				i, target)
		}

		require.Equal(t, before, bitmapBytes(t, shared),
			"bitmap %d: a value search mutated the bitmap it searched: the slab "+
				"walk cannot run them on denseState.snapshot bitmaps at this version", i)
	}

	empty := roaring.New()
	require.Equal(t, int64(-1), empty.NextValue(0),
		"an empty bitmap must report no next value")
	require.Equal(t, int64(-1), empty.PreviousValue(1<<20),
		"an empty bitmap must report no previous value")
}

// searchTargets returns each id, its neighbors, the container boundaries the
// ids span, and the ends of the uint32 range.
func searchTargets(ids []uint32) []uint32 {
	out := []uint32{0, 1<<32 - 1}
	for _, id := range ids {
		out = append(out, id)
		if id > 0 {
			out = append(out, id-1)
		}
		if id < 1<<32-1 {
			out = append(out, id+1)
		}
	}
	for key := range uint32(6) {
		out = append(out, key<<16)
		if key > 0 {
			out = append(out, key<<16-1)
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}
