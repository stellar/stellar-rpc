package event

// roaring_contract_test.go pins the roaring properties the event index rests
// on. ConcurrentBitmaps.Get publishes one bitmap to every concurrent reader
// and never mutates it, and the match path hands those bitmaps to FastAnd,
// NextValue and PreviousValue from many goroutines at once. That is sound
// only while FastAnd reads its arguments and returns containers that share
// no storage with them, and the searches are read-only, inclusive of the
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

// freshRange is the slab window the match path hands to FastAnd: one
// contiguous id range, in a bitmap the test may write.
func freshRange(lo, hi uint64) *roaring.Bitmap {
	bm := roaring.New()
	bm.AddRange(lo, hi)
	return bm
}

// scratchBitmap is a bitmap the test owns outright and may write: one array
// container of scattered ids.
func scratchBitmap() *roaring.Bitmap {
	scratch := roaring.New()
	for i := range uint32(3000) {
		scratch.Add(i * 3)
	}
	return scratch
}

// windowRanges are slab windows that take different paths inside roaring: a
// whole high key, a partial one, one across a key boundary, a span over
// several keys, one the inputs do not reach, and the four-id span that is the
// smallest range roaring keeps as a run.
var windowRanges = []struct {
	name   string
	lo, hi uint64
}{
	{"whole key", 0, 1 << 16},
	{"partial key", 1000, 40_000},
	{"key boundary", 65_000, 66_000},
	{"several keys", 0, 4 << 16},
	{"disjoint key", 40 << 16, 41 << 16},
	{"four ids", 8000, 8004},
}

// TestRoaringContract_AggregationDoesNotMutateInputs pins that FastAnd
// leaves shared arguments byte-identical and equals the pairwise And chain,
// for every non-empty subset of the shared bitmaps beside the window: each
// container kind meets the window alone, in pairs and all together, which
// is every kernel a plan of one to seven terms can reach.
func TestRoaringContract_AggregationDoesNotMutateInputs(t *testing.T) {
	t.Parallel()

	for _, w := range windowRanges {
		t.Run(w.name, func(t *testing.T) {
			t.Parallel()
			shared := sharedBitmaps(t)
			for pick := 1; pick < 1<<len(shared); pick++ {
				ops := []*roaring.Bitmap{freshRange(w.lo, w.hi)}
				for i, bm := range shared {
					if pick&(1<<i) != 0 {
						ops = append(ops, bm)
					}
				}
				before := bitmapImages(t, ops)
				res := roaring.FastAnd(ops...)
				requireUnchanged(t, ops, before, "FastAnd")

				want := freshRange(w.lo, w.hi)
				for _, s := range ops[1:] {
					want.And(s)
				}
				require.Equal(t, bitmapBytes(t, want), bitmapBytes(t, res),
					"FastAnd must equal the pairwise And chain for %d args", len(ops))
			}
		})
	}
}

// TestRoaringContract_FastAndReturnsFreshContainers pins that a FastAnd
// result shares no storage with its inputs, on the two-input path and the
// wider one, over a whole key and a partial one: the slab walk unions
// results in place across plans. The probes remove one id, a write every
// container kind applies in place; the snapshot is copy-on-write, so only
// the private inputs are written back.
func TestRoaringContract_FastAndReturnsFreshContainers(t *testing.T) {
	t.Parallel()

	for _, w := range windowRanges[:2] {
		shared := sharedBitmaps(t)
		for i, one := range shared {
			other := shared[(i+1)%len(shared)]
			for _, tail := range [][]*roaring.Bitmap{{}, {scratchBitmap()}, {other}} {
				window := freshRange(w.lo, w.hi)
				ops := append([]*roaring.Bitmap{window, one}, tail...)
				before := bitmapImages(t, ops)
				res := roaring.FastAnd(ops...)
				require.False(t, res.IsEmpty(),
					"fixture: FastAnd over %d inputs selects nothing in %q", len(ops), w.name)
				res.Remove(res.Minimum())
				requireUnchanged(t, ops, before, "writing a FastAnd result")

				res = roaring.FastAnd(ops...)
				answer := bitmapBytes(t, res)
				window.Remove(window.Minimum())
				for _, bm := range tail {
					if !bm.GetCopyOnWrite() {
						bm.Remove(bm.Minimum())
					}
				}
				require.Equal(t, answer, bitmapBytes(t, res),
					"FastAnd over %d inputs aliased an input's storage into its result", len(ops))
			}
		}
	}
}

// TestRoaringContract_RangeIsRunContainer pins that a slab window built with
// AddRange is stored as run containers, the shape whose intersections a
// count-first FastAnd sizes before allocating; the pinned FastAnd intersects
// pairwise, so that allocation pin waits for the roaring bump.
func TestRoaringContract_RangeIsRunContainer(t *testing.T) {
	t.Parallel()

	for _, w := range windowRanges {
		st := freshRange(w.lo, w.hi).Stats()
		require.Equal(t, st.Containers, st.RunContainers,
			"range %q is not stored as run containers", w.name)
	}
}

// TestRoaringContract_ConcurrentReaders is the race-detector gate: eight
// goroutines run FastAnd, NextValue and PreviousValue over the same shared
// copy-on-write snapshots at once, as concurrent queries do. Under -race any
// write to a shared bitmap fails the run; without it, the byte-identity
// check and the agreement between goroutines still catch one.
func TestRoaringContract_ConcurrentReaders(t *testing.T) {
	t.Parallel()

	const goroutines = 8
	const rounds = 32
	targets := []uint32{0, 1, 1 << 15, 1 << 16, 1<<16 + 1, 3 << 16, 1<<20 - 1}

	shared := sharedBitmaps(t)
	before := bitmapImages(t, shared)

	// The single-threaded answers every goroutine must reproduce.
	wantFastAnd := bitmapBytes(t,
		roaring.FastAnd(append([]*roaring.Bitmap{freshRange(0, 4<<16)}, shared...)...))
	wantSearch := valueSearches(shared, targets)

	gotFastAnd := make([][]byte, goroutines)
	gotSearch := make([][]int64, goroutines)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for g := range goroutines {
		wg.Go(func() {
			<-start
			for range rounds {
				fb, err := roaring.FastAnd(
					append([]*roaring.Bitmap{freshRange(0, 4<<16)}, shared...)...).ToBytes()
				if err != nil {
					panic(err)
				}
				gotFastAnd[g] = fb
				gotSearch[g] = valueSearches(shared, targets)
			}
		})
	}
	close(start)
	wg.Wait()

	requireUnchanged(t, shared, before, "concurrent reads")
	for g := range goroutines {
		require.Equal(t, wantFastAnd, gotFastAnd[g], "goroutine %d disagreed on FastAnd", g)
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
