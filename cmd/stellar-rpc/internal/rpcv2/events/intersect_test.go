package events

import (
	"math/rand"
	"slices"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeTestPostings builds card postings scattered over span, in the requested
// form, and returns the equivalent bitmap for cross-checking.
func makeTestPostings(tb testing.TB, rng *rand.Rand, card int, span uint32, asIDs bool) (Postings, *roaring.Bitmap) {
	tb.Helper()
	// Without this the distinct-value loop below never terminates.
	require.LessOrEqual(tb, uint32(card), span, "cardinality must fit in the span")
	set := make(map[uint32]struct{}, card)
	for len(set) < card {
		set[uint32(rng.Int31n(int32(span)))] = struct{}{}
	}
	ids := make([]uint32, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	bm := roaring.BitmapOf(ids...)
	if asIDs {
		return IDPostings(ids), bm
	}
	return BitmapPostings(bm), bm
}

// TestIntersectMatchesFastAnd cross-checks Intersect against roaring.FastAnd,
// the operation it replaces. The span is kept tight relative to cardinality so
// intersections are usually non-empty — with postings spread thinly, most
// rounds would compare one empty result against another and prove nothing.
func TestIntersectMatchesFastAnd(t *testing.T) {
	rng := rand.New(rand.NewSource(7))

	for round := range 300 {
		inputs := make([]Postings, 2+rng.Intn(3))
		want := make([]*roaring.Bitmap, len(inputs))
		for i := range inputs {
			// Mixed forms and a wide cardinality spread, so the driver is
			// sometimes an ID list and sometimes a bitmap, and both probe
			// strategies get exercised.
			inputs[i], want[i] = makeTestPostings(t, rng, 1+rng.Intn(60), 100, rng.Intn(2) == 0)
		}

		got := Intersect(inputs)
		expected := roaring.FastAnd(want...)
		assert.Equal(t, expected.ToArray(), got.Bitmap().ToArray(), "round %d", round)
		assert.Equal(t, !expected.IsEmpty(), got.Present(), "round %d presence", round)
	}
}

// TestIntersectDriverStrategies covers the shapes that select each probe
// strategy, including the ratio at which a cursored walk gives way to binary
// search and the case where the smallest side is a bitmap.
func TestIntersectDriverStrategies(t *testing.T) {
	rng := rand.New(rand.NewSource(11))

	for _, tc := range []struct {
		name        string
		driverCard  int
		otherCard   int
		driverAsIDs bool
		otherAsIDs  bool
	}{
		{"equal lists take the walk", 400, 400, true, true},
		{"lopsided lists take the search", 4, 400, true, true},
		{"tiny list driver forces the search", 1, 2000, true, true},
		{"list driver against a bitmap", 40, 2000, true, false},
		{"bitmap driver against a list", 40, 2000, false, true},
		{"all bitmaps fall back to FastAnd", 40, 2000, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, abm := makeTestPostings(t, rng, tc.driverCard, 4000, tc.driverAsIDs)
			b, bbm := makeTestPostings(t, rng, tc.otherCard, 4000, tc.otherAsIDs)

			// Pass largest-first: Intersect owns the ordering.
			got := Intersect([]Postings{b, a})
			assert.Equal(t, roaring.And(abm, bbm).ToArray(), got.Bitmap().ToArray())
		})
	}
}

func TestIntersectDegenerate(t *testing.T) {
	present := IDPostings([]uint32{1, 2, 3})

	assert.False(t, Intersect(nil).Present())
	assert.False(t, Intersect([]Postings{}).Present())
	assert.False(t, Intersect([]Postings{present, {}}).Present(),
		"one absent term empties the intersection")
	assert.False(t, Intersect([]Postings{IDPostings([]uint32{1}), IDPostings([]uint32{2})}).Present(),
		"disjoint inputs are absent, not an empty present set")

	// A single input passes through untouched, so a lone bitmap-backed term is
	// not materialized on the way past.
	bm := roaring.BitmapOf(1, 2, 3)
	single := Intersect([]Postings{BitmapPostings(bm)})
	require.True(t, single.Present())
	assert.Same(t, bm, single.Bitmap())
}

// TestUnionMatchesFastOr cross-checks the merge path against roaring.FastOr,
// which is both the operation it replaces and the fallback it still uses when
// any input is bitmap-backed.
func TestUnionMatchesFastOr(t *testing.T) {
	rng := rand.New(rand.NewSource(13))

	for round := range 400 {
		inputs := make([]Postings, 2+rng.Intn(4))
		want := make([]*roaring.Bitmap, len(inputs))
		// Mixed forms so both the merge and the FastOr fallback run, and
		// occasionally all-lists so the merge runs on every input.
		allLists := rng.Intn(3) == 0
		for i := range inputs {
			asIDs := allLists || rng.Intn(2) == 0
			inputs[i], want[i] = makeTestPostings(t, rng, 1+rng.Intn(80), 200, asIDs)
		}

		got := Union(inputs)
		expected := roaring.FastOr(want...)
		require.True(t, got.Present(), "round %d", round)
		assert.Equal(t, expected.ToArray(), got.Bitmap().ToArray(), "round %d", round)
		if allLists {
			ids := got.IDs()
			require.NotNil(t, ids, "round %d: all-list inputs must merge, not materialize", round)
			// Assert on the raw slice, not through Bitmap(): that sorts and
			// dedups, so it would launder a merge that emitted duplicates or
			// went backwards. IDPostings does not check the precondition, and
			// violating it answers wrongly rather than failing.
			assert.Equal(t, expected.ToArray(), ids, "round %d: merged ids", round)
		}
	}
}

// TestUnionMergeBoundaries covers the shapes a randomized sweep reaches rarely:
// full overlap, no overlap, one list a prefix or suffix of the other, and
// duplicate values across every input.
func TestUnionMergeBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   [][]uint32
		want []uint32
	}{
		{"identical", [][]uint32{{1, 2, 3}, {1, 2, 3}}, []uint32{1, 2, 3}},
		{"disjoint interleaved", [][]uint32{{1, 3, 5}, {2, 4, 6}}, []uint32{1, 2, 3, 4, 5, 6}},
		{"disjoint blocks", [][]uint32{{1, 2}, {8, 9}}, []uint32{1, 2, 8, 9}},
		{"prefix", [][]uint32{{1, 2}, {1, 2, 3, 4}}, []uint32{1, 2, 3, 4}},
		{"suffix", [][]uint32{{3, 4}, {1, 2, 3, 4}}, []uint32{1, 2, 3, 4}},
		{"singletons", [][]uint32{{5}, {5}, {5}}, []uint32{5}},
		{"three-way", [][]uint32{{1, 4}, {2, 5}, {3, 6}}, []uint32{1, 2, 3, 4, 5, 6}},
		{"extremes", [][]uint32{{0}, {4294967295}}, []uint32{0, 4294967295}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ps := make([]Postings, len(tc.in))
			for i, ids := range tc.in {
				ps[i] = IDPostings(ids)
			}
			got := Union(ps)
			require.NotNil(t, got.IDs(), "all-list inputs must merge")
			assert.Equal(t, tc.want, got.SelectIDs(0, false))
		})
	}
}

func TestUnion(t *testing.T) {
	a := IDPostings([]uint32{1, 5})
	b := BitmapPostings(roaring.BitmapOf(5, 9))

	assert.False(t, Union(nil).Present())
	assert.False(t, Union([]Postings{{}, {}}).Present())

	// A lone present input passes through, keeping its form.
	single := Union([]Postings{{}, a, {}})
	require.True(t, single.Present())
	assert.NotNil(t, single.IDs(), "a passed-through input must keep its form")

	both := Union([]Postings{a, b})
	require.True(t, both.Present())
	assert.Equal(t, []uint32{1, 5, 9}, both.SelectIDs(0, false))
}
