package events

import (
	"encoding/hex"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPostingsForms(t *testing.T) {
	ids := []uint32{3, 9, 40, 41, 9000}

	for _, tc := range []struct {
		name    string
		post    Postings
		wantIDs bool
	}{
		{"ids", IDPostings(ids), true},
		{"bitmap", BitmapPostings(roaring.BitmapOf(ids...)), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.True(t, tc.post.Present())
			assert.Equal(t, uint64(len(ids)), tc.post.Cardinality())
			assert.Equal(t, tc.wantIDs, tc.post.IDs() != nil,
				"IDs() is the planner's signal that it can drive from this side")

			for _, id := range ids {
				assert.True(t, tc.post.Contains(id), "missing %d", id)
			}
			for _, id := range []uint32{0, 2, 4, 39, 42, 8999, 9001} {
				assert.False(t, tc.post.Contains(id), "unexpected %d", id)
			}

			assert.Equal(t, ids, tc.post.Bitmap().ToArray())
		})
	}
}

// TestPostingsAbsent pins the zero value as the miss signal, which is what
// LookupKeys returns for a key the index does not hold.
func TestPostingsAbsent(t *testing.T) {
	for _, tc := range []struct {
		name string
		post Postings
	}{
		{"zero value", Postings{}},
		{"nil ids", IDPostings(nil)},
		{"empty ids", IDPostings([]uint32{})},
		{"nil bitmap", BitmapPostings(nil)},
		{"empty bitmap", BitmapPostings(roaring.New())},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.False(t, tc.post.Present())
			assert.Zero(t, tc.post.Cardinality())
			assert.False(t, tc.post.Contains(0))
			assert.True(t, tc.post.Bitmap().IsEmpty())
		})
	}
}

// TestPostingsBitmapBorrows pins that a bitmap-backed Postings hands back the
// store's own bitmap rather than a copy. The hot store's dense overlay relies
// on that to answer without a per-key Clone, which is also why callers must
// treat the result as read-only.
func TestPostingsBitmapBorrows(t *testing.T) {
	bm := roaring.BitmapOf(1, 2, 3)
	assert.Same(t, bm, BitmapPostings(bm).Bitmap())

	// An ID-backed one has nothing to borrow, so it materializes fresh and
	// mutating the result cannot reach the postings.
	ids := []uint32{1, 2, 3}
	p := IDPostings(ids)
	p.Bitmap().Add(4)
	assert.False(t, p.Contains(4))
	assert.Equal(t, []uint32{1, 2, 3}, ids)
}

// TestPostingsClipRangeAndSelect checks both forms agree, since the ID form
// takes a completely different route: two binary searches and a subslice
// against a window bitmap and an intersection.
func TestPostingsClipRangeAndSelect(t *testing.T) {
	ids := []uint32{3, 9, 40, 41, 9000}

	forms := map[string]Postings{
		"ids":    IDPostings(ids),
		"bitmap": BitmapPostings(roaring.BitmapOf(ids...)),
	}

	for _, tc := range []struct {
		name       string
		start, end uint32
		want       []uint32
	}{
		{"everything", 0, 10000, ids},
		{"exact bounds are half-open", 3, 9000, []uint32{3, 9, 40, 41}},
		{"interior", 9, 42, []uint32{9, 40, 41}},
		{"between postings", 42, 8999, nil},
		{"empty range", 40, 40, nil},
		{"inverted range", 100, 10, nil},
		{"below everything", 0, 3, nil},
		{"above everything", 9001, 99999, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for form, p := range forms {
				got := p.ClipRange(tc.start, tc.end)
				if tc.want == nil {
					assert.False(t, got.Present(), form)
					continue
				}
				require.True(t, got.Present(), form)
				assert.Equal(t, tc.want, got.SelectIDs(0, false), form)
			}
		})
	}

	for _, tc := range []struct {
		name       string
		maxIDs     int
		descending bool
		want       []uint32
	}{
		{"uncapped", 0, false, ids},
		{"uncapped descending is still ascending", 0, true, ids},
		{"cap keeps the lowest", 2, false, []uint32{3, 9}},
		{"descending cap keeps the highest", 2, true, []uint32{41, 9000}},
		{"cap above cardinality", 99, false, ids},
		{"negative cap means all", -1, false, ids},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for form, p := range forms {
				assert.Equal(t, tc.want, p.SelectIDs(tc.maxIDs, tc.descending), form)
			}
		})
	}
}

// emptyRunContainerBitmap is a bitmap roaring's UnmarshalBinary accepts but
// that holds containers and no postings: one run container with zero
// intervals. Minimum and Maximum fault on it.
func emptyRunContainerBitmap(t *testing.T) *roaring.Bitmap {
	t.Helper()
	raw, err := hex.DecodeString("3b3000000100008713000000008713")
	require.NoError(t, err)
	bm := roaring.New()
	require.NoError(t, bm.UnmarshalBinary(raw), "roaring must accept it, else this test proves nothing")
	require.False(t, bm.IsEmpty(), "must have containers")
	require.Zero(t, bm.GetCardinality(), "and no postings")
	return bm
}

// TestPostingsClipRangeToleratesPostingless pins that ClipRange does not fault
// on a bitmap with containers but no postings. Producers never make one, and
// the cold reader rejects it at decode, so this is the last line rather than
// the first.
func TestPostingsClipRangeToleratesPostingless(t *testing.T) {
	p := BitmapPostings(emptyRunContainerBitmap(t))

	assert.NotPanics(t, func() {
		assert.False(t, p.ClipRange(0, 1_000_000).Present())
	})
	assert.NotPanics(t, func() {
		assert.Empty(t, p.SelectIDs(0, false))
	})
	assert.NotPanics(t, func() {
		assert.False(t, Intersect([]Postings{p, IDPostings([]uint32{1})}).Present())
	})
}
