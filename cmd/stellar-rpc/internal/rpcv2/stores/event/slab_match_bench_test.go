package event

// slab_match_bench_test.go measures the slab engine against the cursor tree at
// two levels, because they answer different questions:
//
//   - BenchmarkMatchPage is what a getEvents page costs end to end — candidate
//     generation plus the fetch and post-filter both engines share. It is the
//     number a request sees, and the shared half dilutes the engine difference
//     exactly as production does.
//   - BenchmarkMatchCandidates strips the shared half and times only the
//     machinery being replaced: the cursor tree and the descending union
//     against the slab stepper.
//
// Both run over the shaped corpus from slab_match_differential_test.go, sized
// to span several real 65536-id slabs.

import (
	"context"
	"iter"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
)

// matchEngine is the shape both engines share, so a benchmark can name one
// and drive it without branching in the timed loop.
type matchEngine = func(
	context.Context, Reader, []Filter, IDRange, bool, int,
) iter.Seq2[Match, error]

// benchDir is one direction arm of every case.
type benchDir struct {
	name string
	desc bool
}

func benchDirs() []benchDir { return []benchDir{{"asc", false}, {"desc", true}} }

// benchArms pairs each engine with the name its benchmark reports under.
func benchArms() []struct {
	name   string
	engine matchEngine
} {
	return []struct {
		name   string
		engine matchEngine
	}{{"cursor", Matches}, {"slab", slabMatches}}
}

// benchCorpusSize spans four and a half slabs, so a page served from the head
// of the window leaves most of the corpus untouched and a full scan crosses
// every slab seam.
const benchCorpusSize = 300_000

// The corpus is expensive enough to build that every benchmark shares one.
var benchFixtureCache *shapedFixture

func benchFixture(tb testing.TB) *shapedFixture {
	tb.Helper()
	if benchFixtureCache == nil {
		benchFixtureCache = newShapedFixture(tb, benchCorpusSize)
	}
	return benchFixtureCache
}

// filterCommonPage is the everyday getEvents shape: a chunk-sized contract
// term ANDed with a chunk-sized topic term, beside a second filter naming a
// rare contract whose postings are still a sparse id list. Matches are common
// enough that a 1000-item page fills from the head of the window.
func (f *shapedFixture) filterCommonPage() []Filter {
	return []Filter{
		{
			ContractID: f.vocab.contracts[1],
			Topics:     [protocol.MaxTopicCount][]byte{0: f.vocab.topicRaw[1]},
		},
		{ContractID: f.vocab.contracts[2]},
	}
}

// benchCase is one query shape plus the page size a consumer stops at. A zero
// limit is a full-window scan.
type benchCase struct {
	name    string
	filters []Filter
	limit   int
}

func benchCases(f *shapedFixture) []benchCase {
	return []benchCase{
		{"common", f.filterCommonPage(), 1000},
		{"spill", f.filterThinOverlap(), 1000},
		{"fullscan", f.filterDenseOnly(), 0},
	}
}

// BenchmarkMatchPage times one served page, fetch and post-filter included.
func BenchmarkMatchPage(b *testing.B) {
	f := benchFixture(b)
	r := diffPostingsReader{diffReader{f.corpus}}
	for _, c := range benchCases(f) {
		for _, dir := range benchDirs() {
			for _, arm := range benchArms() {
				b.Run(c.name+"/"+dir.name+"/"+arm.name, func(b *testing.B) {
					benchPageRun(b, r, arm.engine, c, dir.desc)
				})
			}
		}
	}
}

func benchPageRun(b *testing.B, r Reader, engine matchEngine, c benchCase, desc bool) {
	b.Helper()
	ctx := context.Background()
	window := IDRange{0, benchCorpusSize}
	b.ReportAllocs()
	var sink uint32
	for b.Loop() {
		n := 0
		for m, err := range engine(ctx, r, c.filters, window, desc, c.limit) {
			if err != nil {
				b.Fatal(err)
			}
			sink, n = m.Ordinal, n+1
			if c.limit > 0 && n == c.limit {
				break
			}
		}
	}
	_ = sink
}

// BenchmarkMatchCandidates times candidate generation alone: the cursor tree
// and the descending materialized union against the slab stepper, drained in
// the same batch sizes the streaming loops use, with no fetch and no
// post-filter in the way.
func BenchmarkMatchCandidates(b *testing.B) {
	f := benchFixture(b)
	r := diffPostingsReader{diffReader{f.corpus}}
	window := IDRange{0, benchCorpusSize}

	for _, c := range benchCases(f) {
		for _, dir := range benchDirs() {
			for _, name := range []string{"cursor", "slab"} {
				b.Run(c.name+"/"+dir.name+"/"+name, func(b *testing.B) {
					ctx := context.Background()
					b.ReportAllocs()
					var sink int
					for b.Loop() {
						if name == "slab" {
							sink = drainSlabCandidates(ctx, b, r, c, window, dir.desc)
						} else {
							sink = drainCursorCandidates(ctx, b, r, c, window, dir.desc)
						}
					}
					_ = sink
				})
			}
		}
	}
}

// drainCursorCandidates mirrors streamCandidates and streamUnion without the
// fetch: same batch sizing, same per-batch id collection, same early stop.
func drainCursorCandidates(
	ctx context.Context, b *testing.B, r Reader, c benchCase, window IDRange, desc bool,
) int {
	b.Helper()
	plans, keys, matchAll := planIndexTerms(c.filters)
	if matchAll {
		b.Fatal("benchmark filters must reach the index")
	}
	if desc {
		union, err := unionForFilters(ctx, r, plans, keys, window)
		if err != nil {
			b.Fatal(err)
		}
		it := union.ReverseIterator()
		return drainBatches(c, func(ids []uint32, batch int) []uint32 {
			for it.HasNext() && len(ids) < batch {
				ids = append(ids, it.Next())
			}
			return ids
		})
	}
	sources, err := lookupPostings(ctx, r, keys)
	if err != nil {
		b.Fatal(err)
	}
	cand := candidateIter(plans, sources, window)
	return drainBatches(c, func(ids []uint32, batch int) []uint32 {
		for len(ids) < batch {
			id, ok := cand.peek()
			if !ok {
				return ids
			}
			ids = append(ids, id)
			cand.next()
		}
		return ids
	})
}

// drainSlabCandidates is the same drain over the slab stepper.
func drainSlabCandidates(
	ctx context.Context, b *testing.B, r Reader, c benchCase, window IDRange, desc bool,
) int {
	b.Helper()
	plans, keys, matchAll := planIndexTerms(c.filters)
	if matchAll {
		b.Fatal("benchmark filters must reach the index")
	}
	sources, err := lookupPostings(ctx, r, keys)
	if err != nil {
		b.Fatal(err)
	}
	st := newSlabStepper(plans, sources, window, desc)
	return drainBatches(c, func(ids []uint32, batch int) []uint32 {
		return st.appendUpTo(ids, batch)
	})
}

// drainBatches is the streaming loops' batch cadence with the fetch removed:
// fill up to the batch size, stop when a fill comes back empty or the page is
// full. Both arms share it so the harness cannot favor either.
func drainBatches(c benchCase, fill func(ids []uint32, batch int) []uint32) int {
	batch, rest := batchSizes(c.limit)
	ids := make([]uint32, 0, batch)
	total := 0
	for {
		ids = fill(ids[:0], batch)
		batch = rest
		if len(ids) == 0 {
			return total
		}
		total += len(ids)
		if c.limit > 0 && total >= c.limit {
			return total
		}
	}
}

// ───────────── the in-tree shape matrix, with a slab arm ─────────────

// BenchmarkCandidateSlab is the third arm of match_iter_test.go's per-shape
// A/B: the same plan, the same postings, the same page fingerprint, answered
// by the slab stepper instead of the cursor tree (BenchmarkCandidateTree) or
// the whole-window union (BenchmarkCandidateMaterialized). The shape matrix
// already holds the fat/thin-overlap geometries the alignment budget was built
// for, so this is the directly comparable number.
func BenchmarkCandidateSlab(b *testing.B) {
	for _, sh := range benchShapes(benchEvents) {
		b.Run(sh.name, func(b *testing.B) {
			benchSlabPage(b, shapeFor(sh.name, sh.build))
		})
	}
}

func benchSlabPage(b *testing.B, s *benchShape) {
	b.Helper()
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		sources, err := lookupPostings(ctx, s.reader, s.keys)
		if err != nil {
			b.Fatal(err)
		}
		st := newSlabStepper(s.plans, sources, s.window, false)
		ids := make([]uint32, 0, benchPage)
		n, sum := 0, uint64(0)
		for n < benchPage {
			ids = st.appendUpTo(ids[:0], benchPage-n)
			if len(ids) == 0 {
				break
			}
			for _, v := range ids {
				n, sum = n+1, sum+uint64(v)
			}
		}
		if n != s.wantCount || sum != s.wantSum {
			b.Fatalf("page mismatch: got (%d, %d), want (%d, %d)",
				n, sum, s.wantCount, s.wantSum)
		}
	}
}

// TestBenchShapesAgreeSlab is TestBenchShapesAgree's slab twin: every geometry
// the microbench above measures must be one the slab stepper answers exactly,
// so a shape can never post a number for a query it gets wrong.
func TestBenchShapesAgreeSlab(t *testing.T) {
	const domain = 1 << 16
	for _, sh := range benchShapes(domain) {
		t.Run(sh.name, func(t *testing.T) {
			s := sh.build()
			sources, err := lookupPostings(context.Background(), s.reader, s.keys)
			require.NoError(t, err)
			want := referenceCandidates(s.plans, sources, s.window)

			st := newSlabStepper(s.plans, sources, s.window, false)
			got := []uint32{}
			for {
				before := len(got)
				got = st.appendUpTo(got, before+512)
				if len(got) == before {
					break
				}
			}
			require.Equal(t, want, got)
			require.NotEmpty(t, got, "shape sanity: the plan must select something")

			// The descending arm reads the same set backwards.
			rev := newSlabStepper(s.plans, sources, s.window, true)
			gotDesc := []uint32{}
			for {
				before := len(gotDesc)
				gotDesc = rev.appendUpTo(gotDesc, before+512)
				if len(gotDesc) == before {
					break
				}
			}
			slices.Reverse(gotDesc)
			require.Equal(t, want, gotDesc)
		})
	}
}
