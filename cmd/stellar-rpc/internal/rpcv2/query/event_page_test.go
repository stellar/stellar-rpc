package query

import (
	"context"
	"fmt"
	"iter"
	"math"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rpcv2test"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/hotchunk"
)

// Pager fixtures: hot chunks seeded through the real ingest path, read
// through the real ReadView, so QueryEvents is exercised end to end.
// Events are labeled by their data symbol; tests assert on label
// sequences.

var (
	cidA = testContractID(0x0a)
	cidB = testContractID(0x0b)
	cidC = testContractID(0x0c)
)

func testContractID(b byte) xdr.ContractId {
	var id xdr.ContractId
	id[0] = b
	return id
}

// eventChunkSpec seeds one hot chunk: ledgers[i] holds the events of
// ledger c.FirstLedger()+i (nil = a committed, empty ledger). Ingest
// is contiguous from the chunk's first ledger (the store enforces it),
// and a seeded chunk must cover every ledger a test's walk needs: the
// pager refuses a window its offsets do not cover.
type eventChunkSpec struct {
	c       chunk.ID
	ledgers [][]xdr.ContractEvent
}

// seedEventChunks builds a registry over the given hot chunks with the
// retention floor pinned at earliest, and returns the per-chunk DBs so
// follow-the-tip tests can keep ingesting.
func seedEventChunks(
	t *testing.T, earliest chunk.ID, latest uint32, specs ...eventChunkSpec,
) (*Registry, map[chunk.ID]*hotchunk.DB) {
	t.Helper()
	cat := openTestCatalog(t, silentLogger())
	r := NewRegistry(cat, geometry.NewRetention(0, earliest))
	dbs := make(map[chunk.ID]*hotchunk.DB, len(specs))
	for _, s := range specs {
		db, err := hotchunk.Open(cat.Layout().HotChunkPath(s.c), s.c, silentLogger())
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		for i, evs := range s.ledgers {
			ingestEvents(t, db, s.c.FirstLedger()+uint32(i), evs)
		}
		require.NoError(t, cat.FlipHotReady(s.c))
		r.PublishHandle(s.c, db)
		dbs[s.c] = db
	}
	r.SetLatestLedger(latest, CloseTimeAt(0))
	return r, dbs
}

func ingestEvents(t *testing.T, db *hotchunk.DB, seq uint32, evs []xdr.ContractEvent) {
	t.Helper()
	var raw []byte
	if len(evs) == 0 {
		raw = rpcv2test.ZeroTxLCMBytes(t, seq)
	} else {
		raw = rpcv2test.EventsLCMBytes(t, seq, evs...)
	}
	rpcv2test.IngestLedger(t, db, seq, raw)
}

func symEvent(cid xdr.ContractId, label string) xdr.ContractEvent {
	return rpcv2test.SymbolContractEvent(cid, label, label)
}

// singleChunkFixture is the layout most tests run against. Chunk 5,
// F = its first ledger:
//
//	F+0: a0, a1, b0
//	F+1: (empty)
//	F+2: a2
//	F+3: b1, a3
//
// latest = F+3. Match-all order: a0 a1 b0 a2 b1 a3.
func singleChunkFixture(t *testing.T) (*Registry, *hotchunk.DB, uint32) {
	t.Helper()
	const c = chunk.ID(5)
	f := c.FirstLedger()
	r, dbs := seedEventChunks(t, c, f+3, eventChunkSpec{c: c, ledgers: [][]xdr.ContractEvent{
		{symEvent(cidA, "a0"), symEvent(cidA, "a1"), symEvent(cidB, "b0")},
		nil,
		{symEvent(cidA, "a2")},
		{symEvent(cidB, "b1"), symEvent(cidA, "a3")},
	}})
	return r, dbs[c], f
}

// fullChunkSeamFixture stages the real chunk 5 / chunk 6 seam: chunk 5
// fully ingested (its last three ledgers hold events, everything
// before them committed empty), chunk 6 holding two event ledgers,
// latest = chunk 6's second ledger. The full ingest costs ~42s at
// ~4.2ms per ledger through the real path, so the seam tests run in
// one testing.Short()-guarded test, like the e2e lifecycle test.
// Match-all order: x0 x1 y0 x2 | x3 y1 x4.
func fullChunkSeamFixture(t *testing.T) (*Registry, uint32, uint32) {
	t.Helper()
	const c5, c6 = chunk.ID(5), chunk.ID(6)
	f6 := c6.FirstLedger()
	lo := f6 - 3
	chunk5 := make([][]xdr.ContractEvent, chunk.LedgersPerChunk)
	chunk5[chunk.LedgersPerChunk-3] = []xdr.ContractEvent{symEvent(cidA, "x0")}
	chunk5[chunk.LedgersPerChunk-2] = []xdr.ContractEvent{symEvent(cidA, "x1"), symEvent(cidB, "y0")}
	chunk5[chunk.LedgersPerChunk-1] = []xdr.ContractEvent{symEvent(cidA, "x2")}
	r, _ := seedEventChunks(t, c5, f6+1,
		eventChunkSpec{c: c5, ledgers: chunk5},
		eventChunkSpec{c: c6, ledgers: [][]xdr.ContractEvent{
			{symEvent(cidA, "x3"), symEvent(cidB, "y1")},
			{symEvent(cidA, "x4")},
		}},
	)
	return r, lo, f6
}

func labels(t *testing.T, payloads []event.Payload) []string {
	t.Helper()
	out := make([]string, len(payloads))
	for i := range payloads {
		var ev xdr.ContractEvent
		require.NoError(t, ev.UnmarshalBinary(payloads[i].ContractEventBytes))
		require.NotNil(t, ev.Body.V0)
		out[i] = string(*ev.Body.V0.Data.Sym)
	}
	return out
}

// pageDriver plays the handler role across pages: a fresh ReadView per
// page, each minted cursor encoded and decoded before the next call.
// Exactly the wire loop, so every pager test also enforces the codec
// contract.
type pageDriver struct {
	t      *testing.T
	r      *Registry
	cursor EventCursor
	limit  int
}

func (d *pageDriver) next() *EventPage {
	d.t.Helper()
	page, err := d.tryNext()
	require.NoError(d.t, err)
	return page
}

func (d *pageDriver) tryNext() (*EventPage, error) {
	d.t.Helper()
	a, err := d.r.NewReadView()
	require.NoError(d.t, err)
	defer a.Release()
	page, err := a.QueryEvents(context.Background(), d.cursor, d.limit)
	if err != nil {
		return nil, err
	}
	// Every minted cursor rides the wire codec, so each pager test also
	// enforces the encode/decode contract.
	enc, err := page.Next.Encode()
	require.NoError(d.t, err)
	dec, err := DecodeEventCursor(enc)
	require.NoError(d.t, err)
	d.cursor = *dec
	return page, nil
}

// drain pages until a terminal status, returning the concatenated
// labels and the final status. Fails the test if any page overflows
// the limit or the walk takes too many pages.
func (d *pageDriver) drain() ([]string, ScanStatus) {
	d.t.Helper()
	var all []string
	for range 50 {
		page := d.next()
		require.LessOrEqual(d.t, len(page.Events), d.limit, "a page must not overflow its limit")
		all = append(all, labels(d.t, page.Events)...)
		if page.Status != ScanHasMore {
			return all, page.Status
		}
	}
	d.t.Fatal("drain did not terminate in 50 pages")
	return nil, 0
}

// topicFilter is a filter naming raw as its topic0 value.
func topicFilter(raw []byte) event.Filter {
	var f event.Filter
	f.Topics[0] = raw
	return f
}

// voidScValBytes is the shortest well-formed ScVal encoding.
func voidScValBytes(t *testing.T) []byte {
	t.Helper()
	raw, err := xdr.ScVal{Type: xdr.ScValTypeScvVoid}.MarshalBinary()
	require.NoError(t, err)
	return raw
}

func TestQueryEvents_SinglePageComplete(t *testing.T) {
	r, _, f := singleChunkFixture(t)
	maxL := f + 3
	d := &pageDriver{
		t: t, r: r, limit: 10,
		cursor: EventCursor{Scope: EventScope{MinLedger: f, MaxLedger: &maxL}},
	}
	page := d.next()
	assert.Equal(t, []string{"a0", "a1", "b0", "a2", "b1", "a3"}, labels(t, page.Events))
	assert.Equal(t, ScanComplete, page.Status)
	assert.Equal(t, f+3, page.Next.ScannedLedger)
	assert.Nil(t, page.Next.Position, "the watermark passed the last delivery")
}

// TestQueryEvents_PagingSeamMatrix walks the whole fixture at every
// small limit, in both directions. This traverses each resume boundary
// class inside one chunk (mid-ledger, ledger end) and pins
// no-duplicate, no-gap delivery across pages regardless of where the
// seams land.
func TestQueryEvents_PagingSeamMatrix(t *testing.T) {
	r, _, f := singleChunkFixture(t)
	maxL := f + 3
	asc := []string{"a0", "a1", "b0", "a2", "b1", "a3"}
	desc := []string{"a3", "b1", "a2", "b0", "a1", "a0"}
	for _, dir := range []Direction{Ascending, Descending} {
		for _, limit := range []int{1, 2, 3, 4} {
			t.Run(fmt.Sprintf("dir=%d/limit=%d", dir, limit), func(t *testing.T) {
				d := &pageDriver{
					t: t, r: r, limit: limit,
					cursor: EventCursor{Scope: EventScope{MinLedger: f, MaxLedger: &maxL, Dir: dir}},
				}
				got, status := d.drain()
				want := asc
				if dir == Descending {
					want = desc
				}
				assert.Equal(t, want, got)
				assert.Equal(t, ScanComplete, status)
			})
		}
	}
}

// TestQueryEvents_FilteredMidLedgerResume pins the page-boundary
// watermark on the filtered path: page 1 stops knowing the next match
// is in F+2, so F and the empty F+1 count as covered, and the position
// is dropped because the watermark passed it.
func TestQueryEvents_FilteredMidLedgerResume(t *testing.T) {
	r, _, f := singleChunkFixture(t)
	maxL := f + 3
	d := &pageDriver{
		t: t, r: r, limit: 2,
		cursor: EventCursor{Scope: EventScope{
			MinLedger: f, MaxLedger: &maxL,
			Filters: []event.Filter{{ContractID: cidA[:]}},
		}},
	}

	page := d.next()
	assert.Equal(t, []string{"a0", "a1"}, labels(t, page.Events))
	assert.Equal(t, ScanHasMore, page.Status)
	assert.Equal(t, f+1, page.Next.ScannedLedger,
		"the next match is in F+2, so F and the empty F+1 are fully covered")
	assert.Nil(t, page.Next.Position, "the watermark passed it")

	page = d.next()
	assert.Equal(t, []string{"a2", "a3"}, labels(t, page.Events))
	assert.Equal(t, ScanComplete, page.Status, "the walk finished the bounds as the page filled")
}

// TestQueryEvents_DescendingFilteredMidLedgerResume is the descending
// mirror: page 1 stops between two matches inside ledger f, so page 2
// re-enters that ledger below the position.
func TestQueryEvents_DescendingFilteredMidLedgerResume(t *testing.T) {
	r, _, f := singleChunkFixture(t)
	maxL := f + 3
	d := &pageDriver{
		t: t, r: r, limit: 3,
		cursor: EventCursor{Scope: EventScope{
			MinLedger: f, MaxLedger: &maxL, Dir: Descending,
			Filters: []event.Filter{{ContractID: cidA[:]}},
		}},
	}

	page := d.next()
	assert.Equal(t, []string{"a3", "a2", "a1"}, labels(t, page.Events))
	assert.Equal(t, ScanHasMore, page.Status)
	assert.Equal(t, f+1, page.Next.ScannedLedger,
		"the next match is in f, so f+1 and everything above are fully covered")
	require.NotNil(t, page.Next.Position)
	assert.Equal(t, f, page.Next.Position.Ledger)
	assert.Equal(t, uint32(1), page.Next.Position.LedgerOrdinal, "a1 is ledger f's second stored event")

	page = d.next()
	assert.Equal(t, []string{"a0"}, labels(t, page.Events))
	assert.Equal(t, ScanComplete, page.Status)
}

func TestQueryEvents_ChunkSeamFullChunk(t *testing.T) {
	if testing.Short() {
		t.Skip("the seam fixture ingests a full 10k-ledger chunk; skipped in -short")
	}
	r, lo, f6 := fullChunkSeamFixture(t)
	maxL := f6 + 1

	// Cross-chunk walks: both directions at limits that break pages
	// inside chunks, at the seam, and not at all.
	asc := []string{"x0", "x1", "y0", "x2", "x3", "y1", "x4"}
	desc := []string{"x4", "y1", "x3", "x2", "y0", "x1", "x0"}
	for _, tc := range []struct {
		dir  Direction
		want []string
	}{{Ascending, asc}, {Descending, desc}} {
		for _, limit := range []int{2, 3, 7} {
			t.Run(fmt.Sprintf("walk/dir=%d/limit=%d", tc.dir, limit), func(t *testing.T) {
				d := &pageDriver{
					t: t, r: r, limit: limit,
					cursor: EventCursor{Scope: EventScope{MinLedger: lo, MaxLedger: &maxL, Dir: tc.dir}},
				}
				got, status := d.drain()
				assert.Equal(t, tc.want, got)
				assert.Equal(t, ScanComplete, status)
			})
		}
	}

	// Chunk-end resume: the page break lands exactly at the seam; the
	// resume enters the next chunk, whose ordinals restart at 0.
	t.Run("resume at the seam", func(t *testing.T) {
		d := &pageDriver{
			t: t, r: r, limit: 4,
			cursor: EventCursor{Scope: EventScope{MinLedger: lo, MaxLedger: &maxL}},
		}
		page := d.next()
		assert.Equal(t, []string{"x0", "x1", "y0", "x2"}, labels(t, page.Events),
			"page 1 is exactly chunk 5's events")
		assert.Equal(t, ScanHasMore, page.Status)
		assert.Equal(t, lo+2, page.Next.ScannedLedger,
			"the finished part's watermark survives the page break at the seam")
		page = d.next()
		assert.Equal(t, []string{"x3", "y1", "x4"}, labels(t, page.Events))
		assert.Equal(t, ScanComplete, page.Status)
		assert.Nil(t, page.Next.Position, "the watermark passed the last delivery")
	})

	// ScanOldestReached is a per-node stop, not a scope stop: a node
	// with deeper retention (the full-chunk registry) resumes the same
	// cursor from the watermark and finishes the scope. The shallow
	// node serves chunk 6 only.
	t.Run("deeper node continues after OldestReached", func(t *testing.T) {
		const c6 = chunk.ID(6)
		shallow, _ := seedEventChunks(t, c6, f6+1, eventChunkSpec{c: c6, ledgers: [][]xdr.ContractEvent{
			{symEvent(cidA, "x3"), symEvent(cidB, "y1")},
			{symEvent(cidA, "x4")},
		}})
		d := &pageDriver{t: t, r: shallow, limit: 10, cursor: EventCursor{
			Scope: EventScope{MinLedger: lo, MaxLedger: &maxL, Dir: Descending},
		}}
		page := d.next()
		assert.Equal(t, []string{"x4", "y1", "x3"}, labels(t, page.Events))
		require.Equal(t, ScanOldestReached, page.Status)
		require.Equal(t, f6, page.Next.ScannedLedger, "covered down to the shallow node's floor")

		d.r = r
		all, status := d.drain()
		assert.Equal(t, []string{"x2", "y0", "x1", "x0"}, all)
		assert.Equal(t, ScanComplete, status)
	})
}

// TestQueryEvents_UncoveredWindowFailsLoud pins the offsets coverage
// check: a walk that needs ledgers a chunk's offsets do not cover is a
// store bug (backfill is chunk-aligned, latest never exceeds ingested
// range), and quietly clipping would let the watermark claim ledgers
// nothing scanned. The fixture stages the impossible state directly: a
// registry whose latest is beyond a short-seeded chunk.
func TestQueryEvents_UncoveredWindowFailsLoud(t *testing.T) {
	const c = chunk.ID(5)
	f := c.FirstLedger()
	r, _ := seedEventChunks(t, c, f+10, eventChunkSpec{c: c, ledgers: [][]xdr.ContractEvent{
		{symEvent(cidA, "a0")},
	}})
	a, err := r.NewReadView()
	require.NoError(t, err)
	defer a.Release()
	_, err = a.QueryEvents(context.Background(),
		EventCursor{Scope: EventScope{MinLedger: f}}, 10)
	require.ErrorContains(t, err, "offsets cover")

	// Head side: ingest enforces chunk-aligned starts, so this state
	// cannot be staged through a real store; scanChunk is checked
	// directly with offsets that begin past the part's From.
	ofs := event.NewLedgerOffsets(f + 8)
	require.NoError(t, ofs.Append(f+8, 1))
	_, err = scanChunk(context.Background(),
		eventPart{Chunk: c, Reader: &fakeEventReader{chunkID: c, ofs: ofs}, From: f, To: f + 8},
		nil, nil, false, 10)
	require.ErrorContains(t, err, "offsets cover")
}

// TestQueryEvents_FollowsTheTip pins the open-upper-bound walk: an
// unbounded ascending query drains served history, reports
// WAITING_FOR_LEDGERS, and a later page (new view, advanced tip)
// delivers exactly the newly committed events.
func TestQueryEvents_FollowsTheTip(t *testing.T) {
	r, db, f := singleChunkFixture(t)
	d := &pageDriver{t: t, r: r, limit: 10, cursor: EventCursor{Scope: EventScope{MinLedger: f}}}

	page := d.next()
	assert.Len(t, page.Events, 6)
	assert.Equal(t, ScanWaitingForLedgers, page.Status)
	assert.Equal(t, f+3, page.Next.ScannedLedger)

	ingestEvents(t, db, f+4, []xdr.ContractEvent{symEvent(cidA, "a4"), symEvent(cidB, "b2")})
	r.SetLatestLedger(f+4, CloseTimeAt(0))

	page = d.next()
	assert.Equal(t, []string{"a4", "b2"}, labels(t, page.Events), "only the new ledger's events")
	assert.Equal(t, ScanWaitingForLedgers, page.Status)
	assert.Equal(t, f+4, page.Next.ScannedLedger)
}

// TestQueryEvents_WatermarkOnlyResume is the plan's watermark
// rationale: a page that delivers nothing still advances the scanned
// watermark, so the next page does not rescan the same ledgers, and
// resume works with no position at all.
func TestQueryEvents_WatermarkOnlyResume(t *testing.T) {
	r, db, f := singleChunkFixture(t)
	d := &pageDriver{
		t: t, r: r, limit: 10,
		cursor: EventCursor{Scope: EventScope{
			MinLedger: f,
			Filters:   []event.Filter{{ContractID: cidC[:]}},
		}},
	}

	page := d.next()
	assert.Empty(t, page.Events)
	assert.Nil(t, page.Next.Position, "no event was ever delivered")
	assert.Equal(t, f+3, page.Next.ScannedLedger)
	assert.Equal(t, ScanWaitingForLedgers, page.Status)

	ingestEvents(t, db, f+4, []xdr.ContractEvent{symEvent(cidC, "c0")})
	r.SetLatestLedger(f+4, CloseTimeAt(0))

	page = d.next()
	assert.Equal(t, []string{"c0"}, labels(t, page.Events))
	assert.Equal(t, f+4, page.Next.ScannedLedger)
}

func TestQueryEvents_BoundBeyondLatestWaits(t *testing.T) {
	r, _, f := singleChunkFixture(t)
	maxL := f + 10 // beyond latest (f+3)
	d := &pageDriver{
		t: t, r: r, limit: 10,
		cursor: EventCursor{Scope: EventScope{MinLedger: f, MaxLedger: &maxL}},
	}
	page := d.next()
	assert.Len(t, page.Events, 6)
	assert.Equal(t, ScanWaitingForLedgers, page.Status,
		"the bound extends beyond latest, so the walk is not complete")
}

func TestQueryEvents_MinLedgerBeyondLatestIsEmptyWait(t *testing.T) {
	r, _, f := singleChunkFixture(t)
	d := &pageDriver{
		t: t, r: r, limit: 10,
		cursor: EventCursor{Scope: EventScope{MinLedger: f + 100}},
	}
	page := d.next()
	assert.Empty(t, page.Events)
	assert.Equal(t, ScanWaitingForLedgers, page.Status)
	assert.Zero(t, page.Next.ScannedLedger, "nothing was covered; the echoed watermark stays zero")
}

func TestQueryEvents_OldestReachedDescending(t *testing.T) {
	r, _, f := singleChunkFixture(t)
	maxL := f + 3
	d := &pageDriver{
		t: t, r: r, limit: 10,
		cursor: EventCursor{Scope: EventScope{MinLedger: 2, MaxLedger: &maxL, Dir: Descending}},
	}
	page := d.next()
	assert.Len(t, page.Events, 6)
	assert.Equal(t, ScanOldestReached, page.Status,
		"bounds extend below the retention floor")
	assert.Equal(t, f, page.Next.ScannedLedger)

	// A re-pull of the cursor the floor page minted stays OldestReached:
	// empty page, bookmarks unmoved, never an out-of-range error.
	before := d.cursor
	page = d.next()
	assert.Empty(t, page.Events)
	assert.Equal(t, ScanOldestReached, page.Status)
	assert.Equal(t, before, d.cursor)
}

// TestQueryEvents_BelowFloorByDirection pins the proposal's asymmetry
// at the floor: an ascending scan starting below it is an out-of-range
// error, a descending scan whose remaining range is below it reports
// an empty OldestReached page instead.
func TestQueryEvents_BelowFloorByDirection(t *testing.T) {
	r, _, f := singleChunkFixture(t)
	a, err := r.NewReadView()
	require.NoError(t, err)
	defer a.Release()

	// Fresh ascending query starting below the floor.
	_, err = a.QueryEvents(context.Background(),
		EventCursor{Scope: EventScope{MinLedger: 2}}, 10)
	var re *RangeError
	require.ErrorAs(t, err, &re)

	// Fresh descending scope entirely below the floor: legal and empty.
	lowMax := f - 1
	page, err := a.QueryEvents(context.Background(), EventCursor{
		Scope: EventScope{MinLedger: 2, MaxLedger: &lowMax, Dir: Descending},
	}, 10)
	require.NoError(t, err)
	assert.Empty(t, page.Events)
	assert.Equal(t, ScanOldestReached, page.Status)

	// Descending resume whose watermark stepped below the floor: the
	// same empty OldestReached page, not an error.
	maxL := f + 3
	cur := EventCursor{
		Scope:         EventScope{MinLedger: 2, MaxLedger: &maxL, Dir: Descending},
		ScannedLedger: f,
	}
	page, err = a.QueryEvents(context.Background(), cur, 10)
	require.NoError(t, err)
	assert.Empty(t, page.Events)
	assert.Equal(t, ScanOldestReached, page.Status)
	assert.Equal(t, cur, page.Next, "the cursor is unchanged")
}

// TestQueryEvents_ConsumedRangeCompletes: resume already moved past the
// query's far bound. The page is empty and terminal, and echoes the
// cursor state rather than erroring or rescanning.
func TestQueryEvents_ConsumedRangeCompletes(t *testing.T) {
	r, _, f := singleChunkFixture(t)
	a, err := r.NewReadView()
	require.NoError(t, err)
	defer a.Release()
	maxL := f + 3
	page, err := a.QueryEvents(context.Background(), EventCursor{
		Scope:         EventScope{MinLedger: f, MaxLedger: &maxL},
		ScannedLedger: f + 3,
	}, 10)
	require.NoError(t, err)
	assert.Empty(t, page.Events)
	assert.Equal(t, ScanComplete, page.Status)
	assert.Equal(t, f+3, page.Next.ScannedLedger)
	assert.Nil(t, page.Next.Position, "a completed cursor is a bare watermark")
}

// TestQueryEvents_EndStabilityAcrossIngest is the plan's End-pinning
// reconciliation in action: pages re-derive their ranges from ledger
// bounds against a growing hot chunk, and committed ledgers page
// identically while newly committed ones are visited exactly once.
func TestQueryEvents_EndStabilityAcrossIngest(t *testing.T) {
	r, db, f := singleChunkFixture(t)
	d := &pageDriver{t: t, r: r, limit: 2, cursor: EventCursor{Scope: EventScope{MinLedger: f}}}

	page := d.next()
	assert.Equal(t, []string{"a0", "a1"}, labels(t, page.Events))

	// Ingest between pages; the next page continues mid-ledger F.
	ingestEvents(t, db, f+4, []xdr.ContractEvent{symEvent(cidA, "a4")})
	r.SetLatestLedger(f+4, CloseTimeAt(0))

	var all []string
	all = append(all, labels(t, page.Events)...)
	for range 10 {
		page = d.next()
		all = append(all, labels(t, page.Events)...)
		if page.Status != ScanHasMore {
			break
		}
	}
	assert.Equal(t, []string{"a0", "a1", "b0", "a2", "b1", "a3", "a4"}, all,
		"no duplicates, no gaps, the new ledger visited exactly once")
	assert.Equal(t, ScanWaitingForLedgers, page.Status)
}

// TestQueryEvents_DescendingResumeAboveLatestWaits: a descending
// cursor minted by a node whose tip was ahead carries a resume point
// above this view's latest. A descending walk never revisits a
// ledger, so clamping and serving would skip the resume ledger's
// remainder and every ledger in between forever. The page waits
// instead: empty, cursor unchanged, ScanWaitingForLedgers. Once the
// view catches up, the walk continues with no gap and no duplicates.
func TestQueryEvents_DescendingResumeAboveLatestWaits(t *testing.T) {
	r, db, f := singleChunkFixture(t) // latest = f+3
	ingestEvents(t, db, f+4, []xdr.ContractEvent{symEvent(cidC, "c0")})
	ingestEvents(t, db, f+5, []xdr.ContractEvent{symEvent(cidB, "b2"), symEvent(cidA, "a4")})

	// Page 1 on the ahead node: latest = f+5, stop mid-ledger f+5.
	r.SetLatestLedger(f+5, CloseTimeAt(0))
	maxL := f + 5
	d := &pageDriver{
		t: t, r: r, limit: 1,
		cursor: EventCursor{Scope: EventScope{MinLedger: f, MaxLedger: &maxL, Dir: Descending}},
	}
	page := d.next()
	require.Equal(t, []string{"a4"}, labels(t, page.Events))
	require.Equal(t, f+5, page.Next.Position.Ledger)

	// Page 2 on a node that is behind: wait, don't skip past b2 and c0.
	r.SetLatestLedger(f+3, CloseTimeAt(0))
	before := d.cursor
	page = d.next()
	assert.Empty(t, page.Events)
	assert.Equal(t, ScanWaitingForLedgers, page.Status)
	assert.Equal(t, before, d.cursor, "waiting must not move the bookmarks")

	// Caught up: the walk continues at b2 with no gap and no duplicates.
	r.SetLatestLedger(f+5, CloseTimeAt(0))
	all, status := d.drain()
	assert.Equal(t, []string{"b2", "c0", "a3", "b1", "a2", "b0", "a1", "a0"}, all)
	assert.Equal(t, ScanComplete, status)
}

// TestQueryEvents_ScanBudget pins the per-page scan window: a filter
// that matches nothing advances the watermark one window per page
// (empty pages, ScanHasMore) instead of scanning the whole scope in
// one call, and delivery stays gapless across window seams.
func TestQueryEvents_ScanBudget(t *testing.T) {
	r, _, f := singleChunkFixture(t)
	r.maxScanLedgers = 2
	maxL := f + 3

	// Match-nothing filter: two windows cover the scope, no events.
	d := &pageDriver{t: t, r: r, limit: 10, cursor: EventCursor{Scope: EventScope{
		MinLedger: f, MaxLedger: &maxL,
		Filters: []event.Filter{{ContractID: cidC[:]}},
	}}}
	page := d.next()
	assert.Empty(t, page.Events)
	assert.Equal(t, ScanHasMore, page.Status)
	assert.Equal(t, f+1, page.Next.ScannedLedger, "first window covered")
	page = d.next()
	assert.Empty(t, page.Events)
	assert.Equal(t, ScanComplete, page.Status)
	assert.Equal(t, f+3, page.Next.ScannedLedger)

	// Match-all descending: the windows walk down without gaps.
	d = &pageDriver{t: t, r: r, limit: 10, cursor: EventCursor{Scope: EventScope{
		MinLedger: f, MaxLedger: &maxL, Dir: Descending,
	}}}
	all, status := d.drain()
	assert.Equal(t, []string{"a3", "b1", "a2", "b0", "a1", "a0"}, all)
	assert.Equal(t, ScanComplete, status)
}

// TestQueryEvents_OpensOnlyScannedChunks pins lazy chunk resolution: a
// page that fills inside the walk's first chunk never resolves the
// chunks behind it. The far chunk has no serving store here, so an
// eager open would fail every page; the pager only fails once the walk
// actually reaches it.
func TestQueryEvents_OpensOnlyScannedChunks(t *testing.T) {
	const c5, c6 = chunk.ID(5), chunk.ID(6)
	f5, f6 := c5.FirstLedger(), c6.FirstLedger()
	r, _ := seedEventChunks(t, c5, f6+1, eventChunkSpec{c: c6, ledgers: [][]xdr.ContractEvent{
		{symEvent(cidA, "q0"), symEvent(cidA, "q1")},
		{symEvent(cidB, "q2")},
	}})
	maxL := f6 + 1
	d := &pageDriver{t: t, r: r, limit: 1, cursor: EventCursor{
		Scope: EventScope{MinLedger: f5, MaxLedger: &maxL, Dir: Descending},
	}}

	got := make([]string, 0, 3)
	for range 3 {
		got = append(got, labels(t, d.next().Events)...)
	}
	assert.Equal(t, []string{"q2", "q1", "q0"}, got,
		"chunk 6 pages serve while chunk 5 has no store")

	_, err := d.tryNext()
	require.ErrorIs(t, err, ErrUnavailable,
		"the unserved chunk resolves only when the walk reaches it")
}

// The watermark-derived resume point waits the same way: covered
// through f+6 means f+5 is next in walk order, and this view does not
// serve it yet.
func TestQueryEvents_DescendingWatermarkAboveLatestWaits(t *testing.T) {
	r, _, f := singleChunkFixture(t) // latest = f+3
	a, err := r.NewReadView()
	require.NoError(t, err)
	defer a.Release()
	maxL := f + 8
	page, err := a.QueryEvents(context.Background(), EventCursor{
		Scope:         EventScope{MinLedger: f, MaxLedger: &maxL, Dir: Descending},
		ScannedLedger: f + 6,
	}, 10)
	require.NoError(t, err)
	assert.Empty(t, page.Events)
	assert.Equal(t, ScanWaitingForLedgers, page.Status)
	assert.Equal(t, f+6, page.Next.ScannedLedger)
}

// TestQueryEvents_DescendingFreshScopeAboveLatestWaits: a fresh
// descending scope whose MaxLedger is above this view's latest waits
// the same way. Clamping the top on page 1 would leave
// [latest+1, MaxLedger] unreachable on this cursor forever, with
// ScanComplete claiming otherwise; fresh and resumed scopes follow
// one rule, the one the proposal's Bounds section names.
func TestQueryEvents_DescendingFreshScopeAboveLatestWaits(t *testing.T) {
	r, db, f := singleChunkFixture(t) // latest = f+3
	maxL := f + 5
	d := &pageDriver{t: t, r: r, limit: 10, cursor: EventCursor{
		Scope: EventScope{MinLedger: f, MaxLedger: &maxL, Dir: Descending},
	}}
	page := d.next()
	assert.Empty(t, page.Events)
	assert.Equal(t, ScanWaitingForLedgers, page.Status)

	// The chain reaches MaxLedger: the whole scope serves, top first.
	ingestEvents(t, db, f+4, nil)
	ingestEvents(t, db, f+5, []xdr.ContractEvent{symEvent(cidA, "a4")})
	r.SetLatestLedger(f+5, CloseTimeAt(0))
	all, status := d.drain()
	assert.Equal(t, []string{"a4", "a3", "b1", "a2", "b0", "a1", "a0"}, all)
	assert.Equal(t, ScanComplete, status)
}

// TestQueryEvents_WatermarkStaysInsideWindow pins the watermark
// derivation: a mid-ledger stop never reports a value outside the
// clamped window (an earlier version minted latest+1), and a stop
// whose ledger was fully examined claims exactly that ledger.
func TestQueryEvents_WatermarkStaysInsideWindow(t *testing.T) {
	r, _, f := singleChunkFixture(t)
	maxL := f + 3

	// Descending, stop inside the very first ledger examined: nothing
	// fully covered, so the fresh cursor's zero watermark is kept.
	d := &pageDriver{
		t: t, r: r, limit: 1,
		cursor: EventCursor{Scope: EventScope{MinLedger: f, MaxLedger: &maxL, Dir: Descending}},
	}
	page := d.next()
	assert.Equal(t, []string{"a3"}, labels(t, page.Events))
	assert.Zero(t, page.Next.ScannedLedger, "no ledger fully covered; never latest+1")

	// Descending, page fills after draining all of ledger f+3: covered.
	d = &pageDriver{
		t: t, r: r, limit: 2,
		cursor: EventCursor{Scope: EventScope{MinLedger: f, MaxLedger: &maxL, Dir: Descending}},
	}
	page = d.next()
	assert.Equal(t, []string{"a3", "b1"}, labels(t, page.Events))
	assert.Equal(t, f+3, page.Next.ScannedLedger)

	// Ascending, stop inside the very first ledger examined: the next
	// match is still in ledger f, so the echo keeps the fresh cursor's
	// zero watermark. Never f-1, which would be below the query's own
	// MinLedger.
	d = &pageDriver{
		t: t, r: r, limit: 1,
		cursor: EventCursor{Scope: EventScope{MinLedger: f, MaxLedger: &maxL}},
	}
	page = d.next()
	assert.Equal(t, []string{"a0"}, labels(t, page.Events))
	assert.Zero(t, page.Next.ScannedLedger, "no ledger fully covered; never MinLedger-1")
}

// TestQueryEvents_ResumeMismatchFailsLoud pins the fail-loud resume
// contract: within-ledger order is deterministic by design, so a
// position whose claimed slot disagrees with the store is a wrong
// cursor, not something to recover from. Three disagreement shapes,
// including a ledger with a non-zero start ordinal (f+3), so the
// chunk-relative slot arithmetic is pinned too.
func TestQueryEvents_ResumeMismatchFailsLoud(t *testing.T) {
	r, _, f := singleChunkFixture(t)
	d := &pageDriver{t: t, r: r, limit: 5, cursor: EventCursor{Scope: EventScope{MinLedger: f}}}
	page := d.next()
	require.Equal(t, []string{"a0", "a1", "b0", "a2", "b1"}, labels(t, page.Events))
	require.Equal(t, uint32(0), page.Next.Position.LedgerOrdinal, "b1 opens ledger f+3")

	for name, corrupt := range map[string]func(p EventPosition) EventPosition{
		"ordinal points at a different event": func(p EventPosition) EventPosition {
			p.LedgerOrdinal = 1 // that slot holds a3, not b1
			return p
		},
		"ordinal beyond the ledger's count": func(p EventPosition) EventPosition {
			p.LedgerOrdinal = 9
			return p
		},
		"identity no event has": func(p EventPosition) EventPosition {
			p.Tx = 7
			return p
		},
	} {
		t.Run(name, func(t *testing.T) {
			bad := corrupt(*page.Next.Position)
			d2 := &pageDriver{t: t, r: r, limit: 5, cursor: EventCursor{
				Scope:         EventScope{MinLedger: f},
				Position:      &bad,
				ScannedLedger: page.Next.ScannedLedger,
			}}
			_, err := d2.tryNext()
			require.ErrorIs(t, err, ErrPositionMismatch)
		})
	}
}

func TestQueryEvents_CursorValidation(t *testing.T) {
	r, _, f := singleChunkFixture(t)
	a, err := r.NewReadView()
	require.NoError(t, err)
	defer a.Release()
	maxL := f
	maxHigh := f + 3
	minAboveMax := f + 5
	for name, tc := range map[string]struct {
		cursor  EventCursor
		limit   int
		wantErr error // the handler maps on these sentinels
	}{
		"zero limit":        {EventCursor{Scope: EventScope{MinLedger: f}}, 0, ErrInvalidLimit},
		"negative limit":    {EventCursor{Scope: EventScope{MinLedger: f}}, -3, ErrInvalidLimit},
		"descending no max": {EventCursor{Scope: EventScope{MinLedger: f, Dir: Descending}}, 1, ErrCursorMalformed},
		"invalid direction": {EventCursor{Scope: EventScope{MinLedger: f, Dir: Direction(9)}}, 1, ErrCursorMalformed},
		"min above max": {
			EventCursor{Scope: EventScope{MinLedger: minAboveMax, MaxLedger: &maxL}},
			1, ErrInvertedRange,
		},
		"min below genesis": {
			EventCursor{Scope: EventScope{MinLedger: chunk.FirstLedgerSeq - 1}},
			1, ErrCursorMalformed,
		},
		// The server mints scope and bookmarks together, so a bookmark
		// outside the scope's own bounds is forged or corrupt.
		"position below min": {EventCursor{
			Scope:    EventScope{MinLedger: f},
			Position: &EventPosition{Ledger: f - 1},
		}, 1, ErrCursorMalformed},
		"position above max": {EventCursor{
			Scope:    EventScope{MinLedger: f, MaxLedger: &maxL},
			Position: &EventPosition{Ledger: maxL + 1},
		}, 1, ErrCursorMalformed},
		"watermark above max": {EventCursor{
			Scope:         EventScope{MinLedger: f, MaxLedger: &maxL},
			ScannedLedger: maxL + 1,
		}, 1, ErrCursorMalformed},
		"watermark below min": {EventCursor{
			Scope:         EventScope{MinLedger: f},
			ScannedLedger: f - 1,
		}, 1, ErrCursorMalformed},
		// The pair must also be mintable together: a present position
		// sits exactly one ledger past the watermark in walk order (or
		// at the zero-watermark anchor), since assemblePage drops a
		// passed one. Any other pair is forged.
		"position behind watermark ascending": {EventCursor{
			Scope:         EventScope{MinLedger: f},
			Position:      &EventPosition{Ledger: f},
			ScannedLedger: f + 1,
		}, 1, ErrCursorMalformed},
		"position ahead of watermark": {EventCursor{
			Scope:         EventScope{MinLedger: f},
			Position:      &EventPosition{Ledger: f + 2},
			ScannedLedger: f,
		}, 1, ErrCursorMalformed},
		"position with zero watermark off the scope start": {EventCursor{
			Scope:    EventScope{MinLedger: f},
			Position: &EventPosition{Ledger: f + 1},
		}, 1, ErrCursorMalformed},
		"position behind watermark descending": {EventCursor{
			Scope:         EventScope{MinLedger: f, MaxLedger: &maxHigh, Dir: Descending},
			Position:      &EventPosition{Ledger: f},
			ScannedLedger: f + 2,
		}, 1, ErrCursorMalformed},
		"position with zero watermark off the scope top descending": {EventCursor{
			Scope:    EventScope{MinLedger: f, MaxLedger: &maxHigh, Dir: Descending},
			Position: &EventPosition{Ledger: f + 1},
		}, 1, ErrCursorMalformed},
		// A scope above the codec's filter cap would do the page's work
		// and then fail to encode the advanced cursor: refused up front.
		"more filters than the codec cap": {EventCursor{
			Scope: EventScope{
				MinLedger: f,
				Filters:   make([]event.Filter, maxCursorFilters+1),
			},
		}, 1, ErrCursorMalformed},
		// Malformed filters fail up front, even on a range this view
		// would not scan (beyond latest).
		"malformed filter on an unscanned range": {EventCursor{
			Scope: EventScope{
				MinLedger: f + 100,
				Filters:   []event.Filter{{ContractID: []byte{0x0a, 0x0b}}},
			},
		}, 1, ErrCursorMalformed},
		// Topic bytes reach the term hash unparsed, so a cursor naming
		// anything but one whole ScVal was forged, never minted.
		"filter topic that is not one ScVal": {EventCursor{
			Scope: EventScope{
				MinLedger: f,
				Filters:   []event.Filter{topicFilter([]byte{0xff})},
			},
		}, 1, ErrCursorMalformed},
		// A whole ScVal with one byte after it: the value parses, so only
		// the length comparison catches the extra byte.
		"filter topic with trailing bytes": {EventCursor{
			Scope: EventScope{
				MinLedger: f,
				Filters:   []event.Filter{topicFilter(append(voidScValBytes(t), 0x00))},
			},
		}, 1, ErrCursorMalformed},
		"watermark overflow sentinel": {EventCursor{
			Scope:         EventScope{MinLedger: f},
			ScannedLedger: math.MaxUint32,
		}, 1, ErrCursorMalformed},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := a.QueryEvents(context.Background(), tc.cursor, tc.limit)
			require.Error(t, err)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			}
		})
	}
}

// ─── all-dropped-batch refill (fault-injected post-filter) ───────────

// fakeEventReader is a canned event.Reader whose index bitmap can
// disagree with its payload bytes: the seam that lets a pager test
// force post-filter drops (term-hash collisions) without reaching into
// another package's store internals.
type fakeEventReader struct {
	chunkID  chunk.ID
	ofs      *event.LedgerOffsets
	payloads []event.Payload // indexed by ordinal
	bitmaps  map[event.TermKey]*roaring.Bitmap
}

func (f *fakeEventReader) ChunkID() chunk.ID { return f.chunkID }

func (f *fakeEventReader) EventCount() (uint32, error) {
	return uint32(len(f.payloads)), nil
}

func (f *fakeEventReader) Offsets() (*event.LedgerOffsets, error) { return f.ofs, nil }

func (f *fakeEventReader) LookupKeys(_ context.Context, keys []event.TermKey) ([]*roaring.Bitmap, error) {
	out := make([]*roaring.Bitmap, len(keys))
	for i, k := range keys {
		out[i] = f.bitmaps[k]
	}
	return out, nil
}

func (f *fakeEventReader) FetchEvents(_ context.Context, ids []uint32) ([]event.Payload, error) {
	out := make([]event.Payload, len(ids))
	for i, id := range ids {
		out[i] = f.payloads[id]
	}
	return out, nil
}

func (f *fakeEventReader) FetchRange(_ context.Context, start, count uint32) iter.Seq2[event.Payload, error] {
	return func(yield func(event.Payload, error) bool) {
		for i := start; i < start+count; i++ {
			if !yield(f.payloads[i], nil) {
				return
			}
		}
	}
}

func (f *fakeEventReader) All(ctx context.Context) iter.Seq2[event.Payload, error] {
	return f.FetchRange(ctx, 0, uint32(len(f.payloads)))
}

// TestEventScan_DropsDoNotStallTheChunk drives scanChunk over a part
// whose index claims ten candidates for a term but only the last one's
// bytes really match: the nine post-filter drops are invisible to the
// walk, which delivers exactly the true match and reports the part
// finished with an honest watermark.
func TestEventScan_DropsDoNotStallTheChunk(t *testing.T) {
	const c = chunk.ID(5)
	f := c.FirstLedger()
	const total = 10

	ofs := event.NewLedgerOffsets(f)
	require.NoError(t, ofs.Append(f, total))
	fake := &fakeEventReader{
		chunkID: c, ofs: ofs,
		bitmaps: map[event.TermKey]*roaring.Bitmap{},
	}
	for i := range total {
		label := fmt.Sprintf("noise%d", i)
		cid := cidB
		if i == total-1 {
			label, cid = "hit", cidA
		}
		ev := symEvent(cid, label)
		raw, err := ev.MarshalBinary()
		require.NoError(t, err)
		fake.payloads = append(fake.payloads, event.Payload{
			LedgerSequence: f, TxIdx: 1, OpIdx: 1, EventIdx: uint32(i),
			ContractEventBytes: raw,
		})
	}
	// The index claims every ordinal matches contract A; nine of them
	// are collision-style false positives the post-filter must drop.
	all := roaring.New()
	all.AddRange(0, total)
	fake.bitmaps[event.ComputeTermKey(cidA[:], event.FieldContractID)] = all

	got, err := scanChunk(context.Background(),
		eventPart{Chunk: c, Reader: fake, From: f, To: f},
		[]event.Filter{{ContractID: cidA[:]}}, nil, false, 5)
	require.NoError(t, err)
	assert.Nil(t, got.nextUnserved, "the stream ended; the page did not fill")
	assert.Equal(t, []string{"hit"}, labels(t, got.events),
		"nine dropped candidates later, exactly the true match")
	require.NotNil(t, got.last)
	assert.Equal(t, uint32(total-1), got.last.LedgerOrdinal)
	require.NotNil(t, got.coveredThrough)
	assert.Equal(t, f, *got.coveredThrough, "the finished chunk's ledger is fully covered")
}
