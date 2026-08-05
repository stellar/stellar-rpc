package event

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/events"
)

// runsFromMap converts a per-term map to the termRuns view ApplyLedger
// takes, in the byte-sorted term order events.AppendPackedRow emits.
func runsFromMap(per map[events.TermKey][]uint32) termRuns {
	terms := make([]events.TermKey, 0, len(per))
	for k := range per {
		terms = append(terms, k)
	}
	slices.SortFunc(terms, func(a, b events.TermKey) int { return bytes.Compare(a[:], b[:]) })
	var runs termRuns
	runs.reset()
	for _, k := range terms {
		runs.addRun(k, per[k])
	}
	return runs
}

// fakeManifest is an in-memory manifestStore. putErr, when set, fails every
// PutRuns without recording anything.
type fakeManifest struct {
	mu         sync.Mutex
	names      []string
	lastSealed uint32
	putErr     error
}

func (m *fakeManifest) PutRuns(names []string, lastSealed uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.putErr != nil {
		return m.putErr
	}
	m.names = append([]string(nil), names...)
	m.lastSealed = lastSealed
	return nil
}

func (m *fakeManifest) GetRuns() ([]string, uint32, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.names...), m.lastSealed, nil
}

// hotIndexHarness drives a HotIndex like the ingest path does: per ledger,
// build perKeyIDs + the packed row, feed ApplyLedger, and mirror everything
// into a reference model.
type hotIndexHarness struct {
	t     *testing.T
	h     *HotIndex
	ref   map[events.TermKey][]uint32
	seq   uint32
	nexID uint32
	rng   *rand.Rand
}

func newHarness(t *testing.T, h *HotIndex) *hotIndexHarness {
	return &hotIndexHarness{t: t, h: h, ref: map[events.TermKey][]uint32{}, rng: rand.New(rand.NewSource(5))}
}

func hiKey(rng *rand.Rand) events.TermKey {
	var k events.TermKey
	rng.Read(k[:])
	return k
}

// ledger applies one synthetic ledger: a firehose term (every event), a few
// mid terms, and fresh singletons — the sac shape.
func (hh *hotIndexHarness) ledger(fire events.TermKey, mids []events.TermKey, eventsN int) {
	per := map[events.TermKey][]uint32{}
	for range eventsN {
		id := hh.nexID
		hh.nexID++
		per[fire] = append(per[fire], id)
		mid := mids[hh.rng.Intn(len(mids))]
		per[mid] = append(per[mid], id)
		single := hiKey(hh.rng)
		per[single] = append(per[single], id)
	}
	row := events.AppendPackedRow(nil, per)
	require.NoError(hh.t, hh.h.ApplyLedger(hh.seq, row, runsFromMap(per)))
	for k, ids := range per {
		hh.ref[k] = append(hh.ref[k], ids...)
	}
	hh.seq++
}

// verifyAll checks EVERY reference term resolves exactly, plus absent-term
// misses.
func (hh *hotIndexHarness) verifyAll() {
	hh.t.Helper()
	for k, want := range hh.ref {
		post, err := hh.h.Get(k)
		require.NoError(hh.t, err)
		require.True(hh.t, post.Present(), "term %x missing", k)
		require.Equal(hh.t, want, post.Bitmap().ToArray(), "term %x wrong ids", k)

		// An ID-backed result goes to the planner un-materialized, where
		// events.Intersect cursor-walks it and Contains binary-searches it.
		// Both need it strictly ascending, and Bitmap().ToArray() above would
		// launder a violation, so assert the slice itself.
		if ids := post.IDs(); ids != nil {
			require.Equal(hh.t, want, ids, "term %x ID slice wrong", k)
		}
	}
	for range 20 {
		absent := hiKey(hh.rng)
		post, err := hh.h.Get(absent)
		require.NoError(hh.t, err)
		assert.False(hh.t, post.Present(), "absent term %x must miss", absent)
	}
}

// settle forces any pending seal to complete and fold in.
func (hh *hotIndexHarness) settle() {
	require.NoError(hh.t, hh.h.reapSeal(true))
}

func testHotIndex(t *testing.T, dir string, m manifestStore) *HotIndex {
	h, err := NewHotIndex(dir, m)
	require.NoError(t, err)
	h.sealEvery = 8 // tiny window: many seals
	h.maxRuns = 3   // frequent merges
	h.ArmSealing()  // tests emulate a validated live engine
	return h
}

// TestHotIndex_EquivalenceAcrossSealsAndMerges is the engine's core gate:
// content equivalence with a reference model through window fills, many
// seals, and several merges, for firehose/mid/singleton term shapes.
func TestHotIndex_EquivalenceAcrossSealsAndMerges(t *testing.T) {
	m := &fakeManifest{}
	h := testHotIndex(t, t.TempDir(), m)
	defer h.Close()
	hh := newHarness(t, h)

	fire := hiKey(hh.rng)
	mids := []events.TermKey{hiKey(hh.rng), hiKey(hh.rng), hiKey(hh.rng)}
	for l := range 100 { // 12+ seals, several merges at maxRuns=3
		hh.ledger(fire, mids, 40) // fire gets 40/ledger → dense-promotes
		if l%17 == 0 {
			hh.settle()
			hh.verifyAll()
		}
	}
	hh.settle()
	hh.verifyAll()

	// The firehose term must be served by the overlay (memory-fast path).
	assert.True(t, h.overlay.Has(fire), "firehose term must have dense-promoted")
	// Live runs stayed capped.
	assert.LessOrEqual(t, len(h.view.Load().runs), 3)
}

// TestHotIndex_WarmupRebuild: close, reopen from the manifest, replay the
// unsealed tail, verify full equivalence — including the dense overlay
// self-healing on the next firehose ledger.
func TestHotIndex_WarmupRebuild(t *testing.T) {
	dir := t.TempDir()
	m := &fakeManifest{}
	h := testHotIndex(t, dir, m)
	hh := newHarness(t, h)

	fire := hiKey(hh.rng)
	mids := []events.TermKey{hiKey(hh.rng), hiKey(hh.rng)}
	tailRows := make([][]byte, 0, len(h.view.Load().rows))
	tailPer := make([]map[events.TermKey][]uint32, 0, len(h.view.Load().rows))

	for range 30 {
		hh.ledger(fire, mids, 40)
	}
	hh.settle()
	// Capture the UNSEALED tail (rows still in the window) for replay — in
	// production these come from the IndexCF packed rows past the sealed
	// frontier.
	for _, row := range h.view.Load().rows {
		tailRows = append(tailRows, row.bytes)
		per := map[events.TermKey][]uint32{}
		require.NoError(t, events.DecodePackedRow(row.bytes, func(k events.TermKey, ids []uint32) {
			per[k] = append(per[k], ids...)
		}))
		tailPer = append(tailPer, per)
	}
	h.Close()

	h2, lastSealed, err := OpenHotIndex(dir, m)
	require.NoError(t, err)
	defer h2.Close()
	h2.sealEvery, h2.maxRuns = 8, 3
	for i, row := range tailRows {
		seq := lastSealed + 1 + uint32(i)
		require.NoError(t, h2.ApplyLedger(seq, row, runsFromMap(tailPer[i])))
	}
	h2.ArmSealing() // production order: replay disarmed, verify, then arm
	hh.h = h2
	hh.verifyAll()
}

// TestHotIndex_SealingDisarmedUntilArmed pins the validation gate every
// open starts behind: a DISARMED engine applies far more than sealEvery
// rows without writing anything durable — no run files, no manifest — and
// the first armed ApplyLedger drains the whole backlog in ONE seal.
func TestHotIndex_SealingDisarmedUntilArmed(t *testing.T) {
	dir := t.TempDir()
	m := &fakeManifest{}
	h, err := NewHotIndex(dir, m)
	require.NoError(t, err)
	defer h.Close()
	h.sealEvery, h.maxRuns = 8, 3 // NOT armed — the warmup-replay state
	hh := newHarness(t, h)

	fire := hiKey(hh.rng)
	mids := []events.TermKey{hiKey(hh.rng)}
	for range 40 { // 5× the seal window
		hh.ledger(fire, mids, 20)
	}
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Empty(t, entries, "disarmed engine must write no run files")
	names, lastSealed, err := m.GetRuns()
	require.NoError(t, err)
	assert.Empty(t, names, "disarmed engine must not touch the manifest")
	assert.Zero(t, lastSealed)
	assert.Len(t, h.view.Load().rows, 40, "backlog retained in the window")
	hh.verifyAll() // reads are fully served from the window while disarmed

	// Arm + one more ledger: the over-full window seals in one pass.
	h.ArmSealing()
	hh.ledger(fire, mids, 20)
	hh.settle()
	names, lastSealed, err = m.GetRuns()
	require.NoError(t, err)
	require.NotEmpty(t, names, "armed engine must seal the backlog")
	assert.Equal(t, hh.seq-1, lastSealed, "one seal must cover the whole backlog")
	assert.Empty(t, h.view.Load().rows, "window drained by the backlog seal")
	hh.verifyAll()
}

// TestHotIndex_WarmupSweepsOrphans: unreferenced run files are deleted at
// open; a manifest-referenced file that is MISSING fails loudly.
func TestHotIndex_WarmupSweepsOrphans(t *testing.T) {
	dir := t.TempDir()
	m := &fakeManifest{}
	h := testHotIndex(t, dir, m)
	hh := newHarness(t, h)
	fire := hiKey(hh.rng)
	mids := []events.TermKey{hiKey(hh.rng)}
	for range 20 {
		hh.ledger(fire, mids, 20)
	}
	hh.settle()
	h.Close()

	orphan := filepath.Join(dir, "seal-999999.run")
	require.NoError(t, os.WriteFile(orphan, []byte("junk"), 0o644))
	h2, _, err := OpenHotIndex(dir, m)
	require.NoError(t, err)
	h2.Close()
	_, serr := os.Stat(orphan)
	assert.True(t, os.IsNotExist(serr), "orphan must be swept at warmup")

	// Missing referenced run: loud failure.
	names, _, _ := m.GetRuns()
	require.NotEmpty(t, names)
	require.NoError(t, os.Remove(filepath.Join(dir, names[0])))
	_, _, err = OpenHotIndex(dir, m)
	require.Error(t, err, "missing manifest-referenced run must fail open")
}

// TestHotIndex_WriterReaderRace: concurrent Gets (incl. held-view reads)
// while the writer applies, seals, and merges. Run with -race.
func TestHotIndex_WriterReaderRace(t *testing.T) {
	m := &fakeManifest{}
	h := testHotIndex(t, t.TempDir(), m)
	defer h.Close()
	hh := newHarness(t, h)
	fire := hiKey(hh.rng)
	mids := []events.TermKey{hiKey(hh.rng), hiKey(hh.rng)}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for r := range 3 {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for {
				select {
				case <-stop:
					return
				default:
				}
				if post, err := h.Get(fire); err == nil && post.Present() {
					_ = post.Cardinality() // full read of a held snapshot
				}
				_, _ = h.Get(hiKey(rng)) // absent probes exercise blooms/fences
			}
		}(int64(r))
	}
	for range 60 {
		hh.ledger(fire, mids, 30)
	}
	hh.settle()
	close(stop)
	wg.Wait()
	hh.verifyAll()
}

// runExtent is one record's place in a written run, reconstructed from the
// window that produced it.
type runExtent struct {
	term events.TermKey
	off  int64
	len  int64
}

func runExtents(window map[events.TermKey][]uint32) ([]runExtent, int64) {
	terms := make([]events.TermKey, 0, len(window))
	for k := range window {
		terms = append(terms, k)
	}
	slices.SortFunc(terms, func(a, b events.TermKey) int { return bytes.Compare(a[:], b[:]) })
	recs := make([]runExtent, len(terms))
	var off int64
	for i, k := range terms {
		l := int64(events.TermPostingsLen(window[k]))
		recs[i] = runExtent{term: k, off: off, len: l}
		off += l
	}
	return recs, off
}

// assertFencePolicy checks the two caps over a run's fence array: no window
// holds more than fenceEvery records, no multi-record window spans 2x
// fenceSpanBytes, and every oversized record has fences at both edges.
func assertFencePolicy(t *testing.T, fences []fence, recs []runExtent, payloadLen int64) {
	t.Helper()
	require.Equal(t, int64(0), fences[0].off)
	require.Equal(t, payloadLen, fences[len(fences)-1].off)
	for i := 0; i+1 < len(fences); i++ {
		start, end := fences[i].off, fences[i+1].off
		require.Less(t, start, end)
		inWindow := 0
		for _, r := range recs {
			if r.off >= start && r.off < end {
				inWindow++
			}
		}
		require.LessOrEqual(t, inWindow, fenceEvery)
		if end-start >= 2*fenceSpanBytes {
			require.Equal(t, 1, inWindow, "span [%d,%d) exceeds 2x cap with >1 record", start, end)
		}
	}
	for _, r := range recs {
		if r.len < fenceSpanBytes {
			continue
		}
		i := slices.IndexFunc(fences, func(x fence) bool { return x.off == r.off })
		require.GreaterOrEqual(t, i, 0, "oversized record at %d must start a window", r.off)
		require.Equal(t, r.off+r.len, fences[i+1].off, "oversized record at %d must end its window", r.off)
	}
}

// TestRunFences_ByteCapAndIsolation pins the fence policy: multi-record
// windows stay under 2x fenceSpanBytes, every record of fenceSpanBytes or
// more sits alone in its window (including one at the end of the run), the
// write pass and the warmup drain build identical fences, and lookups on and
// around the oversized records stay exact.
func TestRunFences_ByteCapAndIsolation(t *testing.T) {
	mkTerm := func(hi byte, lo uint16) events.TermKey {
		var k events.TermKey
		k[0] = hi
		binary.BigEndian.PutUint16(k[1:3], lo)
		return k
	}
	seqIDs := func(n int) []uint32 {
		ids := make([]uint32, n)
		for i := range ids {
			ids[i] = uint32(i)
		}
		return ids
	}

	// Sort order: jumboFirst < 150 small terms < exactMid < 100 medium terms
	// < jumboMid < 150 small terms < jumboEnd. Jumbo postings delta-encode
	// to ~1 byte per ID: jumboFirst encodes to EXACTLY fenceSpanBytes (the
	// inclusive boundary) and sits at record 0, whose start fence
	// pre-exists — its isolation exercises put's dedupe. The medium stretch
	// accumulates past the cap between cadence points, the byte cap's
	// ordinary trigger.
	window := map[events.TermKey][]uint32{}
	jumboFirst := mkTerm(0x00, 0)
	window[jumboFirst] = seqIDs(fenceSpanBytes - 19) // 16+3+1 framing + count-1 deltas
	for i := range 150 {
		window[mkTerm(0x10, uint16(i))] = []uint32{1, 5, 9}
		window[mkTerm(0x90, uint16(i))] = []uint32{2, 6}
	}
	// A second exactly-at-cap record whose start is mid-window (record 151:
	// off-cadence, a few hundred bytes past the last fence) — isolated only
	// by isolatePrev's retro fence, unlike jumboFirst whose start fence
	// pre-exists.
	exactMid := mkTerm(0x18, 0)
	window[exactMid] = seqIDs(fenceSpanBytes - 19)
	for i := range 100 {
		window[mkTerm(0x20, uint16(i))] = seqIDs(2500)
	}
	jumboMid := mkTerm(0x80, 0)
	jumboEnd := mkTerm(0xFF, 0xFFFF)
	window[jumboMid] = seqIDs(5 * fenceSpanBytes)
	window[jumboEnd] = seqIDs(5 * fenceSpanBytes)

	path := filepath.Join(t.TempDir(), "cap.run")
	rt := newRunRouting(len(window))
	payloadLen, err := writeSortedRun(window, path, rt.observe)
	require.NoError(t, err)
	written, err := rt.open(path, payloadLen)
	require.NoError(t, err)
	defer written.close()

	reopened, err := openSealedRun(path)
	require.NoError(t, err)
	defer reopened.close()

	// The write pass and the warmup drain must route identically — fences,
	// record count, and the bloom's size (the header count feeds it at reopen).
	require.Equal(t, written.fences, reopened.fences)
	require.Equal(t, written.terms, reopened.terms)
	require.Equal(t, written.bloom.mask, reopened.bloom.mask)

	recs, total := runExtents(window)
	require.Equal(t, payloadLen, total)
	assertFencePolicy(t, written.fences, recs, payloadLen)

	probes := []events.TermKey{
		jumboFirst, exactMid, jumboMid, jumboEnd,
		mkTerm(0x10, 149), mkTerm(0x20, 50), mkTerm(0x90, 0),
	}
	for _, k := range probes {
		ids, lerr := written.lookup(k)
		require.NoError(t, lerr)
		require.Equal(t, window[k], ids, "term %x", k)
	}
	miss, err := written.lookup(mkTerm(0x70, 7))
	require.NoError(t, err)
	require.Nil(t, miss)
}

// TestRunFences_SmallRecordsKeepRecordCadence pins that the byte cap is
// inert for ordinary shapes: with small records the fence array is exactly
// the per-fenceEvery cadence plus the end sentinel.
func TestRunFences_SmallRecordsKeepRecordCadence(t *testing.T) {
	window := map[events.TermKey][]uint32{}
	rng := rand.New(rand.NewSource(7))
	for range 200 {
		window[hiKey(rng)] = []uint32{3, 4, 8}
	}
	path := filepath.Join(t.TempDir(), "small.run")
	rt := newRunRouting(len(window))
	payloadLen, err := writeSortedRun(window, path, rt.observe)
	require.NoError(t, err)
	run, err := rt.open(path, payloadLen)
	require.NoError(t, err)
	defer run.close()

	require.Len(t, run.fences, 200/fenceEvery+2) // records 0,64,128,192 + sentinel
	recs, total := runExtents(window)
	for i, fc := range run.fences[:len(run.fences)-1] {
		require.Equal(t, recs[i*fenceEvery].off, fc.off)
		require.Equal(t, recs[i*fenceEvery].term, fc.term)
	}
	require.Equal(t, total, run.fences[len(run.fences)-1].off)
}

// TestHotIndex_MergeFailureDisposesSealedRun pins the sealResult contract:
// an errored seal job hands back no resources. The merge is failed for real
// (a live run corrupted on disk fails its CRC drain), and the freshly sealed
// run the goroutine had already produced must be closed and removed rather
// than leaked to a GC finalizer and the next warmup's sweep.
func TestHotIndex_MergeFailureDisposesSealedRun(t *testing.T) {
	dir := t.TempDir()
	m := &fakeManifest{}
	h := testHotIndex(t, dir, m) // sealEvery=8, maxRuns=3
	defer h.Close()
	hh := newHarness(t, h)

	fire := hiKey(hh.rng)
	mids := []events.TermKey{hiKey(hh.rng)}
	for range 3 { // three settled seals: the merge triggers on the fourth
		for range 8 {
			hh.ledger(fire, mids, 20)
		}
		hh.settle()
	}
	names, sealedBefore, err := m.GetRuns()
	require.NoError(t, err)
	require.Len(t, names, 3)

	// Flip the CRC trailer's last byte of one live run: structurally intact,
	// so the merge fails only at that source's drain-to-EOF verification.
	victim := filepath.Join(dir, names[0])
	blob, err := os.ReadFile(victim)
	require.NoError(t, err)
	blob[len(blob)-1] ^= 0xFF
	require.NoError(t, os.WriteFile(victim, blob, 0o644))

	for range 8 {
		hh.ledger(fire, mids, 20) // eighth ApplyLedger starts seal #4 + merge
	}
	err = h.reapSeal(true)
	require.ErrorContains(t, err, "hotindex seal")

	// The failed job's seal file is disposed of, the merge's partial output
	// cleaned up on this failure path, and durable state untouched.
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	onDisk := make([]string, 0, len(entries))
	for _, e := range entries {
		onDisk = append(onDisk, e.Name())
	}
	slices.Sort(onDisk)
	want := append([]string(nil), names...)
	slices.Sort(want)
	require.Equal(t, want, onDisk, "only the three manifest runs may remain")
	afterNames, afterSealed, err := m.GetRuns()
	require.NoError(t, err)
	require.Equal(t, names, afterNames)
	require.Equal(t, sealedBefore, afterSealed)
	require.Len(t, h.view.Load().runs, 3)
}

// mkStagedRun builds a real committed run in dir for tests that stage sealer
// hand-back state directly.
func mkStagedRun(t *testing.T, dir, name string) *sealedRun {
	t.Helper()
	var term events.TermKey
	term[0] = name[0]
	window := map[events.TermKey][]uint32{term: {1, 2}}
	path := filepath.Join(dir, name)
	rt := newRunRouting(len(window))
	payloadLen, err := writeSortedRun(window, path, rt.observe)
	require.NoError(t, err)
	run, err := rt.open(path, payloadLen)
	require.NoError(t, err)
	return run
}

// TestHotIndex_CloseClosesUnreapedSealHandbacks pins Close's drain branch: a
// successful seal result that was never reaped hands back the fresh run plus
// the merge's obsolete inputs, and Close must close every one of those
// handles. When the follow-on merge succeeded, obsolete includes the freshly
// sealed, never-published run, which the view close cannot cover.
func TestHotIndex_CloseClosesUnreapedSealHandbacks(t *testing.T) {
	dir := t.TempDir()
	h := testHotIndex(t, dir, &fakeManifest{})

	merged := mkStagedRun(t, dir, "merge-000001.run")
	sealed := mkStagedRun(t, dir, "seal-000001.run")

	// The state startSeal's goroutine leaves behind when its merge completed
	// but reapSeal never ran: result pending, sealInFlight still set.
	h.pendingSeal <- sealResult{run: merged, obsolete: []*sealedRun{sealed}, replaceAll: true}
	h.sealInFlight = true
	h.Close()

	require.ErrorIs(t, merged.file.Close(), os.ErrClosed, "drained result's run must be closed")
	require.ErrorIs(t, sealed.file.Close(), os.ErrClosed, "drained result's obsolete handles must be closed")
}

// TestHotIndex_ManifestFailureDisposesRun pins reapSeal's manifest-failure
// branch: a run the manifest never listed is unpublished and must be closed
// and removed, while the obsolete inputs (still live in the view, still
// manifest-listed) stay open.
func TestHotIndex_ManifestFailureDisposesRun(t *testing.T) {
	dir := t.TempDir()
	m := &fakeManifest{}
	h := testHotIndex(t, dir, m)
	defer h.Close()

	merged := mkStagedRun(t, dir, "merge-000002.run")
	obsolete := mkStagedRun(t, dir, "seal-000002.run")
	h.pendingSeal <- sealResult{run: merged, obsolete: []*sealedRun{obsolete}, replaceAll: true}
	h.sealInFlight = true
	m.putErr = errors.New("manifest unavailable")

	err := h.reapSeal(true)
	require.ErrorContains(t, err, "hotindex manifest")
	require.ErrorIs(t, merged.file.Close(), os.ErrClosed, "un-listed run's handle must be closed")
	_, serr := os.Stat(merged.path)
	require.ErrorIs(t, serr, os.ErrNotExist, "un-listed run's file must be removed")
	require.NoError(t, obsolete.file.Close(), "obsolete inputs must stay open for live readers")
	_, serr = os.Stat(obsolete.path)
	require.NoError(t, serr, "obsolete inputs' files must remain")
}
