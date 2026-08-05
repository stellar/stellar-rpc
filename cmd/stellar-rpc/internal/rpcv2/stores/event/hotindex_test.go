package event

import (
	"bytes"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// runsFromMap converts a per-term map to the termRuns view ApplyLedger
// takes, in the byte-sorted term order AppendPackedRow emits.
func runsFromMap(per map[TermKey][]uint32) termRuns {
	terms := make([]TermKey, 0, len(per))
	for k := range per {
		terms = append(terms, k)
	}
	slices.SortFunc(terms, func(a, b TermKey) int { return bytes.Compare(a[:], b[:]) })
	var runs termRuns
	runs.reset()
	for _, k := range terms {
		runs.addRun(k, per[k])
	}
	return runs
}

// fakeManifest is an in-memory manifestStore.
type fakeManifest struct {
	mu         sync.Mutex
	names      []string
	lastSealed uint32
}

func (m *fakeManifest) PutRuns(names []string, lastSealed uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
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
	ref   map[TermKey][]uint32
	seq   uint32
	nexID uint32
	rng   *rand.Rand
}

func newHarness(t *testing.T, h *HotIndex) *hotIndexHarness {
	return &hotIndexHarness{t: t, h: h, ref: map[TermKey][]uint32{}, rng: rand.New(rand.NewSource(5))}
}

func hiKey(rng *rand.Rand) TermKey {
	var k TermKey
	rng.Read(k[:])
	return k
}

// ledger applies one synthetic ledger: a firehose term (every event), a few
// mid terms, and fresh singletons — the sac shape.
func (hh *hotIndexHarness) ledger(fire TermKey, mids []TermKey, eventsN int) {
	per := map[TermKey][]uint32{}
	for range eventsN {
		id := hh.nexID
		hh.nexID++
		per[fire] = append(per[fire], id)
		mid := mids[hh.rng.Intn(len(mids))]
		per[mid] = append(per[mid], id)
		single := hiKey(hh.rng)
		per[single] = append(per[single], id)
	}
	row := AppendPackedRow(nil, per)
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
	mids := []TermKey{hiKey(hh.rng), hiKey(hh.rng), hiKey(hh.rng)}
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
	mids := []TermKey{hiKey(hh.rng), hiKey(hh.rng)}
	tailRows := make([][]byte, 0, len(h.view.Load().rows))
	tailPer := make([]map[TermKey][]uint32, 0, len(h.view.Load().rows))

	for range 30 {
		hh.ledger(fire, mids, 40)
	}
	hh.settle()
	// Capture the UNSEALED tail (rows still in the window) for replay — in
	// production these come from the IndexCF packed rows past the sealed
	// frontier.
	for _, row := range h.view.Load().rows {
		tailRows = append(tailRows, row.bytes)
		per := map[TermKey][]uint32{}
		require.NoError(t, DecodePackedRow(row.bytes, func(k TermKey, ids []uint32) {
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
	mids := []TermKey{hiKey(hh.rng)}
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
	mids := []TermKey{hiKey(hh.rng)}
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
	mids := []TermKey{hiKey(hh.rng), hiKey(hh.rng)}

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
