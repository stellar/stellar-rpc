package txhash

import (
	"encoding/binary"
	"errors"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

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

// hotIndexHarness drives a HotIndex like the ingest path will: per ledger,
// EncodeRow the hashes, feed ApplyRow, and mirror every hash into a
// reference model.
type hotIndexHarness struct {
	t    *testing.T
	h    *HotIndex
	ref  map[[32]byte][]uint32
	seen [][32]byte // pool for cross-ledger duplicate draws
	seq  uint32
	rng  *rand.Rand
}

func newHarness(t *testing.T, h *HotIndex, firstSeq uint32) *hotIndexHarness {
	return &hotIndexHarness{t: t, h: h, ref: map[[32]byte][]uint32{}, seq: firstSeq, rng: testRNG(5)}
}

// ledger applies one ledger's hashes through the production shape — EncodeRow
// then ApplyRow — and mirrors them into the reference model.
func (hh *hotIndexHarness) ledger(hashes [][32]byte) {
	hh.t.Helper()
	row, err := EncodeRow(hashes)
	require.NoError(hh.t, err)
	require.NoError(hh.t, hh.h.ApplyRow(hh.seq, row, len(hashes)))
	for _, h := range hashes {
		hh.ref[h] = append(hh.ref[h], hh.seq)
	}
	hh.seq++
}

// randomLedger applies one synthetic ledger: fresh random hashes plus a few
// re-used from earlier ledgers (fee-bump-style cross-ledger duplicates —
// legal; only the intra-ledger repeat is a reject, so reuse draws skip
// hashes already in this ledger).
func (hh *hotIndexHarness) randomLedger(fresh, reuse int) {
	hh.t.Helper()
	hashes := make([][32]byte, 0, fresh+reuse)
	inLedger := make(map[[32]byte]bool, fresh+reuse)
	for range fresh {
		h := randHash(hh.rng)
		hashes = append(hashes, h)
		inLedger[h] = true
		hh.seen = append(hh.seen, h)
	}
	for range reuse {
		if len(hh.seen) == 0 {
			break
		}
		h := hh.seen[hh.rng.IntN(len(hh.seen))]
		if inLedger[h] {
			continue
		}
		hashes = append(hashes, h)
		inLedger[h] = true
	}
	hh.ledger(hashes)
}

// verifyAll checks EVERY reference hash resolves to a containing ledger
// (exactly, when the hash lives in one), plus absent-hash misses.
func (hh *hotIndexHarness) verifyAll() {
	hh.t.Helper()
	for h, seqs := range hh.ref {
		got, err := hh.h.Get(h)
		require.NoError(hh.t, err, "hash %x", h)
		if len(seqs) == 1 {
			require.Equal(hh.t, seqs[0], got, "hash %x wrong ledger", h)
		} else {
			require.Contains(hh.t, seqs, got, "hash %x resolved outside its ledgers", h)
		}
	}
	for range 20 {
		absent := randHash(hh.rng)
		_, err := hh.h.Get(absent)
		require.ErrorIs(hh.t, err, stores.ErrNotFound, "absent hash %x must miss", absent)
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
	h.ArmSealing()  // tests emulate a validated live engine
	return h
}

// TestHotIndex_EquivalenceAcrossSeals is the engine's core gate: content
// equivalence with a reference model through window fills and many seals,
// including tx-less ledgers (empty rows) and cross-ledger duplicates.
func TestHotIndex_EquivalenceAcrossSeals(t *testing.T) {
	m := &fakeManifest{}
	h := testHotIndex(t, t.TempDir(), m)
	defer h.Close()
	hh := newHarness(t, h, 5000)

	for l := range 100 {
		if l%9 == 4 {
			hh.ledger(nil) // tx-less ledger: the dense chain's empty row
		} else {
			hh.randomLedger(hh.rng.IntN(40)+1, 2)
		}
		if l%17 == 0 {
			hh.settle()
			hh.verifyAll()
		}
	}
	hh.settle()
	hh.verifyAll()
	// No merge tier: runs only accumulate, none are ever displaced.
	assert.GreaterOrEqual(t, len(h.view.Load().runs), 2)
}

// TestHotIndex_DenseChainViolationRejected pins ApplyRow's write-time
// tripwires: the dense chain (anchored by the first row) and the
// count/bytes cross-check.
func TestHotIndex_DenseChainViolationRejected(t *testing.T) {
	m := &fakeManifest{}
	h := testHotIndex(t, t.TempDir(), m)
	defer h.Close()

	require.Error(t, h.ApplyRow(0, nil, 0), "seq 0 can never anchor the chain")
	require.NoError(t, h.ApplyRow(100, nil, 0), "first row anchors the chain")
	require.Error(t, h.ApplyRow(100, nil, 0), "repeated ledger")
	require.Error(t, h.ApplyRow(102, nil, 0), "gap")
	require.Error(t, h.ApplyRow(99, nil, 0), "regression")
	require.NoError(t, h.ApplyRow(101, nil, 0))

	row, err := EncodeRow([][32]byte{{1}})
	require.NoError(t, err)
	require.Error(t, h.ApplyRow(102, row, 2), "count/bytes mismatch must not publish")
	require.NoError(t, h.ApplyRow(102, row, 1))
}

// TestHotIndex_SealingDisarmedUntilArmed pins the validation gate every
// open starts behind: a DISARMED engine applies far more than sealEvery
// rows without writing anything durable — no run files, no manifest — and
// the first armed ApplyRow drains the whole backlog in ONE seal.
func TestHotIndex_SealingDisarmedUntilArmed(t *testing.T) {
	dir := t.TempDir()
	m := &fakeManifest{}
	h, err := NewHotIndex(dir, m)
	require.NoError(t, err)
	defer h.Close()
	h.sealEvery = 8 // NOT armed — the warmup-replay state
	hh := newHarness(t, h, 40)

	for range 40 { // 5× the seal window
		hh.randomLedger(10, 1)
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
	hh.randomLedger(10, 1)
	hh.settle()
	names, lastSealed, err = m.GetRuns()
	require.NoError(t, err)
	require.NotEmpty(t, names, "armed engine must seal the backlog")
	assert.Equal(t, hh.seq-1, lastSealed, "one seal must cover the whole backlog")
	assert.Empty(t, h.view.Load().rows, "window drained by the backlog seal")
	hh.verifyAll()
}

// TestHotIndex_WarmupRebuild: close, reopen from the manifest, replay the
// unsealed tail (validateRow + ApplyRow, the production warmup shape),
// verify full equivalence and the re-anchored dense chain.
func TestHotIndex_WarmupRebuild(t *testing.T) {
	dir := t.TempDir()
	m := &fakeManifest{}
	h := testHotIndex(t, dir, m)
	hh := newHarness(t, h, 900)

	for range 8 {
		hh.randomLedger(15, 2)
	}
	hh.settle() // one seal: ledgers 900..907
	for range 5 {
		hh.randomLedger(15, 2) // unsealed tail: 908..912
	}
	// Capture the UNSEALED tail (rows still in the window) for replay — in
	// production these come from the txhash CF rows past the sealed
	// frontier.
	tail := append([]windowRow(nil), h.view.Load().rows...)
	h.Close()

	h2, lastSealed, err := OpenHotIndex(dir, 900, m)
	require.NoError(t, err)
	defer h2.Close()
	h2.sealEvery = 8
	require.Equal(t, uint32(907), lastSealed)

	// The chain re-anchors at the frontier: a tail replay may not skip.
	require.Error(t, h2.ApplyRow(lastSealed+2, nil, 0))

	for _, row := range tail {
		n, verr := validateRow(row.bytes)
		require.NoError(t, verr)
		require.NoError(t, h2.ApplyRow(row.seq, row.bytes, n))
	}
	h2.ArmSealing() // production order: replay disarmed, verify, then arm
	hh.h = h2
	hh.verifyAll()
}

// TestHotIndex_WarmupSweepsOrphans: unreferenced run files are deleted at
// open; a manifest-referenced file that is MISSING fails loudly.
func TestHotIndex_WarmupSweepsOrphans(t *testing.T) {
	dir := t.TempDir()
	m := &fakeManifest{}
	h := testHotIndex(t, dir, m)
	hh := newHarness(t, h, 60)
	for range 8 {
		hh.randomLedger(10, 0)
	}
	hh.settle()
	h.Close()

	orphan := filepath.Join(dir, "seal-999999.run")
	require.NoError(t, os.WriteFile(orphan, []byte("junk"), 0o644))
	h2, _, err := OpenHotIndex(dir, 60, m)
	require.NoError(t, err)
	h2.Close()
	_, serr := os.Stat(orphan)
	assert.True(t, os.IsNotExist(serr), "orphan must be swept at warmup")

	// Missing referenced run: loud failure.
	names, _, _ := m.GetRuns()
	require.NotEmpty(t, names)
	require.NoError(t, os.Remove(filepath.Join(dir, names[0])))
	_, _, err = OpenHotIndex(dir, 60, m)
	require.Error(t, err, "missing manifest-referenced run must fail open")
}

// TestHotIndex_WarmupRejectsCorruptRun: a flipped payload bit and a
// truncated tail both fail the drain-verify loudly — and identically on
// retry, since a failed open writes nothing.
func TestHotIndex_WarmupRejectsCorruptRun(t *testing.T) {
	dir := t.TempDir()
	m := &fakeManifest{}
	h := testHotIndex(t, dir, m)
	hh := newHarness(t, h, 300)
	for range 8 {
		hh.randomLedger(20, 0)
	}
	hh.settle()
	h.Close()

	names, _, err := m.GetRuns()
	require.NoError(t, err)
	require.NotEmpty(t, names)
	path := filepath.Join(dir, names[0])
	raw, err := os.ReadFile(path)
	require.NoError(t, err)

	flipped := append([]byte(nil), raw...)
	flipped[runHeaderLen+5] ^= 0x01
	require.NoError(t, os.WriteFile(path, flipped, 0o644))
	_, _, err = OpenHotIndex(dir, 300, m)
	require.Error(t, err, "corrupt payload must fail the open loudly")

	require.NoError(t, os.WriteFile(path, raw[:len(raw)-runRecordLen], 0o644))
	_, _, err = OpenHotIndex(dir, 300, m)
	require.Error(t, err, "truncated payload must fail the open loudly")
}

// TestOpenHotIndex_RejectsBrokenRunChain: the manifest and the run headers
// must agree on one dense history — reordered runs and a frontier that
// disagrees with the newest run both fail the open.
func TestOpenHotIndex_RejectsBrokenRunChain(t *testing.T) {
	dir := t.TempDir()
	m := &fakeManifest{}
	h := testHotIndex(t, dir, m)
	hh := newHarness(t, h, 100)
	for range 8 {
		hh.randomLedger(5, 0)
	}
	hh.settle()
	for range 8 {
		hh.randomLedger(5, 0)
	}
	hh.settle()
	h.Close()
	names, lastSealed, err := m.GetRuns()
	require.NoError(t, err)
	require.Len(t, names, 2)

	require.NoError(t, m.PutRuns([]string{names[1], names[0]}, lastSealed))
	_, _, err = OpenHotIndex(dir, 100, m)
	require.Error(t, err, "reordered runs break the dense chain")

	require.NoError(t, m.PutRuns(names, lastSealed+3))
	_, _, err = OpenHotIndex(dir, 100, m)
	require.Error(t, err, "frontier disagreeing with the newest run is loud")

	require.NoError(t, m.PutRuns(names, lastSealed))
	h2, gotLast, err := OpenHotIndex(dir, 100, m)
	require.NoError(t, err)
	assert.Equal(t, lastSealed, gotLast)
	h2.Close()
}

// TestHotIndex_EmptyLedgersSealEmptyRun: an all-tx-less window seals to a
// count-0 run that round-trips the drain-verify at reopen — the dense chain
// exists independently of transaction volume.
func TestHotIndex_EmptyLedgersSealEmptyRun(t *testing.T) {
	dir := t.TempDir()
	m := &fakeManifest{}
	h := testHotIndex(t, dir, m)
	h.sealEvery = 4
	for seq := uint32(20); seq < 24; seq++ {
		require.NoError(t, h.ApplyRow(seq, nil, 0))
	}
	require.NoError(t, h.reapSeal(true))
	names, lastSealed, err := m.GetRuns()
	require.NoError(t, err)
	require.Len(t, names, 1)
	assert.Equal(t, uint32(23), lastSealed)
	_, err = h.Get(randHash(testRNG(1)))
	require.ErrorIs(t, err, stores.ErrNotFound)
	h.Close()

	h2, lastSealed2, err := OpenHotIndex(dir, 20, m)
	require.NoError(t, err)
	defer h2.Close()
	assert.Equal(t, uint32(23), lastSealed2)
	_, err = h2.Get(randHash(testRNG(2)))
	require.ErrorIs(t, err, stores.ErrNotFound)
}

// prefixedHash builds a hash whose first 16 bytes repeat p and whose last 4
// carry a counter — many DISTINCT hashes sharing one ladder prefix.
func prefixedHash(p byte, n uint32) [32]byte {
	var h [32]byte
	for i := range ladderKeyLen {
		h[i] = p
	}
	binary.BigEndian.PutUint32(h[28:], n)
	return h
}

// TestHotIndex_LadderBoundaryTie crafts a run whose page boundary splits a
// range of records sharing one 16-byte ladder prefix: the probe must read
// the NEXT page too when the target prefix equals its ladder key, or every
// record past the boundary would be unfindable.
func TestHotIndex_LadderBoundaryTie(t *testing.T) {
	m := &fakeManifest{}
	h := testHotIndex(t, t.TempDir(), m)
	defer h.Close()
	h.sealEvery = 1 // the crafted ledger seals into exactly one run

	// 100 hashes with prefix 0x11, then 50 with prefix 0x22: the page
	// boundary at record pageRecords (113) falls inside the 0x22 range.
	hashes := make([][32]byte, 0, 150)
	for i := range uint32(100) {
		hashes = append(hashes, prefixedHash(0x11, i))
	}
	for i := range uint32(50) {
		hashes = append(hashes, prefixedHash(0x22, i))
	}
	row, err := EncodeRow(hashes)
	require.NoError(t, err)
	require.NoError(t, h.ApplyRow(7, row, len(hashes)))
	require.NoError(t, h.reapSeal(true))

	v := h.view.Load()
	require.Empty(t, v.rows, "the crafted row must have sealed")
	require.Len(t, v.runs, 1)
	require.Len(t, v.runs[0].ladder, 2, "crafted run must span two pages")
	var tieKey [ladderKeyLen]byte
	for i := range tieKey {
		tieKey[i] = 0x22
	}
	require.Equal(t, tieKey, v.runs[0].ladder[1], "page boundary must split the equal-prefix range")

	for _, hash := range hashes {
		got, gerr := h.Get(hash)
		require.NoError(t, gerr, "hash %x", hash)
		require.Equal(t, uint32(7), got, "hash %x", hash)
	}
	_, err = h.Get(prefixedHash(0x22, 200))
	require.ErrorIs(t, err, stores.ErrNotFound)
}

// TestHotIndex_WriterReaderRace: concurrent Gets (present + absent) while
// the writer applies and seals. Run with -race.
func TestHotIndex_WriterReaderRace(t *testing.T) {
	m := &fakeManifest{}
	h := testHotIndex(t, t.TempDir(), m)
	defer h.Close()
	hh := newHarness(t, h, 700)
	known := randHash(hh.rng)
	hh.ledger([][32]byte{known})

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for r := range 3 {
		seed := uint64(r)
		wg.Go(func() {
			rng := testRNG(seed)
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, _ = h.Get(known)
				_, _ = h.Get(randHash(rng)) // absent probes exercise blooms/ladders
			}
		})
	}
	for range 60 {
		hh.randomLedger(20, 2)
	}
	hh.settle()
	close(stop)
	wg.Wait()
	hh.verifyAll()
}

// TestRocksdbManifest_RoundTrip covers the RocksDB-backed manifest the
// integration layer will hand to OpenHotIndex in step 2.
func TestRocksdbManifest_RoundTrip(t *testing.T) {
	store, err := rocksdb.New(rocksdb.Config{
		Path:           t.TempDir(),
		ColumnFamilies: CFNames(),
		Logger:         silentLogger(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	m := rocksdbManifest{store: store}

	names, lastSealed, err := m.GetRuns()
	require.NoError(t, err)
	assert.Empty(t, names)
	assert.Zero(t, lastSealed)

	require.NoError(t, m.PutRuns([]string{"seal-000000.run", "seal-000001.run"}, 42))
	names, lastSealed, err = m.GetRuns()
	require.NoError(t, err)
	assert.Equal(t, []string{"seal-000000.run", "seal-000001.run"}, names)
	assert.Equal(t, uint32(42), lastSealed)

	// A frontier with an empty run list round-trips (the engine never
	// writes it, but the codec must not conflate it with "no manifest").
	require.NoError(t, m.PutRuns(nil, 7))
	names, lastSealed, err = m.GetRuns()
	require.NoError(t, err)
	assert.Empty(t, names)
	assert.Equal(t, uint32(7), lastSealed)
}

// TestHotIndex_ManifestFailureDisposesRun pins reapSeal's manifest-failure
// branch: a run the manifest never listed is unpublished and must be closed
// and removed — the twin of the events contract.
func TestHotIndex_ManifestFailureDisposesRun(t *testing.T) {
	dir := t.TempDir()
	m := &fakeManifest{}
	h := testHotIndex(t, dir, m)
	defer h.Close()

	row, err := EncodeRow([][32]byte{{1}, {2}})
	require.NoError(t, err)
	run, err := sealWindow([]windowRow{{seq: 5, bytes: row}}, filepath.Join(dir, "seal-000009.run"))
	require.NoError(t, err)

	h.pendingSeal <- sealResult{run: run, rows: 1, lastSeq: 5}
	h.sealInFlight = true
	m.putErr = errors.New("manifest unavailable")

	err = h.reapSeal(true)
	require.ErrorContains(t, err, "hotindex manifest")
	require.ErrorIs(t, run.file.Close(), os.ErrClosed, "un-listed run's handle must be closed")
	_, serr := os.Stat(run.path)
	require.ErrorIs(t, serr, os.ErrNotExist, "un-listed run's file must be removed")
}
