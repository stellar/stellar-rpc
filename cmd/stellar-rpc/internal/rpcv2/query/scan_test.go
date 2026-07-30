package query

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rpcv2test"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/hotchunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/ledger"
)

// seedHotLedgers opens chunk c's hot DB, commits the given (contiguous) seqs,
// marks the chunk ready, and publishes the handle.
func seedHotLedgers(t *testing.T, cat *catalog.Catalog, r *Registry, c chunk.ID, seqs ...uint32) {
	t.Helper()
	db, err := hotchunk.Open(cat.Layout().HotChunkPath(c), c, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	for _, seq := range seqs {
		_, err = db.IngestLedger(seq, rpcv2test.ZeroTxLCMBytes(t, seq))
		require.NoError(t, err)
	}
	require.NoError(t, cat.FlipHotReady(c))
	r.PublishHandle(c, db)
}

// borderFixture seeds two adjacent hot chunks, each with its first two ledgers
// committed (the events CF requires ingestion to start at the chunk's first
// ledger). latest is pinned to c1's FIRST ledger — one below c1's last committed
// one — so clamping at latest is distinguishable from data simply running out.
func borderFixture(t *testing.T) (*Registry, chunk.ID, chunk.ID) {
	t.Helper()
	cat := openTestCatalog(t, silentLogger())
	r := NewRegistry(cat, geometry.NewRetention(0, 0))
	const c0, c1 = chunk.ID(5), chunk.ID(6)
	seedHotLedgers(t, cat, r, c0, c0.FirstLedger(), c0.FirstLedger()+1)
	seedHotLedgers(t, cat, r, c1, c1.FirstLedger(), c1.FirstLedger()+1)
	r.SetLatestLedger(c1.FirstLedger())
	return r, c0, c1
}

func collectSeqs(t *testing.T, r *Registry, lo, hi uint32) []uint32 {
	t.Helper()
	a, err := r.NewReadView()
	require.NoError(t, err)
	defer a.Release()
	scan, err := a.ScanLedgers(lo, hi)
	require.NoError(t, err)
	var seqs []uint32
	for e, err := range scan {
		require.NoError(t, err)
		seqs = append(seqs, e.Seq)
	}
	return seqs
}

// TestScanLedgers_CrossesChunkBorder pins the flat iterator across a chunk
// border: one ascending sequence, no seam visible to the caller.
func TestScanLedgers_CrossesChunkBorder(t *testing.T) {
	r, c0, c1 := borderFixture(t)
	got := collectSeqs(t, r, c0.FirstLedger(), c1.FirstLedger())
	assert.Equal(t, []uint32{
		c0.FirstLedger(), c0.FirstLedger() + 1, c1.FirstLedger(),
	}, got, "the border chunk's entries follow the first chunk's, flat")
}

// TestScanLedgers_ClampsAtLatest pins that the internal clamp truncates the
// trailing edge: c1's FirstLedger()+1 is committed and would be yielded, but
// latest sits one below it, so a caller cannot iterate past the view's latestLedger.
func TestScanLedgers_ClampsAtLatest(t *testing.T) {
	r, c0, c1 := borderFixture(t)
	got := collectSeqs(t, r, c0.FirstLedger()+1, c1.FirstLedger()+500)
	assert.Equal(t, []uint32{
		c0.FirstLedger() + 1, c1.FirstLedger(),
	}, got, "entries stop at latest, not at the requested hi or the committed data")
}

// TestScanLedgers_BeyondLatestIsEmpty pins the valid-empty case: a range
// entirely above latest scans nothing and errors nothing.
func TestScanLedgers_BeyondLatestIsEmpty(t *testing.T) {
	r, _, c1 := borderFixture(t)
	got := collectSeqs(t, r, c1.FirstLedger()+100, c1.FirstLedger()+200)
	assert.Empty(t, got)
}

// TestScanLedgers_UnroutableChunkFailsUpFront pins that a chunk with no serving
// store fails at ScanLedgers, not mid-stream: the readers are resolved eagerly.
func TestScanLedgers_UnroutableChunkFailsUpFront(t *testing.T) {
	cat := openTestCatalog(t, silentLogger())
	r := NewRegistry(cat, geometry.NewRetention(0, 0))
	const c1 = chunk.ID(6)
	seedHotLedgers(t, cat, r, c1, c1.FirstLedger())
	r.SetLatestLedger(c1.FirstLedger())

	a, err := r.NewReadView()
	require.NoError(t, err)
	defer a.Release()

	// The range starts in chunk 5, which has no store at all.
	_, err = a.ScanLedgers(chunk.ID(5).LastLedger(), c1.FirstLedger())
	require.ErrorIs(t, err, ErrUnavailable)
}

// TestScanLedgers_InvertedInputRejected pins that the internal clamp surfaces
// malformed input as ErrInvertedRange, same as calling ClampRange directly.
func TestScanLedgers_InvertedInputRejected(t *testing.T) {
	r, c0, _ := borderFixture(t)
	a, err := r.NewReadView()
	require.NoError(t, err)
	defer a.Release()
	_, err = a.ScanLedgers(c0.FirstLedger()+1, c0.FirstLedger())
	require.ErrorIs(t, err, ErrInvertedRange)
}

// TestEventParts pins the per-chunk parts shape the events engine consumes:
// scan order, intersected bounds, one part per overlapping chunk.
func TestEventParts(t *testing.T) {
	r, c0, c1 := borderFixture(t)
	a, err := r.NewReadView()
	require.NoError(t, err)
	defer a.Release()

	lo, hi := c0.FirstLedger()+1, c1.FirstLedger()

	t.Run("ascending two parts with intersected bounds", func(t *testing.T) {
		parts, err := a.EventParts(Ascending, lo, hi)
		require.NoError(t, err)
		require.Len(t, parts, 2)
		assert.Equal(t, c0, parts[0].Chunk)
		assert.Equal(t, lo, parts[0].From)
		assert.Equal(t, c0.LastLedger(), parts[0].To, "first part ends at its chunk border")
		assert.Equal(t, c1, parts[1].Chunk)
		assert.Equal(t, c1.FirstLedger(), parts[1].From, "second part starts at its chunk border")
		assert.Equal(t, hi, parts[1].To)
		assert.NotNil(t, parts[0].Reader)
		assert.NotNil(t, parts[1].Reader)
	})

	t.Run("descending reverses the parts, not the bounds", func(t *testing.T) {
		parts, err := a.EventParts(Descending, lo, hi)
		require.NoError(t, err)
		require.Len(t, parts, 2)
		assert.Equal(t, c1, parts[0].Chunk)
		assert.Equal(t, c0, parts[1].Chunk)
		assert.Equal(t, lo, parts[1].From, "bounds stay ascending within a part")
	})

	t.Run("single chunk yields one part", func(t *testing.T) {
		parts, err := a.EventParts(Ascending, c0.FirstLedger(), c0.FirstLedger()+1)
		require.NoError(t, err)
		require.Len(t, parts, 1)
		assert.Equal(t, c0, parts[0].Chunk)
	})

	t.Run("beyond latest yields no parts", func(t *testing.T) {
		parts, err := a.EventParts(Ascending, c1.FirstLedger()+100, c1.FirstLedger()+200)
		require.NoError(t, err)
		assert.Empty(t, parts)
	})
}

// TestScanLedgers_ColdHotBorder pins the cold-to-hot border at the scan level,
// by payload bytes: chunk c0 is BOTH frozen-cold and ready-hot (an empty hot DB
// is published for it), so the marker bytes written only to the cold pack prove
// cold-wins tier selection; c1's bytes round-trip the hot store. A border read
// served from the wrong tier cannot pass.
func TestScanLedgers_ColdHotBorder(t *testing.T) {
	cat := openTestCatalog(t, silentLogger())
	r := NewRegistry(cat, geometry.NewRetention(0, 0))
	const c0, c1 = chunk.ID(5), chunk.ID(6)

	// c0's cold pack: two marker payloads at the chunk's last two ledgers. The
	// pack covers only what the scan requests; AppendLedger stores raw bytes.
	packPath := cat.Layout().LedgerPackPath(c0)
	require.NoError(t, os.MkdirAll(filepath.Dir(packPath), 0o755))
	cw, err := ledger.NewColdWriter(packPath, c0.LastLedger()-1, ledger.ColdWriterOptions{})
	require.NoError(t, err)
	coldA, coldB := []byte("cold-payload-a"), []byte("cold-payload-b")
	require.NoError(t, cw.AppendLedger(c0.LastLedger()-1, coldA))
	require.NoError(t, cw.AppendLedger(c0.LastLedger(), coldB))
	require.NoError(t, cw.Commit())
	require.NoError(t, cat.FlipChunkFrozen(c0, geometry.KindLedgers))

	// c0 is ALSO ready-hot with a published (empty) handle: cold must win.
	emptyHot, err := hotchunk.Open(cat.Layout().HotChunkPath(c0), c0, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = emptyHot.Close() })
	require.NoError(t, cat.FlipHotReady(c0))
	r.PublishHandle(c0, emptyHot)

	// c1 is hot with real committed ledgers.
	seedHotLedgers(t, cat, r, c1, c1.FirstLedger(), c1.FirstLedger()+1)
	r.SetLatestLedger(c1.FirstLedger() + 1)

	a, err := r.NewReadView()
	require.NoError(t, err)
	defer a.Release()

	scan, err := a.ScanLedgers(c0.LastLedger()-1, c1.FirstLedger()+1)
	require.NoError(t, err)
	var seqs []uint32
	var payloads [][]byte
	for e, ierr := range scan {
		require.NoError(t, ierr)
		seqs = append(seqs, e.Seq)
		payloads = append(payloads, append([]byte(nil), e.Bytes...)) // Bytes is borrowed
	}

	require.Equal(t, []uint32{
		c0.LastLedger() - 1, c0.LastLedger(), c1.FirstLedger(), c1.FirstLedger() + 1,
	}, seqs, "the flat walk crosses the cold-to-hot border")
	assert.Equal(t, coldA, payloads[0], "c0 served from the cold pack (cold-wins), not the empty hot DB")
	assert.Equal(t, coldB, payloads[1])
	assert.Equal(t, rpcv2test.ZeroTxLCMBytes(t, c1.FirstLedger()), payloads[2], "c1 round-trips the hot store")
	assert.Equal(t, rpcv2test.ZeroTxLCMBytes(t, c1.FirstLedger()+1), payloads[3])
}
