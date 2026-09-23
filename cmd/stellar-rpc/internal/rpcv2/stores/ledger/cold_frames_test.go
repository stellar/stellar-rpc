package ledger

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sdkingest "github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/packfile"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/txspan"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/zstd"
)

// testColdPassphrase is the network the framed fixtures below hash their
// envelopes under, on both the hot and the cold side — the two must agree or
// the tables differ and the packs stop matching.
const testColdPassphrase = network.TestNetworkPassphrase

// coldFrameWindow is the window these tests cut frames at. Real ledgers past
// FrameWindow are megabytes; lowering the window reproduces the same shape
// from fixtures a test can build, and every other part of the path — the
// directory, the compressed offsets, the reads — is the production one.
const coldFrameWindow = 1024

// framedLedger returns a real LedgerCloseMeta large enough to be cut into
// several frames at coldFrameWindow.
func framedLedger(t *testing.T, seq uint32) []byte {
	t.Helper()
	return framedLedgerOf(t, seq, 48)
}

// framedLedgerOf is framedLedger with the transaction count chosen, so a test
// can put records of visibly different table sizes in one pack.
func framedLedgerOf(t *testing.T, seq uint32, txs int) []byte {
	t.Helper()
	lcm, _ := makeRandomLedgerCloseMeta(seq, txs)
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	require.Greater(t, len(raw), 4*coldFrameWindow, "the fixture must span several frames")
	return raw
}

// writeFramedPack writes a walk-mode pack holding lcms from firstSeq and
// returns its path.
func writeFramedPack(t *testing.T, firstSeq uint32, passphrase string, lcms ...[]byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "framed.pack")
	w, err := NewColdWriter(path, firstSeq, ColdWriterOptions{Passphrase: passphrase})
	require.NoError(t, err)
	for i, raw := range lcms {
		require.NoError(t, w.AppendLedger(firstSeq+uint32(i), raw))
	}
	require.NoError(t, w.Commit())
	require.NoError(t, w.Close())
	return path
}

// TestColdWithTxTable_PiecesEqualTheRawSpans is the cold read path's core
// claim: for every transaction of a framed record, the bytes read out of the
// covering frames are exactly the bytes the same spans name in the ledger.
func TestColdWithTxTable_PiecesEqualTheRawSpans(t *testing.T) {
	withFrameWindow(t, coldFrameWindow)
	const first = 4_000
	raw := framedLedger(t, first)
	path := writeFramedPack(t, first, testColdPassphrase, raw)

	r := newTestColdReader(t, path)
	straddled := 0
	require.NoError(t, r.WithTxTable(first,
		func(tbl txspan.Table, _ txspan.LedgerHeader, pieces txspan.PieceReader) error {
			require.Greater(t, tbl.FrameCount(), 4, "the record must hold several frames")
			assert.Equal(t, uint32(first), tbl.LedgerSeq())
			assert.Equal(t, uint32(len(raw)), tbl.RawSize())
			for i := range tbl.TxCount() {
				row := tbl.Row(i)
				env, elem, perr := pieces(row)
				require.NoError(t, perr)
				assert.Equal(t, raw[row.EnvStart:row.EnvEnd], env, "envelope %d", i)
				assert.Equal(t, raw[row.ElemStart:row.ElemEnd], elem, "element %d", i)
				lo, _, _, _ := tbl.RawOffsetFrame(row.EnvStart)
				hi, _, _, _ := tbl.RawOffsetFrame(row.EnvEnd - 1)
				if lo != hi {
					straddled++
				}
			}
			return nil
		}))
	assert.Positive(t, straddled, "no span crossed a cut; the fixture proves nothing")

	// The same record still reads whole, which is what every range query does.
	require.NoError(t, r.WithLedger(first, func(got []byte) error {
		assert.Equal(t, raw, got)
		return nil
	}))
}

// TestColdExtentTouches pins the rule that decides one read from two: extents
// that overlap or abut are read as their union, and extents with even one byte
// between them are not — that byte is a frame this row has no use for, and a
// ledger is megabytes of them.
func TestColdExtentTouches(t *testing.T) {
	base := coldExtent{compStart: 100, compEnd: 200}
	for name, tc := range map[string]struct {
		other coldExtent
		want  bool
	}{
		"abutting after":  {coldExtent{compStart: 200, compEnd: 300}, true},
		"abutting before": {coldExtent{compStart: 40, compEnd: 100}, true},
		"overlapping":     {coldExtent{compStart: 150, compEnd: 300}, true},
		"contained":       {coldExtent{compStart: 120, compEnd: 180}, true},
		"a byte apart":    {coldExtent{compStart: 201, compEnd: 300}, false},
		"far apart":       {coldExtent{compStart: 9_000, compEnd: 9_100}, false},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, base.touches(tc.other))
			assert.Equal(t, tc.want, tc.other.touches(base), "touching is symmetric")
		})
	}

	joined := base.union(coldExtent{compStart: 200, compEnd: 300, rawStart: 7, rawEnd: 9})
	assert.EqualValues(t, 100, joined.compStart)
	assert.EqualValues(t, 300, joined.compEnd)
}

// TestColdWithTxTable_PiecesFromRunsThatTouchAndRunsApart drives the piece
// reader over rows whose two spans sit in one frame, in abutting frames, and
// in frames far apart — the three ways the reader resolves a row, of which
// only the last reads twice. Whichever way it went, the bytes are the raw
// ledger's and BOTH slices are still right after the second was produced,
// which is the claim a shared decode buffer would break.
func TestColdWithTxTable_PiecesFromRunsThatTouchAndRunsApart(t *testing.T) {
	withFrameWindow(t, coldFrameWindow)
	const first = 4_150
	raw := framedLedger(t, first)
	r := newTestColdReader(t, writeFramedPack(t, first, testColdPassphrase, raw))

	require.NoError(t, r.WithTxTable(first,
		func(tbl txspan.Table, _ txspan.LedgerHeader, pieces txspan.PieceReader) error {
			starts := frameRawStarts(tbl)
			require.Greater(t, len(starts), 5, "the fixture must hold several frames")
			for name, row := range map[string]txspan.Row{
				"one frame": {
					EnvStart: starts[1], EnvEnd: starts[1] + 8,
					ElemStart: starts[1] + 16, ElemEnd: starts[1] + 24,
				},
				"abutting frames": {
					EnvStart: starts[2] - 4, EnvEnd: starts[2],
					ElemStart: starts[2], ElemEnd: starts[2] + 8,
				},
				"frames apart": {
					EnvStart: starts[1], EnvEnd: starts[1] + 8,
					ElemStart: starts[4], ElemEnd: starts[4] + 8,
				},
			} {
				t.Run(name, func(t *testing.T) {
					env, elem, perr := pieces(row)
					require.NoError(t, perr)
					assert.Equal(t, raw[row.ElemStart:row.ElemEnd], elem, "element")
					assert.Equal(t, raw[row.EnvStart:row.EnvEnd], env,
						"the envelope must survive the element's decode")
				})
			}
			return nil
		}))
}

// frameRawStarts is the raw offset each of a table's frames begins at.
func frameRawStarts(tbl txspan.Table) []uint32 {
	starts := make([]uint32, tbl.FrameCount())
	var at uint32
	for i := range starts {
		starts[i] = at
		at += tbl.Frame(i).Raw
	}
	return starts
}

// TestColdWithTxTable_UnframedRecordHasNoTable pins the layout's floor: a
// ledger inside the window is stored as the single frame it always was, with
// no table in the record, so its lookups are walk-served.
func TestColdWithTxTable_UnframedRecordHasNoTable(t *testing.T) {
	withFrameWindow(t, coldFrameWindow)
	const first = 4_100
	small := zeroTxLedger(t, first)
	require.Less(t, len(small), coldFrameWindow)
	path := writeFramedPack(t, first, testColdPassphrase, small)

	r := newTestColdReader(t, path)
	require.ErrorIs(t, r.WithTxTable(first,
		func(txspan.Table, txspan.LedgerHeader, txspan.PieceReader) error { return nil }),
		stores.ErrNoTable)
	require.NoError(t, r.WithLedger(first, func(got []byte) error {
		assert.Equal(t, small, got)
		return nil
	}))
}

// TestColdWriter_UnframedRecordsAreThePreFramesBytes pins the byte-identity
// claim for every pack whose ledgers fit the window: tables changed nothing
// about such a record, because it carries none — its bytes are still the
// single zstd frame the writer produced before frames existed, and the pack's
// app data says so.
func TestColdWriter_UnframedRecordsAreThePreFramesBytes(t *testing.T) {
	withFrameWindow(t, coldFrameWindow)
	const first = 4_200
	lcms := [][]byte{zeroTxLedger(t, first), zeroTxLedger(t, first+1)}

	r := newTestColdReader(t, writeFramedPack(t, first, testColdPassphrase, lcms...))
	h, err := r.init()
	require.NoError(t, err)
	assert.False(t, h.tables, "no record carries a table, and the app data says so")
	assert.Zero(t, h.maxFront, "a pack with no tables records no front")

	comp := zstd.NewCompressor()
	t.Cleanup(func() { _ = comp.Close() })
	for i, raw := range lcms {
		at, size, rerr := r.r.RecordRange(i)
		require.NoError(t, rerr)
		record := make([]byte, size)
		require.NoError(t, r.r.ReadAt(record, at))
		want, cerr := comp.Encode(nil, raw)
		require.NoError(t, cerr)
		assert.Equal(t, want, record, "record %d is not the plain single-frame encode", i)
	}
}

// TestVerifyPack_AcceptsAFramedPack pins the artifact verifier's happy path:
// every table parses, its directory matches the frames on disk, and the
// sampled rows' elements carry hashes the table routes to them.
func TestVerifyPack_AcceptsAFramedPack(t *testing.T) {
	withFrameWindow(t, coldFrameWindow)
	const first = 4_400
	lcms := [][]byte{framedLedger(t, first), zeroTxLedger(t, first+1), framedLedger(t, first+2)}
	path := writeFramedPack(t, first, testColdPassphrase, lcms...)

	tabled, err := VerifyPack(path)
	require.NoError(t, err)
	assert.Equal(t, 2, tabled, "only the framed records carry a table")
}

// TestVerifyPack_CatchesADriftedDirectory pins the failure the verifier
// exists for: a directory entry that no longer describes the frames stored
// beside it is reported, naming the ledger — and a lookup through that table
// errors rather than slicing the wrong bytes.
func TestVerifyPack_CatchesADriftedDirectory(t *testing.T) {
	withFrameWindow(t, coldFrameWindow)
	const first = 4_500
	raw := framedLedger(t, first)
	path := writeFramedPack(t, first, testColdPassphrase, raw)
	driftOneFrameSize(t, path)

	_, err := VerifyPack(path)
	require.ErrorIs(t, err, stores.ErrCorrupt)
	require.ErrorContains(t, err, "4500")

	r := newTestColdReader(t, path)
	lookupErr := r.WithTxTable(first, func(tbl txspan.Table, _ txspan.LedgerHeader, pieces txspan.PieceReader) error {
		// Every row is read: a drifted directory must be caught, not silently
		// answered with neighboring bytes.
		for i := range tbl.TxCount() {
			if _, _, perr := pieces(tbl.Row(i)); perr != nil {
				return perr
			}
		}
		return nil
	})
	require.ErrorIs(t, lookupErr, stores.ErrCorrupt)
	require.ErrorContains(t, lookupErr, "4500")
}

// TestVerifyPack_NamesTheRecordWhoseTableFailedItsChecksum pins the
// attribution the digest replay cannot give. A table edited under its own
// checksum is reported as corruption of THAT record, with the reason, where it
// is read — the digest would disagree at the end and name neither.
func TestVerifyPack_NamesTheRecordWhoseTableFailedItsChecksum(t *testing.T) {
	withFrameWindow(t, coldFrameWindow)
	const first = 4_600
	raw := framedLedger(t, first)
	path := writeFramedPack(t, first, testColdPassphrase, raw)
	corruptFirstTable(t, path)

	_, err := VerifyPack(path)
	require.ErrorIs(t, err, stores.ErrCorrupt)
	assert.ErrorContains(t, err, "4600", "the failure must name the record")
	assert.ErrorContains(t, err, path, "the failure must name the pack")
	assert.ErrorContains(t, err, "checksum mismatch", "the failure must give the reason")

	// A serving read of the same record fails the same way, and says the same
	// three things — the pack is a bad artifact whichever read finds it.
	r := newTestColdReader(t, path)
	serveErr := r.WithTxTable(first, func(txspan.Table, txspan.LedgerHeader, txspan.PieceReader) error {
		t.Fatal("fn must not run for a table that will not parse")
		return nil
	})
	require.ErrorIs(t, serveErr, stores.ErrCorrupt)
	require.NotErrorIs(t, serveErr, stores.ErrNoTable, "a bad table is not an absent one")
	assert.ErrorContains(t, serveErr, "4600")
	assert.ErrorContains(t, serveErr, "checksum mismatch")
}

// corruptFirstTable flips one byte of the pack's first record's span table,
// leaving every length alone, so the table fails its own CRC inside a record
// that is otherwise exactly what the writer wrote.
func corruptFirstTable(t *testing.T, path string) {
	t.Helper()
	record, at := firstRecord(t, path)
	payload, _, err := zstd.SkippablePayload(record)
	require.NoError(t, err)
	flipped := bytes.Clone(payload)
	flipped[len(flipped)-1] ^= 0x01

	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	_, err = f.WriteAt(flipped, at+8) // past the skippable frame's header
	require.NoError(t, err)
}

// driftOneFrameSize rewrites the pack's first record so its table claims a
// frame layout the stored frames do not have, with a checksum that still
// validates — the corruption a checksum alone cannot catch.
func driftOneFrameSize(t *testing.T, path string) {
	t.Helper()
	record, at := firstRecord(t, path)
	payload, _, err := zstd.SkippablePayload(record)
	require.NoError(t, err)
	table, err := txspan.Parse(payload)
	require.NoError(t, err)

	frames := make([]txspan.Frame, table.FrameCount())
	for i := range frames {
		frames[i] = table.Frame(i)
	}
	// Move a byte from one frame to the next: the total is unchanged, so the
	// drift shows only against the frames themselves.
	frames[0].Compressed--
	frames[1].Compressed++
	drifted, err := txspan.WithFrames(payload, frames)
	require.NoError(t, err)
	require.Len(t, drifted, len(payload), "the rewrite must keep the record's length")

	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	_, err = f.WriteAt(drifted, at+8) // past the skippable frame's header
	require.NoError(t, err)
}

// firstRecord reads the pack's first record and returns it with its file
// offset.
func firstRecord(t *testing.T, path string) ([]byte, int64) {
	t.Helper()
	r := newTestColdReader(t, path)
	_, err := r.init()
	require.NoError(t, err)
	at, size, err := r.r.RecordRange(0)
	require.NoError(t, err)
	buf := make([]byte, size)
	require.NoError(t, r.r.ReadAt(buf, at))
	return buf, at
}

// TestFreezeColdFromStore_FramedLedgersMatchTheWalk is the identity gate for
// the shape this layout exists for: a chunk whose big ledgers are framed and
// carry span tables must freeze to the same bytes the walk materializer
// writes from the same ledgers, tables included.
func TestFreezeColdFromStore_FramedLedgersMatchTheWalk(t *testing.T) {
	withFrameWindow(t, coldFrameWindow)
	chunkID := chunk.ID(0)
	first, last := chunkID.FirstLedger(), chunkID.LastLedger()
	// A handful of framed, table-bearing ledgers among a chunk of small ones:
	// enough to exercise both record shapes without marshaling ten thousand
	// dense ledgers.
	// The framed fixtures are built ONCE: their source accounts are random, so
	// rebuilding one would hand the two materializers different ledgers.
	framedAt := map[uint32][]byte{}
	for _, seq := range []uint32{first, first + 1, first + 5_000, last} {
		framedAt[seq] = framedLedger(t, seq)
	}
	payload := func(seq uint32) []byte {
		if raw, ok := framedAt[seq]; ok {
			return raw
		}
		return zeroTxLedger(t, seq)
	}

	walkPath := filepath.Join(t.TempDir(), "walk.pack")
	w, err := NewColdWriter(walkPath, first, ColdWriterOptions{Passphrase: testColdPassphrase})
	require.NoError(t, err)
	for seq := first; seq <= last; seq++ {
		require.NoError(t, w.AppendLedger(seq, payload(seq)))
	}
	require.NoError(t, w.Commit())
	require.NoError(t, w.Close())

	h, store := openTestHotStoreAt(t, t.TempDir())
	populateTabledChunk(t, h, chunkID, payload)
	freezePath := filepath.Join(t.TempDir(), "freeze.pack")
	n, err := FreezeColdFromStore(context.Background(), chunkID, store, freezePath, ColdWriterOptions{})
	require.NoError(t, err)
	require.EqualValues(t, chunk.LedgersPerChunk, n)

	walkBytes, err := os.ReadFile(walkPath)
	require.NoError(t, err)
	freezeBytes, err := os.ReadFile(freezePath)
	require.NoError(t, err)
	require.Len(t, freezeBytes, len(walkBytes), "pack sizes diverge")
	require.True(t, bytes.Equal(walkBytes, freezeBytes), "pack bytes diverge")

	// Byte identity is the strong claim; the CONTENT hash is the one that
	// outlives it. It is taken over RAW LCM bytes, so a tabled, framed record
	// must contribute exactly the ledger and never the table's skippable
	// frame — on the walk, which hashes the raw bytes it was handed, and on
	// the freeze, whose ContentHashExtract decodes the WHOLE record back to
	// those same bytes: the multi-frame decoder walks past the leading
	// skippable frame without contributing any, so table bytes never enter
	// the hash. Verify recomputes the hash from the stored items, so it is
	// what catches a writer that hashed something other than what it wrote;
	// asserted directly because a zstd bump would end frame-level identity
	// while leaving this the thing the two must agree on.
	walkHash, hashed, err := openFreezeTestPack(t, walkPath).ContentHash()
	require.NoError(t, err)
	require.True(t, hashed)
	freezeHash, hashed, err := openFreezeTestPack(t, freezePath).ContentHash()
	require.NoError(t, err)
	require.True(t, hashed)
	require.Equal(t, walkHash, freezeHash,
		"a frozen tabled pack and a walked one must hash the same raw ledgers to the same digest")
	require.NoError(t, openFreezeTestPack(t, walkPath).Verify(context.Background()))
	require.NoError(t, openFreezeTestPack(t, freezePath).Verify(context.Background()))

	// The frozen pack answers lookups through its tables and still reads whole.
	r := newTestColdReader(t, freezePath)
	for seq, want := range framedAt {
		require.NoError(t, r.WithTxTable(seq, func(tbl txspan.Table, _ txspan.LedgerHeader, pieces txspan.PieceReader) error {
			for i := range tbl.TxCount() {
				row := tbl.Row(i)
				env, elem, perr := pieces(row)
				require.NoError(t, perr)
				assert.Equal(t, want[row.EnvStart:row.EnvEnd], env)
				assert.Equal(t, want[row.ElemStart:row.ElemEnd], elem)
			}
			return nil
		}))
		require.NoError(t, r.WithLedger(seq, func(got []byte) error {
			assert.Equal(t, want, got)
			return nil
		}))
	}
	tabled, err := VerifyPack(freezePath)
	require.NoError(t, err)
	assert.Equal(t, len(framedAt), tabled)
}

// TestFreezeColdFromStore_WithoutTheTableFamily pins what a freeze does with a
// store that has no table family: it FAILS, naming the family, and writes no
// pack. The freeze pairs every ledger with its table as it scans, so a store
// that cannot be paired would otherwise hand it nil for all ten thousand of
// them and commit a chunk whose every cold transaction lookup decodes a whole
// ledger — an artifact indistinguishable from a legitimately untabled one.
//
// A hot DB always has the family (every open names it), so reaching this needs
// a store built by hand, which is the point: the failure is the one that says
// the store is not a hot ledger store.
func TestFreezeColdFromStore_WithoutTheTableFamily(t *testing.T) {
	withFrameWindow(t, coldFrameWindow)
	chunkID := chunk.ID(0)
	dir := t.TempDir()
	store, err := rocksdb.New(rocksdb.Config{
		Path: dir, ColumnFamilies: []string{LedgersCF}, Logger: silentLogger(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	h := NewWithStore(store, DefaultZstdEncodeWorkers)
	require.NoError(t, store.Batch(func(b *rocksdb.BatchWriter) error {
		return h.AddLedgerToBatch(b, Entry{Seq: chunkID.FirstLedger(), Bytes: freezePayload(chunkID.FirstLedger())})
	}))

	packPath := filepath.Join(t.TempDir(), "freeze.pack")
	n, err := FreezeColdFromStore(context.Background(), chunkID, store, packPath, ColdWriterOptions{})
	require.Error(t, err)
	assert.ErrorContains(t, err, TxSpansCF)
	assert.Zero(t, n, "a freeze that cannot pair must copy nothing")
	assert.NoFileExists(t, packPath, "a failed freeze leaves no pack behind")
}

// populateTabledChunk writes a whole chunk into the hot store the way the
// ingest loop does — the forked compression, the span table, and the value's
// frame directory stamped into it — batched so the test stays affordable.
func populateTabledChunk(t *testing.T, h *HotStore, chunkID chunk.ID, payload func(uint32) []byte) {
	t.Helper()
	const batch = 1000
	first, last := chunkID.FirstLedger(), chunkID.LastLedger()
	for lo := first; lo <= last; lo += batch {
		hi := min(lo+batch-1, last)
		require.NoError(t, h.store.Batch(func(b *rocksdb.BatchWriter) error {
			for seq := lo; seq <= hi; seq++ {
				if err := addTabledLedger(h, b, seq, payload(seq)); err != nil {
					return err
				}
			}
			return nil
		}))
	}
}

// addTabledLedger queues one ledger and its stamped span table into b.
func addTabledLedger(h *HotStore, b *rocksdb.BatchWriter, seq uint32, raw []byte) error {
	pending := h.StartCompress(Entry{Seq: seq, Bytes: raw})
	if err := h.AddPendingToBatch(b, pending); err != nil {
		return err
	}
	txParts, err := sdkingest.ExtractLedgerTxParts(xdr.LedgerCloseMetaView(raw))
	if err != nil {
		return err
	}
	table, err := txspan.Build(raw, txParts, testColdPassphrase)
	if err != nil {
		return err
	}
	stamped, err := txspan.WithFrames(table, pending.Frames())
	if err != nil {
		return err
	}
	h.AddTableToBatch(b, seq, stamped)
	return nil
}

// TestColdWithTxTable_FrontCoversTheTableAndTheHeaderFrame pins the exact
// read from both sides. A front that is what the record needs answers every
// row; a front one byte short of the HEADER frame — the table itself still
// whole — fails, which is what proves the reader takes the pack at its word
// and fetches neither more nor less than the app data records.
//
// A pack that mis-states its own front is corruption, not a cue to go back for
// the rest: the reader has just caught the geometry being wrong.
func TestColdWithTxTable_FrontCoversTheTableAndTheHeaderFrame(t *testing.T) {
	withFrameWindow(t, coldFrameWindow)
	const first = 4_700
	raw := framedLedger(t, first)
	value, frames := encodeFramedValue(t, raw)
	record := append(zstd.SkippableFrame(nil, stampedTable(t, raw, frames)), value...)
	tableFrame := len(record) - len(value)
	front := tableFrame + int(frames[0].Compressed)

	rows := 0
	r := newTestColdReader(t, writeFrontPack(t, first, front, record))
	require.NoError(t, r.WithTxTable(first,
		func(tbl txspan.Table, _ txspan.LedgerHeader, pieces txspan.PieceReader) error {
			assert.Equal(t, uint32(first), tbl.LedgerSeq())
			for i := range tbl.TxCount() {
				row := tbl.Row(i)
				env, elem, perr := pieces(row)
				require.NoError(t, perr)
				assert.Equal(t, raw[row.EnvStart:row.EnvEnd], env, "envelope %d", i)
				assert.Equal(t, raw[row.ElemStart:row.ElemEnd], elem, "element %d", i)
				rows++
			}
			return nil
		}))
	assert.Positive(t, rows)

	for name, tc := range map[string]struct {
		front  int
		reason string
	}{
		"a byte short of the header frame": {front - 1, "its table and header frame take"},
		"the table and nothing else":       {tableFrame, "its table and header frame take"},
		"half the table":                   {tableFrame / 2, "leading frame claims"},
	} {
		t.Run(name, func(t *testing.T) {
			short := newTestColdReader(t, writeFrontPack(t, first, tc.front, record))
			err := short.WithTxTable(first, func(txspan.Table, txspan.LedgerHeader, txspan.PieceReader) error {
				t.Fatal("fn must not run for a record the pack's front does not cover")
				return nil
			})
			require.ErrorIs(t, err, stores.ErrCorrupt)
			require.NotErrorIs(t, err, stores.ErrNoTable, "a mis-stated front is not an absent table")
			assert.ErrorContains(t, err, tc.reason)
			assert.ErrorContains(t, err, "4700")
		})
	}
}

// writeFrontPack writes records verbatim into a cold-format pack whose app
// data claims exactly front bytes as the widest table-and-header prefix. It
// goes through the packfile directly because the ColdWriter measures that
// number itself — which is the point: only a hand-built pack can state one the
// records do not bear out.
func writeFrontPack(t *testing.T, firstSeq uint32, front int, records ...[]byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "front.pack")
	pw, err := packfile.Create(path, packfile.WriterOptions{
		ItemsPerRecord: 1,
		Format:         formatLedgerCold,
		Overwrite:      true,
	})
	require.NoError(t, err)
	for _, record := range records {
		require.NoError(t, pw.AppendItem(record))
	}
	var ad [appDataSize]byte
	ad[0] = coldAppDataVersion
	binary.BigEndian.PutUint32(ad[1:], firstSeq)
	ad[offAppDataFlags] |= coldFlagTables
	binary.BigEndian.PutUint32(ad[offAppDataFront:], uint32(front))
	require.NoError(t, pw.Finish(ad[:]))
	return path
}

// TestColdWriter_RecordsTheWidestFront pins what the writer owes the reader: a
// front that covers the table and the header frame of EVERY record, measured
// over records of different sizes — and no wider, so the read a small record
// pays for is its own size and not the biggest one's.
func TestColdWriter_RecordsTheWidestFront(t *testing.T) {
	withFrameWindow(t, coldFrameWindow)
	const first = 4_750
	lcms := [][]byte{
		zeroTxLedger(t, first),
		framedLedgerOf(t, first+1, 24),
		framedLedgerOf(t, first+2, 160),
	}
	r := newTestColdReader(t, writeFramedPack(t, first, testColdPassphrase, lcms...))
	h, err := r.init()
	require.NoError(t, err)

	fronts := make([]int, len(lcms))
	for i := range lcms {
		fronts[i] = recordFront(t, r, i)
	}
	assert.Zero(t, fronts[0], "premise: a record with no table needs no front")
	assert.Less(t, fronts[1], fronts[2], "premise: the two tabled records need different fronts")
	assert.EqualValues(t, fronts[2], h.maxFront, "the pack records the widest front, exactly")
}

// recordFront reads record i off the pack and measures what a table read of it
// has to fetch: its leading skippable frame plus the ledger header frame
// behind it. Zero for a record that carries no table.
func recordFront(t *testing.T, r *ColdReader, i int) int {
	t.Helper()
	at, size, err := r.r.RecordRange(i)
	require.NoError(t, err)
	record := make([]byte, size)
	require.NoError(t, r.r.ReadAt(record, at))
	if !zstd.IsSkippable(record) {
		return 0
	}
	_, frameLen, err := zstd.SkippablePayload(record)
	require.NoError(t, err)
	first, err := zstd.FrameCompressedSize(record[frameLen:])
	require.NoError(t, err)
	return frameLen + first
}

// TestColdWithTxTable_LeadingFrameClaimingPastTheRecord pins the bound itself:
// a leading frame whose length field reaches past the front the pack records
// is corruption of that record, not license to read the rest of it — or the
// next one's bytes — as a table.
func TestColdWithTxTable_LeadingFrameClaimingPastTheRecord(t *testing.T) {
	withFrameWindow(t, coldFrameWindow)
	const first = 4_800
	path := writeFramedPack(t, first, testColdPassphrase,
		framedLedger(t, first), framedLedger(t, first+1))
	overstateFirstFrameLen(t, path)

	r := newTestColdReader(t, path)
	readErr := r.WithTxTable(first, func(txspan.Table, txspan.LedgerHeader, txspan.PieceReader) error {
		t.Fatal("fn must not run for a frame that does not fit its record")
		return nil
	})
	require.ErrorIs(t, readErr, stores.ErrCorrupt)
	require.NotErrorIs(t, readErr, stores.ErrNoTable)
	assert.ErrorContains(t, readErr, "leading frame claims")

	_, verr := VerifyPack(path)
	require.ErrorIs(t, verr, stores.ErrCorrupt)
	assert.ErrorContains(t, verr, "leading frame claims")
}

// overstateFirstFrameLen rewrites the length field of the pack's first
// record's leading skippable frame so it claims more bytes than the record
// holds, leaving every other byte of the pack alone.
func overstateFirstFrameLen(t *testing.T, path string) {
	t.Helper()
	_, at := firstRecord(t, path)
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	_, err = f.WriteAt(binary.LittleEndian.AppendUint32(nil, math.MaxUint32), at+4)
	require.NoError(t, err)
}

// TestSkippableFrameLen_RefusesWhatIsNotASkippableHeader pins the header read
// the overrun report's length comes from: without eight bytes of skippable
// header there is no length to trust, and none may be invented.
func TestSkippableFrameLen_RefusesWhatIsNotASkippableHeader(t *testing.T) {
	whole := zstd.SkippableFrame(nil, []byte("a table's bytes"))
	n, err := skippableFrameLen(whole)
	require.NoError(t, err)
	assert.Equal(t, len(whole), n)

	for name, src := range map[string][]byte{
		"nothing at all":          nil,
		"a truncated header":      whole[:7],
		"a compressed frame":      append([]byte{0x28, 0xB5, 0x2F, 0xFD}, whole[4:]...),
		"a header of other bytes": make([]byte, 8),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := skippableFrameLen(src)
			require.Error(t, err)
		})
	}
}

// TestFreezeColdFromStore_RefusesAMiskeyedTable pins the freeze's own header
// assertion. The freeze copies a table verbatim without ever decoding it, so
// it is the one producer that could carry a hot row stored under the wrong key
// into a durable pack; a reader serving from that pack would then answer with
// another ledger's transactions. The freeze must fail loudly instead, and must
// not quietly drop the table either.
func TestFreezeColdFromStore_RefusesAMiskeyedTable(t *testing.T) {
	withFrameWindow(t, coldFrameWindow)
	chunkID := chunk.ID(0)
	h, store := openTestHotStoreAt(t, t.TempDir())
	first := chunkID.FirstLedger()

	// An honest table for the NEXT ledger, stored under this one's key.
	other := framedLedger(t, first+1)
	txParts, err := sdkingest.ExtractLedgerTxParts(xdr.LedgerCloseMetaView(other))
	require.NoError(t, err)
	table, err := txspan.Build(other, txParts, testColdPassphrase)
	require.NoError(t, err)
	require.NoError(t, h.store.Batch(func(b *rocksdb.BatchWriter) error {
		if perr := h.AddLedgerToBatch(b, Entry{Seq: first, Bytes: framedLedger(t, first)}); perr != nil {
			return perr
		}
		h.AddTableToBatch(b, first, table)
		return nil
	}))

	packPath := filepath.Join(t.TempDir(), "freeze.pack")
	_, err = FreezeColdFromStore(context.Background(), chunkID, store, packPath, ColdWriterOptions{})
	require.Error(t, err, "a mis-keyed table must never reach a durable pack")
	assert.ErrorContains(t, err, "stamped for ledger")
}
