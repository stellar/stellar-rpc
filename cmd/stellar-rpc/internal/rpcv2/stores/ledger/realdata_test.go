package ledger

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sdkingest "github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/packfile"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/txspan"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/zstd"
)

// Environment for the real-data check: a cold ledger pack to read source
// ledgers from and the window inside it to use, written "firstSeq-count".
//
// The pack must be one this lineage reads: its ledgers' HEADER sequences have
// to equal the sequences the pack resolves them at, because IterateLedgers
// checks that on every ledger it yields. A synthetic corpus that numbered its
// records independently of its headers will not run here any more — that is
// the point of the check, not an obstacle to it.
const (
	packEnv  = "STELLAR_RPC_DIFF_PACK"
	rangeEnv = "STELLAR_RPC_DIFF_RANGE"
)

// TestRealLedgerFraming runs the framing and piece-read contracts over real
// ledgers, which is where the sizes that trigger framing — and the spans that
// straddle a cut — actually occur. It skips unless a pack is named, since no
// pack ships with the tree.
//
// The cold pack is built through the WALK writer rather than the freeze: an
// env range is far shorter than a chunk, which FreezeColdFromStore requires
// whole. The two are byte-identical by TestFreezeColdFromStore_FramedLedgersMatchTheWalk,
// so what is checked here of one holds of the other.
func TestRealLedgerFraming(t *testing.T) {
	packPath, rangeSpec := os.Getenv(packEnv), os.Getenv(rangeEnv)
	if packPath == "" || rangeSpec == "" {
		t.Skipf("set %s and %s (firstSeq-count) to run the real-data framing check", packEnv, rangeEnv)
	}
	first, last := parseDiffRange(t, rangeSpec)

	src, err := OpenColdReader(packPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = src.Close() })
	reportPackTail(t, src, packPath)

	hot, store := openTestHotStoreAt(t, t.TempDir())
	coldPath := filepath.Join(t.TempDir(), "framed.pack")
	w, err := NewColdWriter(coldPath, first, ColdWriterOptions{
		Passphrase: network.PublicNetworkPassphrase,
	})
	require.NoError(t, err)

	sum := ingestRealRange(t, src, hot, store, w, first, last)
	require.NoError(t, w.Commit())
	require.NoError(t, w.Close())

	cold, err := OpenColdReader(coldPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cold.Close() })
	coldHead, err := cold.init()
	require.NoError(t, err)
	sum.maxFront = int(coldHead.maxFront)
	checkRealRange(t, src, hot, cold, first, last, &sum)

	tabled, err := VerifyPack(coldPath)
	require.NoError(t, err)
	sum.report(t, packPath, first, last, tabled)
}

// realSummary accumulates what the run saw, so the check reports the shape of
// the data it proved itself against rather than asserting numbers no fixture
// can predict.
type realSummary struct {
	ledgers, txs, framed, straddling int
	frames                           []int
	rawBytes, framedBytes, oneBytes  int
	// maxFront is what the written pack's app data records as the widest
	// table-and-header-frame prefix — the size of every cold table read.
	maxFront                int
	hotLookups, coldLookups []time.Duration
}

// lookupSample is how many of a ledger's transactions are looked up one at a
// time, each through its own WithTxTable call. That is the served shape — one
// request, one transaction — and the only one whose timing means anything; a
// sweep of every row reuses decoded frames and measures something no request
// does.
const lookupSample = 16

// ingestRealRange writes every source ledger into both tiers the way
// production does — the hot store's forked compression with its table stamped
// beside it, and the cold writer's own encode — and measures the framed
// encoding against a single-frame one over the same bytes.
func ingestRealRange(
	t *testing.T, src *ColdReader, hot *HotStore, store *rocksdb.Store, w *ColdWriter, first, last uint32,
) realSummary {
	t.Helper()
	var sum realSummary
	single := zstd.NewEncoderState(encoderOptions(DefaultZstdEncodeWorkers)...)
	for entry, ierr := range src.IterateLedgers(first, last) {
		require.NoError(t, ierr)
		require.NoError(t, store.Batch(func(b *rocksdb.BatchWriter) error {
			return addRealLedger(hot, b, entry.Seq, entry.Bytes)
		}))
		require.NoError(t, w.AppendLedger(entry.Seq, entry.Bytes))

		end, window := frameCut(entry.Bytes)
		framed, sizes, ferr := single.EncodeFrames(entry.Bytes, end, window)
		require.NoError(t, ferr)
		framedLen := len(framed)
		framedCopy := slices.Clone(framed)
		one, oerr := single.Encode(entry.Bytes)
		require.NoError(t, oerr)
		if len(sizes) == 1 {
			assert.True(t, slices.Equal(framedCopy, one),
				"ledger %d fits the window, so its value must be the pre-frames bytes", entry.Seq)
		}
		sum.ledgers++
		sum.rawBytes += len(entry.Bytes)
		sum.framedBytes += framedLen
		sum.oneBytes += len(one)
		sum.frames = append(sum.frames, len(sizes))
		if len(sizes) > 1 {
			sum.framed++
		}
	}
	require.Positive(t, sum.ledgers, "the pack yielded no ledgers")
	return sum
}

// addRealLedger is the ingest loop's ledger write: the forked compression, the
// span table built from the same bytes, and the value's frame directory
// stamped into that table before both land in one batch.
func addRealLedger(hot *HotStore, b *rocksdb.BatchWriter, seq uint32, raw []byte) error {
	pending := hot.StartCompress(Entry{Seq: seq, Bytes: raw})
	if err := hot.AddPendingToBatch(b, pending); err != nil {
		return err
	}
	txParts, err := sdkingest.ExtractLedgerTxParts(xdr.LedgerCloseMetaView(raw))
	if err != nil {
		return err
	}
	table, err := txspan.Build(raw, txParts, network.PublicNetworkPassphrase)
	if err != nil {
		return err
	}
	stamped, err := txspan.WithFrames(table, pending.Frames())
	if err != nil {
		return err
	}
	hot.AddTableToBatch(b, seq, stamped)
	return nil
}

// checkRealRange proves the four claims the tiers make about every
// transaction: the hot pieces are the raw spans, the cold pieces are the same
// bytes, the cold record still reads whole, and both tiers agree with the
// source ledger.
func checkRealRange(
	t *testing.T, src *ColdReader, hot *HotStore, cold *ColdReader, first, last uint32, sum *realSummary,
) {
	t.Helper()
	for entry, ierr := range src.IterateLedgers(first, last) {
		require.NoError(t, ierr)
		raw := entry.Bytes
		require.NoError(t, cold.WithLedger(entry.Seq, func(got []byte) error {
			assert.Len(t, got, len(raw), "cold ledger %d length", entry.Seq)
			assert.True(t, slices.Equal(raw, got), "cold ledger %d bytes", entry.Seq)
			return nil
		}))

		var hotEnv, hotElem [][]byte
		framed := false
		require.NoError(t, hot.WithTxTable(entry.Seq,
			func(tbl txspan.Table, _ txspan.LedgerHeader, pieces txspan.PieceReader) error {
				framed = tbl.FrameCount() > 1
				for i := range tbl.TxCount() {
					row := tbl.Row(i)
					env, elem, perr := pieces(row)
					require.NoError(t, perr)
					assert.True(t, slices.Equal(raw[row.EnvStart:row.EnvEnd], env),
						"hot envelope %d of ledger %d", i, entry.Seq)
					assert.True(t, slices.Equal(raw[row.ElemStart:row.ElemEnd], elem),
						"hot element %d of ledger %d", i, entry.Seq)
					hotEnv = append(hotEnv, slices.Clone(env))
					hotElem = append(hotElem, slices.Clone(elem))
					if spansACut(tbl, row.EnvStart, row.EnvEnd) || spansACut(tbl, row.ElemStart, row.ElemEnd) {
						sum.straddling++
					}
				}
				sum.txs += tbl.TxCount()
				return nil
			}))

		if !framed {
			// A ledger inside the window keeps the record it has always had:
			// one frame, no table, so a cold lookup decodes and walks.
			require.ErrorIs(t, cold.WithTxTable(entry.Seq, func(txspan.Table, txspan.LedgerHeader, txspan.PieceReader) error {
				return nil
			}), stores.ErrNoTable)
			timeLookups(t, hot, nil, entry.Seq, len(hotEnv), sum)
			continue
		}
		require.NoError(t, cold.WithTxTable(entry.Seq,
			func(tbl txspan.Table, _ txspan.LedgerHeader, pieces txspan.PieceReader) error {
				for i := range tbl.TxCount() {
					env, elem, perr := pieces(tbl.Row(i))
					require.NoError(t, perr)
					assert.True(t, slices.Equal(hotEnv[i], env), "cold envelope %d of ledger %d", i, entry.Seq)
					assert.True(t, slices.Equal(hotElem[i], elem), "cold element %d of ledger %d", i, entry.Seq)
				}
				return nil
			}))
		timeLookups(t, hot, cold, entry.Seq, len(hotEnv), sum)
	}
}

// spansACut reports whether [start, end) crosses a frame boundary, which is
// the case a piece read has to decode a run of frames for.
func spansACut(t txspan.Table, start, end uint32) bool {
	if end <= start {
		return false
	}
	lo, _, _, _ := t.RawOffsetFrame(start)
	hi, _, _, _ := t.RawOffsetFrame(end - 1)
	return lo != hi
}

// timeLookups measures the served shape on both tiers: one request, one
// transaction, each through its own call with nothing decoded in advance. A
// nil cold reader means this ledger's record carries no table, so only the hot
// tier answers through one.
func timeLookups(t *testing.T, hot *HotStore, cold *ColdReader, seq uint32, rows int, sum *realSummary) {
	t.Helper()
	if rows == 0 {
		return
	}
	for k := range min(lookupSample, rows) {
		row := k * rows / min(lookupSample, rows)
		start := time.Now()
		require.NoError(t, hot.WithTxTable(seq,
			func(tbl txspan.Table, _ txspan.LedgerHeader, pieces txspan.PieceReader) error {
				_, _, err := pieces(tbl.Row(row))
				return err
			}))
		sum.hotLookups = append(sum.hotLookups, time.Since(start))
		if cold == nil {
			continue
		}

		start = time.Now()
		require.NoError(t, cold.WithTxTable(seq,
			func(tbl txspan.Table, _ txspan.LedgerHeader, pieces txspan.PieceReader) error {
				_, _, err := pieces(tbl.Row(row))
				return err
			}))
		sum.coldLookups = append(sum.coldLookups, time.Since(start))
	}
}

func (s *realSummary) report(t *testing.T, packPath string, first, last uint32, tabled int) {
	t.Helper()
	slices.Sort(s.frames)
	t.Logf("pack %s ledgers [%d, %d]", packPath, first, last)
	t.Logf("ledgers=%d txs=%d framedLedgers=%d tabledColdRecords=%d straddlingSpans=%d maxFront=%d",
		s.ledgers, s.txs, s.framed, tabled, s.straddling, s.maxFront)
	t.Logf("frames per ledger: min=%d p50=%d max=%d",
		s.frames[0], s.frames[len(s.frames)/2], s.frames[len(s.frames)-1])
	t.Logf("value bytes: raw=%d singleFrame=%d framed=%d (framed is %.3f%% larger)",
		s.rawBytes, s.oneBytes, s.framedBytes,
		100*(float64(s.framedBytes)-float64(s.oneBytes))/float64(s.oneBytes))
	t.Logf("one-transaction WithTxTable, hot:  %s", spreadOf(s.hotLookups))
	t.Logf("one-transaction WithTxTable, cold: %s", spreadOf(s.coldLookups))
}

// reportPackTail measures the region the speculative tail read is aiming at —
// the record index, the app data and the trailer — and says whether the cold
// reader's tuned tail covers it. It is the one number that decides whether
// coldTailRead is set right, and a synthetic pack cannot produce it: it is a
// function of the real record count and the real record sizes.
func reportPackTail(t *testing.T, r *ColdReader, packPath string) {
	t.Helper()
	tr, err := r.r.Trailer()
	require.NoError(t, err)
	tail := int64(tr.IndexSize) + int64(tr.AppDataSize) + packfile.TrailerSize
	t.Logf("pack tail %s: index=%d appData=%d trailer=%d total=%d bytes over %d records "+
		"(coldTailRead=%d covers it: %t)",
		packPath, tr.IndexSize, tr.AppDataSize, packfile.TrailerSize, tail, tr.RecordCount,
		coldTailRead, tail <= coldTailRead)
}

// spreadOf formats a set of durations as the p50/p99/max a measurement reads.
func spreadOf(ds []time.Duration) string {
	if len(ds) == 0 {
		return "no calls"
	}
	sorted := slices.Clone(ds)
	slices.Sort(sorted)
	return fmt.Sprintf("%d calls p50=%s p99=%s max=%s", len(sorted),
		sorted[len(sorted)/2], sorted[len(sorted)*99/100], sorted[len(sorted)-1])
}

func parseDiffRange(t *testing.T, spec string) (uint32, uint32) {
	t.Helper()
	lo, count, ok := strings.Cut(spec, "-")
	require.True(t, ok, "%s must be firstSeq-count, got %q", rangeEnv, spec)
	firstSeq, err := strconv.ParseUint(lo, 10, 32)
	require.NoError(t, err)
	n, err := strconv.ParseUint(count, 10, 32)
	require.NoError(t, err)
	require.Positive(t, n, "%s count must be positive", rangeEnv)
	return uint32(firstSeq), uint32(firstSeq + n - 1)
}
