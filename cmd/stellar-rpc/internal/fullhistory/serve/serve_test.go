package serve

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/registry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/hotchunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/ledger"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/txhash"
)

const (
	fxChunkCold0 = chunk.ID(0) // fully cold: partial pack + tx entries in the window .idx
	fxChunkCold1 = chunk.ID(1) // fully cold: full zero-tx pack (the cold side of the boundary walk)
	fxChunkHot   = chunk.ID(2) // live hot chunk (write handle pre-opened, adopted by the build)
)

// fxTx is one fixture transaction: where it landed and the fields the
// adapters must reproduce.
type fxTx struct {
	hash     xdr.Hash
	seq      uint32
	appOrder int32
	envelope []byte
}

type fixture struct {
	cat     *catalog.Catalog
	reg     *registry.Registry
	hotDB   *hotchunk.DB
	latest  uint32
	coldTxs []fxTx            // in chunk 0, reachable only through the window .idx
	hotTxs  []fxTx            // in the live chunk, reachable through the hot exact index
	raws    map[uint32][]byte // original LCM bytes of every tx-bearing ledger
}

func silentLogger() *supportlog.Entry {
	var buf bytes.Buffer
	log := supportlog.New()
	log.SetLevel(logrus.DebugLevel)
	log.SetOutput(&buf)
	return log
}

func newTestCatalog(t *testing.T) *catalog.Catalog {
	t.Helper()
	idxLayout, err := geometry.NewTxHashIndexLayout(geometry.ChunksPerTxhashIndex)
	require.NoError(t, err)
	cat, err := catalog.Open(
		filepath.Join(t.TempDir(), "rocksdb"), geometry.NewLayout(t.TempDir()), idxLayout, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = cat.Close() })
	return cat
}

// buildLCM builds a V2 LedgerCloseMeta with txCount transactions, close time
// int64(seq), returning its bytes and the per-tx fixture records. Zero-tx
// calls are deterministic (safe to rebuild for byte comparisons); tx-bearing
// calls draw random source accounts, so callers keep the returned raws.
func buildLCM(t *testing.T, seq uint32, txCount int) ([]byte, []fxTx) {
	t.Helper()
	phases := make([]xdr.TransactionPhase, 0, txCount)
	txProcessing := make([]xdr.TransactionResultMetaV1, 0, txCount)
	txs := make([]fxTx, 0, txCount)

	for i := range txCount {
		envelope := xdr.TransactionEnvelope{
			Type: xdr.EnvelopeTypeEnvelopeTypeTx,
			V1: &xdr.TransactionV1Envelope{
				Tx: xdr.Transaction{
					SourceAccount: xdr.MustMuxedAddress(keypair.MustRandom().Address()),
					Ext: xdr.TransactionExt{
						V:           1,
						SorobanData: &xdr.SorobanTransactionData{},
					},
				},
			},
		}
		hash, err := network.HashTransactionInEnvelope(envelope, network.TestNetworkPassphrase)
		require.NoError(t, err)
		envRaw, err := envelope.MarshalBinary()
		require.NoError(t, err)
		txs = append(txs, fxTx{hash: hash, seq: seq, appOrder: int32(i) + 1, envelope: envRaw})

		opResults := []xdr.OperationResult{}
		txProcessing = append(txProcessing, xdr.TransactionResultMetaV1{
			TxApplyProcessing: xdr.TransactionMeta{V: 3, V3: &xdr.TransactionMetaV3{}},
			Result: xdr.TransactionResultPair{
				TransactionHash: hash,
				Result: xdr.TransactionResult{
					FeeCharged: 100,
					Result: xdr.TransactionResultResult{
						Code:    xdr.TransactionResultCodeTxSuccess,
						Results: &opResults,
					},
				},
			},
		})
		comp := []xdr.TxSetComponent{{
			Type: xdr.TxSetComponentTypeTxsetCompTxsMaybeDiscountedFee,
			TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{
				Txs: []xdr.TransactionEnvelope{envelope},
			},
		}}
		phases = append(phases, xdr.TransactionPhase{V: 0, V0Components: &comp})
	}

	lcm := xdr.LedgerCloseMeta{
		V: 2,
		V2: &xdr.LedgerCloseMetaV2{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					ScpValue:  xdr.StellarValue{CloseTime: xdr.TimePoint(seq)},
					LedgerSeq: xdr.Uint32(seq),
				},
			},
			TxSet: xdr.GeneralizedTransactionSet{
				V:       1,
				V1TxSet: &xdr.TransactionSetV1{Phases: phases},
			},
			TxProcessing: txProcessing,
		},
	}
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	return raw, txs
}

// buildFixture assembles the acceptance scenario over a real catalog and real
// stores:
//
//	chunk 0 cold — partial ledger pack [2, 11], txs at seqs 4 (two) and 7 (one),
//	  indexed by window 0's .idx (coverage chunks [0, 1]);
//	chunk 1 cold — full 10k-ledger zero-tx pack (tail feeds the boundary walk);
//	chunk 2 live hot — ledgers [20002, 20011], txs at 20004 (two), handle
//	  pre-opened and adopted by BuildFromCatalog;
//	full retention (floor = chunk 0), latest = 20011.
func buildFixture(t *testing.T) *fixture {
	t.Helper()
	fx := &fixture{raws: map[uint32][]byte{}}
	fx.cat = newTestCatalog(t)
	layout := fx.cat.Layout()

	writePack := func(c chunk.ID, first, last uint32, txSeqs map[uint32]int) {
		path := layout.LedgerPackPath(c)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		w, err := ledger.NewColdWriter(path, first, ledger.ColdWriterOptions{})
		require.NoError(t, err)
		defer func() { _ = w.Close() }()
		for seq := first; seq <= last; seq++ {
			raw, txs := buildLCM(t, seq, txSeqs[seq])
			require.NoError(t, w.AppendLedger(seq, raw))
			if len(txs) > 0 {
				fx.coldTxs = append(fx.coldTxs, txs...)
				fx.raws[seq] = raw
			}
		}
		require.NoError(t, w.Commit())
	}
	writePack(fxChunkCold0, fxChunkCold0.FirstLedger(), fxChunkCold0.FirstLedger()+9,
		map[uint32]int{4: 2, 7: 1})
	writePack(fxChunkCold1, fxChunkCold1.FirstLedger(), fxChunkCold1.LastLedger(), nil)
	for _, c := range []chunk.ID{fxChunkCold0, fxChunkCold1} {
		require.NoError(t, fx.cat.MarkChunkFreezing(c, geometry.KindLedgers))
		require.NoError(t, fx.cat.FlipChunkFrozen(c, geometry.KindLedgers))
	}

	// Window 0's .idx over chunks [0, 1], built from chunk 0's tx entries the
	// way backfill does: truncated-key .bin, then the streaming index build.
	entries := make([]txhash.ColdEntry, 0, len(fx.coldTxs))
	for _, tx := range fx.coldTxs {
		var e txhash.ColdEntry
		copy(e.Key[:], tx.hash[:txhash.ColdKeySize])
		e.Seq = tx.seq
		entries = append(entries, e)
	}
	slices.SortFunc(entries, func(a, b txhash.ColdEntry) int { return bytes.Compare(a.Key[:], b.Key[:]) })
	binPath := filepath.Join(t.TempDir(), txhash.ColdBinName(fxChunkCold0))
	require.NoError(t, txhash.WriteColdBin(binPath, entries))
	cov, err := fx.cat.MarkTxHashIndexFreezing(0, fxChunkCold0, fxChunkCold1)
	require.NoError(t, err)
	require.NoError(t, fx.cat.CommitTxHashIndex(cov))
	idxPath := layout.TxHashIndexFilePath(cov)
	require.NoError(t, os.MkdirAll(filepath.Dir(idxPath), 0o755))
	require.NoError(t, txhash.BuildColdIndex(t.Context(), []string{binPath}, idxPath,
		fxChunkCold0.FirstLedger(), fxChunkCold1.LastLedger()))

	// The live hot chunk: real write handle, ten committed ledgers.
	hotPath := layout.HotChunkPath(fxChunkHot)
	require.NoError(t, os.MkdirAll(filepath.Dir(hotPath), 0o755))
	fx.hotDB, err = hotchunk.Open(hotPath, fxChunkHot, silentLogger())
	require.NoError(t, err)
	first := fxChunkHot.FirstLedger()
	for seq := first; seq <= first+9; seq++ {
		txCount := 0
		if seq == first+2 {
			txCount = 2
		}
		raw, txs := buildLCM(t, seq, txCount)
		_, err := fx.hotDB.IngestLedger(seq, xdr.LedgerCloseMetaView(raw))
		require.NoError(t, err)
		if len(txs) > 0 {
			fx.hotTxs = append(fx.hotTxs, txs...)
			fx.raws[seq] = raw
		}
	}
	require.NoError(t, fx.cat.PutHotTransient(fxChunkHot))
	require.NoError(t, fx.cat.FlipHotReady(fxChunkHot))

	fx.latest = first + 9
	fx.reg, err = registry.BuildFromCatalog(fx.cat, geometry.NewRetention(0, 0), fx.latest,
		registry.Options{
			PreOpened: map[chunk.ID]*hotchunk.DB{fxChunkHot: fx.hotDB},
			Logger:    silentLogger(),
		})
	require.NoError(t, err)
	t.Cleanup(fx.reg.Close)
	return fx
}

func TestLedgerReader_RangeAndPointReads(t *testing.T) {
	fx := buildFixture(t)
	lr := NewLedgerReader(fx.reg)
	ctx := t.Context()

	latest, err := lr.GetLatestLedgerSequence(ctx)
	require.NoError(t, err)
	require.Equal(t, fx.latest, latest)

	rng, err := lr.GetLedgerRange(ctx)
	require.NoError(t, err)
	require.Equal(t, chunk.FirstLedgerSeq, int(rng.FirstLedger.Sequence))
	require.EqualValues(t, chunk.FirstLedgerSeq, rng.FirstLedger.CloseTime, "fixture close time = seq")
	require.Equal(t, fx.latest, rng.LastLedger.Sequence)
	require.EqualValues(t, fx.latest, rng.LastLedger.CloseTime)

	// Point reads resolve through both tiers.
	for _, seq := range []uint32{4, fxChunkCold1.FirstLedger() + 5, fx.latest} {
		lcm, found, gerr := lr.GetLedger(ctx, seq)
		require.NoError(t, gerr)
		require.True(t, found, "seq %d", seq)
		require.Equal(t, seq, lcm.LedgerSequence())
		require.EqualValues(t, seq, lcm.LedgerCloseTime())
	}

	// Outside the admitted bounds: not found, no error.
	for _, seq := range []uint32{1, fx.latest + 1} {
		_, found, gerr := lr.GetLedger(ctx, seq)
		require.NoError(t, gerr, "seq %d", seq)
		require.False(t, found, "seq %d", seq)
	}

	require.ErrorContains(t, lr.StreamAllLedgers(ctx, nil), "not supported by the full-history backend")
	require.ErrorContains(t, lr.StreamLedgerRange(ctx, 1, 2, nil), "not supported by the full-history backend")
	_, _, _, err = lr.GetLedgerCountInRange(ctx, 1, 2)
	require.ErrorContains(t, err, "not supported by the full-history backend")
}

func TestLedgerReaderTx_BatchAcrossColdHotBoundary(t *testing.T) {
	fx := buildFixture(t)
	lr := NewLedgerReader(fx.reg)
	ctx := t.Context()

	readTx, err := lr.NewTx(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, readTx.Done()) }()

	rng, err := readTx.GetLedgerRange(ctx)
	require.NoError(t, err)
	require.Equal(t, fx.latest, rng.LastLedger.Sequence)

	// [cold chunk 1 tail | hot chunk 2 head] in one ascending walk.
	start := fxChunkCold1.LastLedger() - 3
	end := fxChunkHot.FirstLedger() + 3
	batch, err := readTx.BatchGetLedgers(ctx, start, end)
	require.NoError(t, err)
	require.Len(t, batch, int(end-start+1))
	for i, item := range batch {
		seq := start + uint32(i) //nolint:gosec // small test counts
		require.EqualValues(t, seq, item.Header.Header.LedgerSeq)
		expected, ok := fx.raws[seq]
		if !ok {
			expected, _ = buildLCM(t, seq, 0) // zero-tx fixture ledgers rebuild deterministically
		}
		require.Equal(t, expected, item.Lcm, "seq %d", seq)
		var lcm xdr.LedgerCloseMeta
		require.NoError(t, lcm.UnmarshalBinary(item.Lcm))
		require.Equal(t, lcm.LedgerHeaderHistoryEntry(), item.Header, "header derives from the same meta")
	}

	// The trailing edge truncates at the admitted latest.
	batch, err = readTx.BatchGetLedgers(ctx, fx.latest-1, fx.latest+50)
	require.NoError(t, err)
	require.Len(t, batch, 2)
	require.EqualValues(t, fx.latest, batch[1].Header.Header.LedgerSeq)

	_, err = readTx.BatchGetLedgers(ctx, 9, 3)
	require.ErrorContains(t, err, "batch size must be greater than zero")
}

func TestTransactionReader_ColdAndHotLookups(t *testing.T) {
	fx := buildFixture(t)
	tr, err := NewTransactionReader(fx.reg, network.TestNetworkPassphrase)
	require.NoError(t, err)
	ctx := t.Context()

	require.NotEmpty(t, fx.coldTxs)
	require.NotEmpty(t, fx.hotTxs)
	for _, want := range append(slices.Clone(fx.coldTxs), fx.hotTxs...) {
		got, gerr := tr.GetTransaction(ctx, want.hash)
		require.NoError(t, gerr, "tx %x", want.hash)
		require.Equal(t, want.hash.HexString(), got.TransactionHash)
		require.Equal(t, want.seq, got.Ledger.Sequence)
		require.EqualValues(t, want.seq, got.Ledger.CloseTime, "fixture close time = seq")
		require.Equal(t, want.appOrder, got.ApplicationOrder)
		require.Equal(t, want.envelope, got.Envelope)
		require.True(t, got.Successful)
		require.False(t, got.FeeBump)
		require.NotEmpty(t, got.Result)
		require.NotEmpty(t, got.Meta)
	}

	_, err = tr.GetTransaction(ctx, xdr.Hash{0xde, 0xad, 0xbe, 0xef})
	require.ErrorIs(t, err, db.ErrNoTransaction)
}

func TestServe_BelowFloorReadsAsNotFound(t *testing.T) {
	fx := buildFixture(t)
	lr := NewLedgerReader(fx.reg)
	tr, err := NewTransactionReader(fx.reg, network.TestNetworkPassphrase)
	require.NoError(t, err)
	ctx := t.Context()

	// Positive control before the floor moves: chunk 0 serves both shapes.
	_, found, err := lr.GetLedger(ctx, fx.coldTxs[0].seq)
	require.NoError(t, err)
	require.True(t, found)
	_, err = tr.GetTransaction(ctx, fx.coldTxs[0].hash)
	require.NoError(t, err)

	// Prune chunk 0. The window .idx (coverage [0, 1]) stays in the View and
	// keeps naming chunk-0 ledgers — exactly the state the floor gate covers.
	fx.reg.AdvanceFloor(fxChunkCold1)
	floorLedger := fxChunkCold1.FirstLedger()

	_, found, err = lr.GetLedger(ctx, fx.coldTxs[0].seq)
	require.NoError(t, err)
	require.False(t, found, "below-floor ledger reads as not-found (R2)")

	rng, err := lr.GetLedgerRange(ctx)
	require.NoError(t, err)
	require.Equal(t, floorLedger, rng.FirstLedger.Sequence, "the advertised range starts at the new floor")

	readTx, err := lr.NewTx(ctx)
	require.NoError(t, err)
	defer func() { _ = readTx.Done() }()
	batch, err := readTx.BatchGetLedgers(ctx, chunk.FirstLedgerSeq, floorLedger+2)
	require.NoError(t, err)
	require.Len(t, batch, 3, "the below-floor portion of the range returns no rows")
	require.EqualValues(t, floorLedger, batch[0].Header.Header.LedgerSeq)

	// A below-floor index candidate is a clean miss, not a lookup-incomplete
	// error: the tx genuinely existed in chunk 0, and R2 says pruned history
	// reads as not-found.
	_, err = tr.GetTransaction(ctx, fx.coldTxs[0].hash)
	require.ErrorIs(t, err, db.ErrNoTransaction)
}

func TestServe_NeverAheadOfLatest(t *testing.T) {
	fx := buildFixture(t)
	lr := NewLedgerReader(fx.reg)
	tr, err := NewTransactionReader(fx.reg, network.TestNetworkPassphrase)
	require.NoError(t, err)
	ctx := t.Context()

	// Commit one ledger past the admitted watermark (ingestion runs ahead of
	// AdvanceLatest by design) — neither the ledger nor its tx is visible.
	ahead := fx.latest + 1
	raw, txs := buildLCM(t, ahead, 1)
	_, err = fx.hotDB.IngestLedger(ahead, xdr.LedgerCloseMetaView(raw))
	require.NoError(t, err)

	_, found, err := lr.GetLedger(ctx, ahead)
	require.NoError(t, err)
	require.False(t, found)
	_, err = tr.GetTransaction(ctx, txs[0].hash)
	require.ErrorIs(t, err, db.ErrNoTransaction, "a hot exact hit past latest is gated, not served")

	fx.reg.AdvanceLatest(ahead)

	lcm, found, err := lr.GetLedger(ctx, ahead)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, ahead, lcm.LedgerSequence())
	got, err := tr.GetTransaction(ctx, txs[0].hash)
	require.NoError(t, err)
	require.Equal(t, ahead, got.Ledger.Sequence)
}

func TestLedgerReader_EmptyRegistry(t *testing.T) {
	cat := newTestCatalog(t)
	reg, err := registry.BuildFromCatalog(cat, geometry.NewRetention(0, 0), 0,
		registry.Options{Logger: silentLogger()})
	require.NoError(t, err)
	t.Cleanup(reg.Close)
	lr := NewLedgerReader(reg)
	ctx := t.Context()

	_, err = lr.GetLedgerRange(ctx)
	require.ErrorIs(t, err, db.ErrEmptyDB)
	_, err = lr.GetLatestLedgerSequence(ctx)
	require.ErrorIs(t, err, db.ErrEmptyDB)
	_, found, err := lr.GetLedger(ctx, 5)
	require.NoError(t, err)
	require.False(t, found)

	readTx, err := lr.NewTx(ctx)
	require.NoError(t, err)
	defer func() { _ = readTx.Done() }()
	_, err = readTx.GetLedgerRange(ctx)
	require.ErrorIs(t, err, db.ErrEmptyDB)
	batch, err := readTx.BatchGetLedgers(ctx, 2, 10)
	require.NoError(t, err)
	require.Empty(t, batch)
}
