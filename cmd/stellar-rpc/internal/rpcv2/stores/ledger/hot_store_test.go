package ledger

import (
	"bytes"
	"errors"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sdkingest "github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/txspan"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/zstd"
)

func silentLogger() *supportlog.Entry {
	var buf bytes.Buffer
	log := supportlog.New()
	log.SetLevel(logrus.DebugLevel)
	log.SetOutput(&buf)
	return log
}

func openTestHotStore(t *testing.T) *HotStore {
	t.Helper()
	h, _ := openTestHotStoreAt(t, t.TempDir())
	return h
}

func openTestHotStoreAt(t *testing.T, path string) (*HotStore, *rocksdb.Store) {
	t.Helper()
	store, err := rocksdb.New(rocksdb.Config{
		Path:           path,
		ColumnFamilies: CFNames(),
		Logger:         silentLogger(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return NewWithStore(store, DefaultZstdEncodeWorkers), store
}

func TestHotStore_AddGetRoundTripVerbatim(t *testing.T) {
	h := openTestHotStore(t)

	// Miss.
	_, err := readLedgerRaw(h, 42)
	require.ErrorIs(t, err, stores.ErrNotFound)

	// Single-entry write.
	payload := []byte("arbitrary opaque bytes the store has no opinion about")
	require.NoError(t, addLedgers(h, Entry{Seq: 42, Bytes: payload}))
	got, err := readLedgerRaw(h, 42)
	require.NoError(t, err)
	assert.Equal(t, payload, got)

	// Overwrite.
	updated := []byte("different bytes")
	require.NoError(t, addLedgers(h, Entry{Seq: 42, Bytes: updated}))
	got, err = readLedgerRaw(h, 42)
	require.NoError(t, err)
	assert.Equal(t, updated, got)

	// Zero entries — no-op, no error.
	require.NoError(t, addLedgers(h))
}

// TestHotStore_AddLedgersIdempotentRetry mirrors the events store's retry
// contract: re-delivering the same (seq, bytes) — e.g. a restarted ingester
// replaying the in-flight ledger — is a clean no-op. Unlike the
// log-structured events store (which drops the duplicate), the ledger store
// is a seq-keyed upsert, so the retry overwrites with identical bytes and
// does not duplicate the key.
func TestHotStore_AddLedgersIdempotentRetry(t *testing.T) {
	h := openTestHotStore(t)
	payload := []byte("ledger payload")

	require.NoError(t, addLedgers(h, Entry{Seq: 7, Bytes: payload}))
	require.NoError(t, addLedgers(h, Entry{Seq: 7, Bytes: payload})) // retry

	got, err := readLedgerRaw(h, 7)
	require.NoError(t, err)
	assert.Equal(t, payload, got)

	// Still a single entry — the retry overwrote rather than appended.
	last, ok, err := h.LastSeq()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, uint32(7), last)
}

func TestHotStore_LastSeq(t *testing.T) {
	h := openTestHotStore(t)

	// Empty store: ok=false, no error.
	_, ok, err := h.LastSeq()
	require.NoError(t, err)
	require.False(t, ok)

	// Insert seqs out of order; LastSeq reports the max present.
	require.NoError(t, addLedgers(h,
		Entry{Seq: 105, Bytes: []byte("c")},
		Entry{Seq: 100, Bytes: []byte("a")},
		Entry{Seq: 103, Bytes: []byte("b")},
	))
	last, ok, err := h.LastSeq()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, uint32(105), last)
}

func TestHotStore_AddLedgersMultipleEntries(t *testing.T) {
	h := openTestHotStore(t)

	entries := []Entry{
		{Seq: 100, Bytes: []byte("ledger 100 payload")},
		{Seq: 101, Bytes: []byte("ledger 101 payload")},
		{Seq: 102, Bytes: []byte("ledger 102 payload")},
	}
	require.NoError(t, addLedgers(h, entries...))
	for _, e := range entries {
		got, err := readLedgerRaw(h, e.Seq)
		require.NoError(t, err)
		assert.Equal(t, e.Bytes, got)
	}
}

func TestHotStore_IterateLedgers(t *testing.T) {
	h := openTestHotStore(t)
	for _, seq := range []uint32{10, 20, 30, 40, 50} {
		require.NoError(t, addLedgers(h, Entry{Seq: seq, Bytes: []byte("v")}))
	}

	// Full window.
	var seen []uint32
	for e, err := range h.IterateLedgers(10, 50) {
		require.NoError(t, err)
		seen = append(seen, e.Seq)
	}
	assert.Equal(t, []uint32{10, 20, 30, 40, 50}, seen)

	// Partial window starting mid-keyspace.
	seen = nil
	for e, err := range h.IterateLedgers(20, 40) {
		require.NoError(t, err)
		seen = append(seen, e.Seq)
	}
	assert.Equal(t, []uint32{20, 30, 40}, seen)

	// Window below the store's min — empty.
	seen = nil
	for e, err := range h.IterateLedgers(0, 5) {
		require.NoError(t, err)
		seen = append(seen, e.Seq)
	}
	assert.Empty(t, seen)

	// start > end — no-op, no error.
	seen = nil
	for e, err := range h.IterateLedgers(40, 20) {
		require.NoError(t, err)
		seen = append(seen, e.Seq)
	}
	assert.Empty(t, seen)

	// Mid-walk break — caller controls when to stop.
	seen = nil
	for e, err := range h.IterateLedgers(10, 50) {
		require.NoError(t, err)
		seen = append(seen, e.Seq)
		if len(seen) == 2 {
			break
		}
	}
	assert.Equal(t, []uint32{10, 20}, seen)
}

func TestHotStore_IterateLedgersVisibleGap(t *testing.T) {
	h := openTestHotStore(t)
	// Non-contiguous keyspace: missing 30.
	for _, seq := range []uint32{10, 20, 40, 50} {
		require.NoError(t, addLedgers(h, Entry{Seq: seq, Bytes: []byte("v")}))
	}

	var seen []uint32
	for e, err := range h.IterateLedgers(10, 50) {
		require.NoError(t, err)
		seen = append(seen, e.Seq)
	}
	assert.Equal(t, []uint32{10, 20, 40, 50}, seen)
}

func TestHotStore_WithLedgerReusesAndGrows(t *testing.T) {
	h := openTestHotStore(t)

	// A miss reports ErrNotFound and never calls fn.
	called := false
	err := h.WithLedger(42, func([]byte) error { called = true; return nil })
	require.ErrorIs(t, err, stores.ErrNotFound)
	assert.False(t, called)

	small := bytes.Repeat([]byte("a"), 32)
	require.NoError(t, addLedgers(h, Entry{Seq: 42, Bytes: small}))
	require.NoError(t, h.WithLedger(42, func(raw []byte) error {
		assert.Equal(t, small, raw)
		assert.Equal(t, len(raw), cap(raw), "the store caps the loan to the ledger")
		return nil
	}))

	// A ledger too big for the pooled buffer must grow it and decode correctly,
	// and a small one read afterwards must still come back exact and clipped.
	// Whether the grown buffer is the one the pool hands back is not asserted:
	// sync.Pool may discard at any GC, so identity is not a contract.
	big := bytes.Repeat([]byte("b"), 8192)
	require.NoError(t, addLedgers(h, Entry{Seq: 43, Bytes: big}))
	require.NoError(t, h.WithLedger(43, func(raw []byte) error {
		assert.Equal(t, big, raw)
		assert.Equal(t, len(raw), cap(raw))
		return nil
	}))
	require.NoError(t, h.WithLedger(42, func(raw []byte) error {
		assert.Equal(t, small, raw)
		assert.Equal(t, len(raw), cap(raw))
		return nil
	}))
}

func TestHotStore_WithLedgerReturnsWhatWasStored(t *testing.T) {
	h := openTestHotStore(t)
	// A realistic ledger, so the two paths are compared on something with
	// structure rather than a handful of repeated bytes.
	lcm, _ := makeRandomLedgerCloseMeta(7, 4)
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	require.NoError(t, addLedgers(h, Entry{Seq: 7, Bytes: raw}))

	fresh, err := readLedgerRaw(h, 7)
	require.NoError(t, err)
	require.NoError(t, h.WithLedger(7, func(lent []byte) error {
		assert.Equal(t, raw, fresh)
		assert.Equal(t, fresh, lent)
		return nil
	}))
}

// TestHotStore_WithLedgerPropagatesCallbackError pins that a caller's failure
// surfaces unchanged, and that the buffer still goes back.
func TestHotStore_WithLedgerPropagatesCallbackError(t *testing.T) {
	h := openTestHotStore(t)
	require.NoError(t, addLedgers(h, Entry{Seq: 9, Bytes: []byte("payload")}))
	sentinel := errors.New("caller said no")
	require.ErrorIs(t, h.WithLedger(9, func([]byte) error { return sentinel }), sentinel)
	require.NoError(t, h.WithLedger(9, func(raw []byte) error {
		assert.Equal(t, []byte("payload"), raw)
		return nil
	}))
}

// TestPoolable pins the pool ceiling: capacity only ratchets up, so a buffer
// grown past the cap must not be kept for the life of the store.
func TestPoolable(t *testing.T) {
	assert.True(t, poolable(nil))
	assert.True(t, poolable(make([]byte, 0, maxPooledLedgerBytes)))
	assert.False(t, poolable(make([]byte, 0, maxPooledLedgerBytes+1)))
}

func TestHotStore_GracefulCloseAndReopen(t *testing.T) {
	path := t.TempDir()

	seeded := []Entry{
		{Seq: 5, Bytes: []byte("payload-5")},
		{Seq: 10, Bytes: []byte("payload-10")},
		{Seq: 15, Bytes: []byte("payload-15")},
	}

	first, firstStore := openTestHotStoreAt(t, path)
	require.NoError(t, addLedgers(first, seeded...))
	require.NoError(t, firstStore.Close())

	second, _ := openTestHotStoreAt(t, path)

	for _, want := range seeded {
		got, err := readLedgerRaw(second, want.Seq)
		require.NoError(t, err)
		assert.Equal(t, want.Bytes, got)
	}
}

func TestHotStore_PostCloseOps(t *testing.T) {
	h, store := openTestHotStoreAt(t, t.TempDir())
	require.NoError(t, store.Close())

	require.ErrorIs(t, addLedgers(h, Entry{Seq: 1, Bytes: []byte("v")}), stores.ErrStoreClosed)
	_, err := readLedgerRaw(h, 1)
	require.ErrorIs(t, err, stores.ErrStoreClosed)
	var iterErr error
	for _, e := range h.IterateLedgers(0, 100) {
		iterErr = e
	}
	require.ErrorIs(t, iterErr, stores.ErrStoreClosed)

	require.ErrorIs(t, addLedgers(h), stores.ErrStoreClosed)

	// start > end short-circuits before touching the store, so it yields no
	// entries and no error even on a closed store (the documented contract).
	iterErr = nil
	for _, e := range h.IterateLedgers(100, 50) {
		iterErr = e
	}
	require.NoError(t, iterErr)
}

func TestHotStore_ConcurrentOpsAndCloseRaceFree(t *testing.T) {
	h, store := openTestHotStoreAt(t, t.TempDir())
	for i := range uint32(50) {
		require.NoError(t, addLedgers(h, Entry{Seq: i, Bytes: []byte("v")}))
	}

	var wg sync.WaitGroup
	var stop atomic.Bool
	const readers = 4
	// The write side is SINGLE-FLIGHT by contract: exactly one writer
	// goroutine. Reads stay fully concurrent — with each other, with the
	// writer, and with Close.
	wg.Go(func() {
		for i := uint32(0); !stop.Load(); i++ {
			_ = addLedgers(h, Entry{Seq: 1_000_000 + i, Bytes: []byte("v")})
		}
	})
	for range readers {
		wg.Go(func() {
			for i := uint32(0); !stop.Load(); i++ {
				_, _ = readLedgerRaw(h, i%50)
			}
		})
		wg.Go(func() {
			for !stop.Load() {
				for _, err := range h.IterateLedgers(0, 49) {
					if err != nil {
						break
					}
				}
			}
		})
	}

	time.Sleep(50 * time.Millisecond)
	require.NoError(t, store.Close())
	stop.Store(true)
	wg.Wait()

	require.ErrorIs(t, addLedgers(h, Entry{Seq: 1, Bytes: []byte("v")}), stores.ErrStoreClosed)
}

// TestHotStore_AddLedgersEmptyBytes pins behavior on zero-length
// Bytes round-trip. zstd handles empty input; the value is stored
// and read back as empty.
func TestHotStore_AddLedgersEmptyBytes(t *testing.T) {
	h := openTestHotStore(t)
	require.NoError(t, addLedgers(h, Entry{Seq: 1, Bytes: nil}))
	got, err := readLedgerRaw(h, 1)
	require.NoError(t, err)
	assert.Empty(t, got)
}

// TestHotToColdMigration exercises the symmetric byte-convention:
// the hot store lends uncompressed bytes; cold.AppendLedger
// takes uncompressed bytes; the round-trip is byte-equal end to
// end. Regression guard for the double-compress hazard we fixed in
// the convention unification.
func TestHotToColdMigration(t *testing.T) {
	const firstSeq uint32 = 100
	const n = 5

	// Seed hot with N ledgers.
	hot := openTestHotStore(t)
	raws := make([][]byte, n)
	for i := range n {
		lcm, _ := makeRandomLedgerCloseMeta(firstSeq+uint32(i), 2)
		b, err := lcm.MarshalBinary()
		require.NoError(t, err)
		raws[i] = b
		require.NoError(t, addLedgers(hot, Entry{Seq: firstSeq + uint32(i), Bytes: b}))
	}

	// Stream hot → cold. No re-encoding step on the caller side.
	coldPath := filepath.Join(t.TempDir(), "migrated.pack")
	w, err := NewColdWriter(coldPath, firstSeq, ColdWriterOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	for i := range n {
		fromHot, err := readLedgerRaw(hot, firstSeq+uint32(i))
		require.NoError(t, err)
		require.NoError(t, w.AppendLedger(firstSeq+uint32(i), fromHot))
	}
	require.NoError(t, w.Commit())

	// Read back from cold; must byte-equal the original raws.
	c, err := OpenColdReader(coldPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	for i := range n {
		got, err := readLedgerRaw(c, firstSeq+uint32(i))
		require.NoError(t, err)
		assert.Equal(t, raws[i], got, "ledger %d byte-equality", firstSeq+uint32(i))
	}
}

func TestHotStore_XDRRoundTrip(t *testing.T) {
	const ledgerSeq uint32 = 12_345_678
	const txCount = 5

	lcm, wantHashes := makeRandomLedgerCloseMeta(ledgerSeq, txCount)
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)

	h := openTestHotStore(t)
	require.NoError(t, addLedgers(h, Entry{Seq: ledgerSeq, Bytes: raw}))

	gotRaw, err := readLedgerRaw(h, ledgerSeq)
	require.NoError(t, err)
	assert.Equal(t, raw, gotRaw, "stored bytes must come back verbatim")

	var decoded xdr.LedgerCloseMeta
	require.NoError(t, decoded.UnmarshalBinary(gotRaw))

	require.NotNil(t, decoded.V1)
	assert.Equal(t, xdr.Uint32(ledgerSeq), decoded.V1.LedgerHeader.Header.LedgerSeq)

	require.NotNil(t, decoded.V1.TxSet.V1TxSet)
	require.Len(t, decoded.V1.TxSet.V1TxSet.Phases, 1)
	comps := decoded.V1.TxSet.V1TxSet.Phases[0].V0Components
	require.NotNil(t, comps)
	require.Len(t, *comps, 1)
	gotEnvs := (*comps)[0].TxsMaybeDiscountedFee.Txs
	require.Len(t, gotEnvs, txCount)

	gotHashes := make([][32]byte, len(gotEnvs))
	for i, env := range gotEnvs {
		h, err := network.HashTransactionInEnvelope(env, network.TestNetworkPassphrase)
		require.NoError(t, err)
		gotHashes[i] = h
	}
	assert.Equal(t, wantHashes, gotHashes, "tx hashes must match across marshal/unmarshal")
}

// makeRandomLedgerCloseMeta builds a barebones LedgerCloseMetaV1
// carrying txCount random transactions and returns it plus the
// per-tx envelope hashes under the test-network passphrase.
// Shared fixture for hot + cold store tests in this package.
func makeRandomLedgerCloseMeta(
	ledgerSeq uint32,
	txCount int,
) (xdr.LedgerCloseMeta, [][32]byte) {
	const networkPassphrase = network.TestNetworkPassphrase
	envs := make([]xdr.TransactionEnvelope, 0, txCount)
	hashes := make([][32]byte, 0, txCount)
	metas := make([]xdr.TransactionResultMeta, 0, txCount)
	const seqBase = 123_456
	for i := range txCount {
		txEnv := xdr.TransactionEnvelope{
			Type: xdr.EnvelopeTypeEnvelopeTypeTx,
			V1: &xdr.TransactionV1Envelope{
				Tx: xdr.Transaction{
					SourceAccount: xdr.MustMuxedAddress(keypair.MustRandom().Address()),
					Operations:    []xdr.Operation{},
					Fee:           xdr.Uint32(seqBase + i),
					SeqNum:        xdr.SequenceNumber(seqBase + i),
				},
			},
		}
		hash, err := network.HashTransactionInEnvelope(txEnv, networkPassphrase)
		if err != nil {
			panic(err)
		}
		envs = append(envs, txEnv)
		hashes = append(hashes, hash)
		metas = append(metas, xdr.TransactionResultMeta{
			Result: xdr.TransactionResultPair{
				TransactionHash: xdr.Hash(hash),
				Result: xdr.TransactionResult{
					FeeCharged: 100,
					Result: xdr.TransactionResultResult{
						Code:    xdr.TransactionResultCodeTxSuccess,
						Results: &[]xdr.OperationResult{},
					},
				},
			},
			TxApplyProcessing: xdr.TransactionMeta{V: 3, V3: &xdr.TransactionMetaV3{}},
		})
	}
	lcm := xdr.LedgerCloseMeta{
		V: 1,
		V1: &xdr.LedgerCloseMetaV1{
			TxProcessing: metas,
			TxSet: xdr.GeneralizedTransactionSet{
				V: 1,
				V1TxSet: &xdr.TransactionSetV1{
					Phases: []xdr.TransactionPhase{{
						V: 0,
						V0Components: &[]xdr.TxSetComponent{{
							TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{
								Txs: envs,
							},
						}},
					}},
				},
			},
		},
	}
	lcm.V1.LedgerHeader.Header.LedgerSeq = xdr.Uint32(ledgerSeq)
	return lcm, hashes
}

// addLedgers commits entries through AddLedgerToBatch in one batch — the
// production write shape, reduced to a test seeding call.
func addLedgers(h *HotStore, entries ...Entry) error {
	return translateRocksErr(h.store.Batch(func(b *rocksdb.BatchWriter) error {
		for _, e := range entries {
			if err := h.AddLedgerToBatch(b, e); err != nil {
				return err
			}
		}
		return nil
	}))
}

// readLedgerRaw is the owning read the stores no longer expose: WithLedger plus
// the copy. Nothing served keeps a whole ledger, so the copy belongs to the
// tests that assert on one rather than to an API sitting next to the pooled read.
func readLedgerRaw(r interface {
	WithLedger(seq uint32, fn func(raw []byte) error) error
}, seq uint32,
) ([]byte, error) {
	var out []byte
	err := r.WithLedger(seq, func(raw []byte) error {
		out = bytes.Clone(raw)
		return nil
	})
	return out, err
}

// TestStartCompress_StateSurvivesGC pins the owned-state guarantee that
// replaced the sync.Pool: the encode state (CGo context + retained dst
// buffer) must survive arbitrary GC cycles between ledgers. The pool
// predecessor was measured losing it ~1-in-5 ledgers to sync.Pool's
// GC-emptying, re-allocating a worst-case dst (~15MB at stress) each time.
func TestStartCompress_StateSurvivesGC(t *testing.T) {
	h := NewWithStore(nil, DefaultZstdEncodeWorkers)        // StartCompress/Discard never touch the store
	payload := bytes.Repeat([]byte("ledger bytes "), 1<<16) // ~832KB

	warm := h.StartCompress(Entry{Seq: 1, Bytes: payload})
	warm.Discard()

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for i := range 20 {
		runtime.GC()
		runtime.GC() // two cycles: what evicted the sync.Pool state
		p := h.StartCompress(Entry{Seq: uint32(i + 2), Bytes: payload})
		p.Discard()
	}
	runtime.ReadMemStats(&after)
	// 20 encodes may allocate goroutine/pending scaffolding (KBs) but must
	// never re-allocate a dst-buffer-sized block.
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(len(payload)),
		"encode state was dropped and re-allocated across GC cycles")
}

// TestStartCompress_SingleFlightGuard pins the narrowed write contract: a
// second in-flight compression panics loudly instead of racing the owned
// state; the latch releases on join/Discard.
func TestStartCompress_SingleFlightGuard(t *testing.T) {
	h := NewWithStore(nil, DefaultZstdEncodeWorkers)
	payload := []byte("x")

	first := h.StartCompress(Entry{Seq: 1, Bytes: payload})
	require.Panics(t, func() { h.StartCompress(Entry{Seq: 2, Bytes: payload}) },
		"second in-flight StartCompress must trip the single-flight latch")
	first.Discard()

	third := h.StartCompress(Entry{Seq: 3, Bytes: payload})
	third.Discard() // latch released by the join — reusable again
}

// TestCFNamesCarriesTheSpanTableCF pins the CF list the shared hot DB opens
// with: a facade that forgets a CF cannot store into it.
func TestCFNamesCarriesTheSpanTableCF(t *testing.T) {
	assert.Equal(t, []string{LedgersCF, TxSpansCF}, CFNames())
}

// TestWithTxTable_AbsentTableIsErrNoTable pins the ordinary case: a ledger
// without a span table reports the sentinel, which is how a reader learns to
// decode instead.
func TestWithTxTable_AbsentTableIsErrNoTable(t *testing.T) {
	h := openTestHotStore(t)
	raw := []byte("a ledger the store has no table for")
	require.NoError(t, h.store.Batch(func(b *rocksdb.BatchWriter) error {
		return h.AddLedgerToBatch(b, Entry{Seq: 7, Bytes: raw})
	}))

	require.ErrorIs(t, h.WithTxTable(7, func(txspan.Table, txspan.LedgerHeader, txspan.PieceReader) error {
		t.Fatal("fn must not run for a ledger with no table")
		return nil
	}), stores.ErrNoTable)
}

// TestWithTxTable_TableWithoutItsLedgerIsNotFound pins the other half of the
// paired read: a table whose ledger is gone is a missing ledger, not a missing
// table, so the caller's fallback reports the same thing.
func TestWithTxTable_TableWithoutItsLedgerIsNotFound(t *testing.T) {
	h := openTestHotStore(t)
	_, table := ledgerWithTable(t, 21)
	require.NoError(t, h.store.Batch(func(b *rocksdb.BatchWriter) error {
		h.AddTableToBatch(b, 21, table)
		return nil
	}))
	require.ErrorIs(t, h.WithTxTable(21, func(txspan.Table, txspan.LedgerHeader, txspan.PieceReader) error {
		t.Fatal("fn must not run without the ledger")
		return nil
	}), stores.ErrNotFound)
}

// TestWithTxTable_PiecesEqualTheRawSpans is the read path's core claim: the
// bytes a piece read returns for every transaction are exactly the bytes the
// same spans name in the decoded ledger.
func TestWithTxTable_PiecesEqualTheRawSpans(t *testing.T) {
	h := openTestHotStore(t)
	lcm, _ := makeRandomLedgerCloseMeta(9, 24)
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	putLedgerWithTable(t, h, 9, raw)

	var rows int
	require.NoError(t, h.WithTxTable(9, func(tbl txspan.Table, _ txspan.LedgerHeader, pieces txspan.PieceReader) error {
		require.Equal(t, uint32(9), tbl.LedgerSeq())
		require.Equal(t, 1, tbl.FrameCount(), "a ledger inside the window is one frame")
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
	assert.Equal(t, 24, rows)
}

// TestWithTxTable_UnparsableTableIsAnError pins that a table this store wrote
// and cannot read back fails the read naming the ledger and the reason. It is
// NOT reported as an absent table: the ledger beside it could answer, and
// answering would leave the operator with no sign that a stored table is bad.
func TestWithTxTable_UnparsableTableIsAnError(t *testing.T) {
	h := openTestHotStore(t)
	raw, table := ledgerWithTable(t, 11)
	table[len(table)-1] ^= 0x01 // a flipped trailer byte fails the checksum
	require.NoError(t, h.store.Batch(func(b *rocksdb.BatchWriter) error {
		h.AddTableToBatch(b, 11, table)
		return h.AddLedgerToBatch(b, Entry{Seq: 11, Bytes: raw})
	}))

	err := h.WithTxTable(11, func(txspan.Table, txspan.LedgerHeader, txspan.PieceReader) error {
		t.Fatal("fn must not run for a table that will not parse")
		return nil
	})
	require.ErrorIs(t, err, stores.ErrCorrupt)
	require.NotErrorIs(t, err, stores.ErrNoTable, "a bad table is not an absent one")
	assert.ErrorContains(t, err, "ledger 11")
	assert.ErrorContains(t, err, "checksum mismatch")
}

// TestAddTableToBatch_EmptyTableCountsASkip pins that a ledger the build
// refused stores nothing and is tallied apart from the written ones.
func TestAddTableToBatch_EmptyTableCountsASkip(t *testing.T) {
	h := openTestHotStore(t)
	skipped, written := TablesSkipped(), TablesWritten()
	require.NoError(t, h.store.Batch(func(b *rocksdb.BatchWriter) error {
		h.AddTableToBatch(b, 13, nil)
		return h.AddLedgerToBatch(b, Entry{Seq: 13, Bytes: []byte("no table for this one")})
	}))
	assert.Equal(t, skipped+1, TablesSkipped())
	assert.Equal(t, written, TablesWritten())

	require.ErrorIs(t, h.WithTxTable(13, func(txspan.Table, txspan.LedgerHeader, txspan.PieceReader) error { return nil }),
		stores.ErrNoTable)
}

// TestAddTableToBatch_EmptyTableDeletesThePriorRow pins the table family's own
// rule through a replay: a table under a key always describes the ledger
// stored beside it. Re-ingesting a sequence whose build refuses a table must
// therefore remove the table the earlier ingest left there, rather than leave
// it standing over bytes it no longer describes.
func TestAddTableToBatch_EmptyTableDeletesThePriorRow(t *testing.T) {
	h := openTestHotStore(t)
	lcm, _ := makeRandomLedgerCloseMeta(17, 4)
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	putLedgerWithTable(t, h, 17, raw)
	require.NoError(t, h.WithTxTable(17, func(txspan.Table, txspan.LedgerHeader, txspan.PieceReader) error { return nil }),
		"premise: the first ingest stored a readable table")

	require.NoError(t, h.store.Batch(func(b *rocksdb.BatchWriter) error {
		h.AddTableToBatch(b, 17, nil)
		return h.AddLedgerToBatch(b, Entry{Seq: 17, Bytes: raw})
	}))
	require.ErrorIs(t, h.WithTxTable(17, func(txspan.Table, txspan.LedgerHeader, txspan.PieceReader) error {
		t.Fatal("the earlier table must not survive a ledger re-ingested without one")
		return nil
	}), stores.ErrNoTable)
}

// TestWithTxTable_CallbackErrorReachesTheCaller pins that fn's error is
// carried out rather than swallowed by the pinned read.
func TestWithTxTable_CallbackErrorReachesTheCaller(t *testing.T) {
	h := openTestHotStore(t)
	putLedgerWithTable(t, h, 15, zeroTxLedger(t, 15))

	sentinel := errors.New("callback said no")
	require.ErrorIs(t, h.WithTxTable(15,
		func(txspan.Table, txspan.LedgerHeader, txspan.PieceReader) error { return sentinel }),
		sentinel)
}

// TestHotStore_MissingTableFamilyFailsEveryOpen pins the family as part of
// what a hot DB IS. A DB without it is not this facade's DB, and both opens
// that take a DB as they find it — the read-only one the freeze and the
// startup refiner use, and the must-exist one ingestion resumes with — refuse
// it rather than serving its tables as absent. A read that answered ErrNoTable
// for a whole missing family would turn "this is the wrong database" into
// "these ledgers have no tables", which is a slower answer and a silent one.
func TestHotStore_MissingTableFamilyFailsEveryOpen(t *testing.T) {
	dir := t.TempDir()
	// A DB created with the ledgers family alone, which is not a hot ledger DB.
	store, err := rocksdb.New(rocksdb.Config{
		Path: dir, ColumnFamilies: []string{LedgersCF}, Logger: silentLogger(),
	})
	require.NoError(t, err)
	h := NewWithStore(store, 0)
	require.NoError(t, store.Batch(func(b *rocksdb.BatchWriter) error {
		return h.AddLedgerToBatch(b, Entry{Seq: 31, Bytes: zeroTxLedger(t, 31)})
	}))
	require.NoError(t, store.Close())

	for name, cfg := range map[string]rocksdb.Config{
		"read-only": {
			Path: dir, ColumnFamilies: CFNames(), Logger: silentLogger(), ReadOnly: true,
		},
		"must-exist": {
			Path: dir, ColumnFamilies: CFNames(), Logger: silentLogger(), MustExist: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, oerr := rocksdb.New(cfg)
			require.Error(t, oerr, "a DB without the table family is not this facade's DB")
			assert.ErrorContains(t, oerr, TxSpansCF)
		})
	}
}

// TestHotStore_FreshWriteOpenCreatesTheTableFamily pins the other side of the
// same rule: a store opened fresh on this facade's CF list HAS the table
// family, so ingest can write into it and a read finds it there. Every hot DB
// in the deployment starts this way.
func TestHotStore_FreshWriteOpenCreatesTheTableFamily(t *testing.T) {
	h := openTestHotStore(t)
	lcm, _ := makeRandomLedgerCloseMeta(34, 4)
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	putLedgerWithTable(t, h, 34, raw)

	require.NoError(t, h.WithTxTable(34, func(tbl txspan.Table, _ txspan.LedgerHeader, pieces txspan.PieceReader) error {
		row := tbl.Row(0)
		env, _, perr := pieces(row)
		require.NoError(t, perr)
		assert.Equal(t, raw[row.EnvStart:row.EnvEnd], env)
		return nil
	}))
}

// zeroTxLedger marshals a minimal V2 LedgerCloseMeta holding no transactions.
func zeroTxLedger(t *testing.T, seq uint32) []byte {
	t.Helper()
	lcm := xdr.LedgerCloseMeta{
		V: 2,
		V2: &xdr.LedgerCloseMetaV2{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{LedgerSeq: xdr.Uint32(seq)},
			},
			TxSet: xdr.GeneralizedTransactionSet{V: 1, V1TxSet: &xdr.TransactionSetV1{}},
		},
	}
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	return raw
}

// TestWithTxTable_SpansStraddlingAFrameCut pins the piece reader's hardest
// case: with the window low enough that a ledger is cut several times, a span
// crossing a cut must still come back whole. The window is lowered rather than
// a four-megabyte fixture built, but every other part of the path — the
// directory, the compressed offsets, the decode — is the production one.
func TestWithTxTable_SpansStraddlingAFrameCut(t *testing.T) {
	h := openTestHotStore(t)
	lcm, _ := makeRandomLedgerCloseMeta(10, 64)
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	// A window a few hundred bytes wide cuts between (and through) envelopes
	// and elements alike.
	withFrameWindow(t, 512)
	require.Greater(t, len(raw), 4*512, "the fixture must span several frames")
	putLedgerWithTable(t, h, 10, raw)

	straddled := 0
	require.NoError(t, h.WithTxTable(10, func(tbl txspan.Table, _ txspan.LedgerHeader, pieces txspan.PieceReader) error {
		require.Greater(t, tbl.FrameCount(), 4, "the value must be cut into several frames")
		for i := range tbl.TxCount() {
			row := tbl.Row(i)
			env, elem, perr := pieces(row)
			require.NoError(t, perr)
			assert.Equal(t, raw[row.EnvStart:row.EnvEnd], env, "envelope %d", i)
			assert.Equal(t, raw[row.ElemStart:row.ElemEnd], elem, "element %d", i)
			first, _, _, _ := tbl.RawOffsetFrame(row.EnvStart)
			last, _, _, _ := tbl.RawOffsetFrame(row.EnvEnd - 1)
			if first != last {
				straddled++
			}
		}
		return nil
	}))
	assert.Positive(t, straddled, "no envelope crossed a cut; the fixture proves nothing")
}

// TestStartCompress_SingleFrameValueIsUnchanged pins the byte-identity floor:
// a ledger inside the window is compressed to exactly what the plain encoder
// produces, and reports one frame.
func TestStartCompress_SingleFrameValueIsUnchanged(t *testing.T) {
	h := openTestHotStore(t)
	lcm, _ := makeRandomLedgerCloseMeta(41, 8)
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)

	pending := h.StartCompress(Entry{Seq: 41, Bytes: raw})
	require.NoError(t, h.store.Batch(func(b *rocksdb.BatchWriter) error {
		return h.AddPendingToBatch(b, pending)
	}))
	require.Len(t, pending.Frames(), 1)
	assert.Equal(t, uint32(len(raw)), pending.Frames()[0].Raw)

	want, err := zstd.NewEncoderState().Encode(raw)
	require.NoError(t, err)
	var got []byte
	_, err = h.store.GetPinned(LedgersCF, rocksdb.EncodeUint32(41), func(v []byte) error {
		got = bytes.Clone(v)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, want, got, "a ledger inside the window must encode to the pre-frames bytes")
}

// TestStartCompress_FramedValueRoundTripsWhole pins that framing changes only
// how a value is stored: every whole-ledger read still hands back the exact
// ledger bytes.
func TestStartCompress_FramedValueRoundTripsWhole(t *testing.T) {
	h := openTestHotStore(t)
	withFrameWindow(t, 1024)
	lcm, _ := makeRandomLedgerCloseMeta(42, 48)
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	putLedgerWithTable(t, h, 42, raw)

	require.NoError(t, h.WithLedger(42, func(got []byte) error {
		assert.Equal(t, raw, got)
		return nil
	}))
	for e, ierr := range h.IterateLedgers(42, 42) {
		require.NoError(t, ierr)
		assert.Equal(t, raw, e.Bytes)
	}
}

// withFrameWindow lowers the store's frame window for one test, so a fixture
// small enough to build in memory is still cut into several frames.
func withFrameWindow(t *testing.T, window int) {
	t.Helper()
	previous := frameWindow
	frameWindow = window
	t.Cleanup(func() { frameWindow = previous })
}

// putLedgerWithTable commits one ledger the way the ingest loop does: the
// forked compression, the span table built beside it, and the value's frame
// directory stamped into that table before both land in one batch.
func putLedgerWithTable(t *testing.T, h *HotStore, seq uint32, raw []byte) {
	t.Helper()
	pending := h.StartCompress(Entry{Seq: seq, Bytes: raw})
	txParts, err := sdkingest.ExtractLedgerTxParts(xdr.LedgerCloseMetaView(raw))
	require.NoError(t, err)
	table, err := txspan.Build(raw, txParts, network.TestNetworkPassphrase)
	require.NoError(t, err)
	require.NoError(t, h.store.Batch(func(b *rocksdb.BatchWriter) error {
		if perr := h.AddPendingToBatch(b, pending); perr != nil {
			return perr
		}
		stamped, serr := txspan.WithFrames(table, pending.Frames())
		if serr != nil {
			return serr
		}
		h.AddTableToBatch(b, seq, stamped)
		return nil
	}))
}

// ledgerWithTable returns a zero-transaction ledger and the span table built
// for it, so the store tests exercise real encoded tables rather than
// stand-ins. The table carries NO frame directory — nothing stamps one here —
// so it is for tests that never read it back through the piece reader;
// putLedgerWithTable is the readable one.
func ledgerWithTable(t *testing.T, seq uint32) ([]byte, []byte) {
	t.Helper()
	raw := zeroTxLedger(t, seq)
	txParts, err := sdkingest.ExtractLedgerTxParts(xdr.LedgerCloseMetaView(raw))
	require.NoError(t, err)
	table, err := txspan.Build(raw, txParts, network.PublicNetworkPassphrase)
	require.NoError(t, err)
	return raw, table
}
