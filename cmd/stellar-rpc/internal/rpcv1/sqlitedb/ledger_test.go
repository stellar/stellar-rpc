package sqlitedb

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

var (
	passphrase = network.FutureNetworkPassphrase
	logger     = log.DefaultLogger
)

func createLedger(ledgerSequence uint32) xdr.LedgerCloseMeta {
	return xdr.LedgerCloseMeta{
		V: 1,
		V1: &xdr.LedgerCloseMetaV1{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Hash: xdr.Hash{},
				Header: xdr.LedgerHeader{
					LedgerSeq: xdr.Uint32(ledgerSequence),
				},
			},
			TxSet: xdr.GeneralizedTransactionSet{
				V:       1,
				V1TxSet: &xdr.TransactionSetV1{},
			},
		},
	}
}

func assertLedgerRange(t *testing.T, reader LedgerReader, start, end uint32) {
	ctx := t.Context()
	var allLedgers []xdr.LedgerCloseMeta
	err := reader.StreamLedgerRange(ctx, start-1, end+1, func(txmeta xdr.LedgerCloseMeta) error {
		allLedgers = append(allLedgers, txmeta)
		return nil
	})
	require.NoError(t, err)
	for i := start - 1; i <= end+1; i++ {
		ledger, exists, err := reader.GetLedger(ctx, i)
		require.NoError(t, err)
		if i < start || i > end {
			assert.False(t, exists)
			continue
		}
		assert.True(t, exists)
		ledgerBinary, err := ledger.MarshalBinary()
		require.NoError(t, err)
		expected := createLedger(i)
		expectedBinary, err := expected.MarshalBinary()
		require.NoError(t, err)
		assert.Equal(t, expectedBinary, ledgerBinary)

		ledgerBinary, err = allLedgers[0].MarshalBinary()
		require.NoError(t, err)
		assert.Equal(t, expectedBinary, ledgerBinary)
		allLedgers = allLedgers[1:]
	}
	assert.Empty(t, allLedgers)
}

func TestLedgers(t *testing.T) {
	db := NewTestDB(t)
	daemon := host.MakeNoOpDaemon()

	reader := NewLedgerReader(db)
	_, exists, err := reader.GetLedger(t.Context(), 1)
	require.NoError(t, err)
	assert.False(t, exists)

	for i := 1; i <= 10; i++ {
		ledgerSequence := uint32(i)
		tx, err := NewReadWriter(logger, db, daemon, 15, passphrase).NewTx(t.Context())
		require.NoError(t, err)

		ledgerCloseMeta := createLedger(ledgerSequence)
		require.NoError(t, tx.LedgerWriter().InsertLedger(ledgerCloseMeta))
		require.NoError(t, tx.Commit(ledgerCloseMeta, nil))
		// rolling back after a commit is a no-op
		require.NoError(t, tx.Rollback())
	}

	assertLedgerRange(t, reader, 1, 10)

	ledgerSequence := uint32(11)
	tx, err := NewReadWriter(logger, db, daemon, 15, passphrase).NewTx(t.Context())
	require.NoError(t, err)
	ledgerCloseMeta := createLedger(ledgerSequence)
	require.NoError(t, tx.LedgerWriter().InsertLedger(ledgerCloseMeta))
	require.NoError(t, tx.Commit(ledgerCloseMeta, nil))

	assertLedgerRange(t, reader, 1, 11)

	ledgerSequence = uint32(12)
	tx, err = NewReadWriter(logger, db, daemon, 5, passphrase).NewTx(t.Context())
	require.NoError(t, err)
	ledgerCloseMeta = createLedger(ledgerSequence)
	require.NoError(t, tx.LedgerWriter().InsertLedger(ledgerCloseMeta))
	require.NoError(t, tx.Commit(ledgerCloseMeta, nil))

	assertLedgerRange(t, reader, 8, 12)
}

// TestLedgerInfoFromRow_PrefixFallback covers a meta prefix too short to reach
// the close time, which must fall back to reading the full blob.
func TestLedgerInfoFromRow_PrefixFallback(t *testing.T) {
	db := NewTestDB(t)
	tx, err := NewReadWriter(logger, db, host.MakeNoOpDaemon(), 15, passphrase).NewTx(t.Context())
	require.NoError(t, err)
	lcm := createLedger(42)
	require.NoError(t, tx.LedgerWriter().InsertLedger(lcm))
	require.NoError(t, tx.Commit(lcm, nil))

	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)

	info, err := ledgerInfoFromRow(t.Context(), db, ledgerRangeRow{Sequence: 42, MetaPrefix: raw[:8]})
	require.NoError(t, err)
	assert.Equal(t, uint32(42), info.Sequence)
	assert.Equal(t, lcm.LedgerCloseTime(), info.CloseTime)
}

func TestGetLedgerRange_NonEmptyDB(t *testing.T) {
	db := NewTestDB(t)
	ctx := context.TODO()

	writer := NewReadWriter(logger, db, host.MakeNoOpDaemon(), 10, passphrase)
	write, err := writer.NewTx(ctx)
	require.NoError(t, err)

	lcms := []xdr.LedgerCloseMeta{
		txMeta(1234, true),
		txMeta(1235, true),
		txMeta(1236, true),
		txMeta(1237, true),
	}

	ledgerW, txW := write.LedgerWriter(), write.TransactionWriter()
	for _, lcm := range lcms {
		require.NoError(t, ledgerW.InsertLedger(lcm), "ingestion failed for ledger %+v", lcm.V1)
		require.NoError(t, txW.InsertTransactions(lcm), "ingestion failed for ledger %+v", lcm.V1)
	}
	require.NoError(t, write.Commit(lcms[len(lcms)-1], nil))

	reader := NewLedgerReader(db)
	ledgerRange, err := reader.GetLedgerRange(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint32(1334), ledgerRange.FirstLedger.Sequence)
	assert.Equal(t, ledgerCloseTime(1334), ledgerRange.FirstLedger.CloseTime)
	assert.Equal(t, uint32(1337), ledgerRange.LastLedger.Sequence)
	assert.Equal(t, ledgerCloseTime(1337), ledgerRange.LastLedger.CloseTime)
}

func TestGetLedgerRange_SingleDBRow(t *testing.T) {
	db := NewTestDB(t)
	ctx := t.Context()

	writer := NewReadWriter(logger, db, host.MakeNoOpDaemon(), 10, passphrase)
	write, err := writer.NewTx(ctx)
	require.NoError(t, err)

	lcms := []xdr.LedgerCloseMeta{
		txMeta(1234, true),
	}

	ledgerW, txW := write.LedgerWriter(), write.TransactionWriter()
	for _, lcm := range lcms {
		require.NoError(t, ledgerW.InsertLedger(lcm), "ingestion failed for ledger %+v", lcm.V1)
		require.NoError(t, txW.InsertTransactions(lcm), "ingestion failed for ledger %+v", lcm.V1)
	}
	require.NoError(t, write.Commit(lcms[len(lcms)-1], nil))

	reader := NewLedgerReader(db)
	ledgerRange, err := reader.GetLedgerRange(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint32(1334), ledgerRange.FirstLedger.Sequence)
	assert.Equal(t, ledgerCloseTime(1334), ledgerRange.FirstLedger.CloseTime)
	assert.Equal(t, uint32(1334), ledgerRange.LastLedger.Sequence)
	assert.Equal(t, ledgerCloseTime(1334), ledgerRange.LastLedger.CloseTime)
}

// TestGetLedgerRange_OldestCacheInvalidatedOnTrim verifies that the cached
// oldest-ledger scalars are refreshed once the retention window trims the
// ledger they describe -- so GetLedgerRange keeps reporting the true oldest
// ledger rather than a stale cached one, while still avoiding the per-call
// oldest-ledger decode in steady state.
func TestGetLedgerRange_OldestCacheInvalidatedOnTrim(t *testing.T) {
	const retentionWindow = 10
	db := NewTestDB(t)
	ctx := context.TODO()
	writer := NewReadWriter(logger, db, host.MakeNoOpDaemon(), retentionWindow, passphrase)
	reader := NewLedgerReader(db)

	ingest := func(base uint32, count int) {
		write, err := writer.NewTx(ctx)
		require.NoError(t, err)
		ledgerW, txW := write.LedgerWriter(), write.TransactionWriter()
		var last xdr.LedgerCloseMeta
		for i := range count {
			lcm := txMeta(base+uint32(i), true)
			require.NoError(t, ledgerW.InsertLedger(lcm))
			require.NoError(t, txW.InsertTransactions(lcm))
			last = lcm
		}
		require.NoError(t, write.Commit(last, nil))
	}

	// Phase 1: ingest exactly the retention window (sequences 1334..1343); no
	// trimming yet, oldest = 1334. The read populates the oldest cache.
	ingest(1234, retentionWindow)
	ledgerRange, err := reader.GetLedgerRange(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint32(1334), ledgerRange.FirstLedger.Sequence)
	assert.Equal(t, ledgerCloseTime(1334), ledgerRange.FirstLedger.CloseTime)
	assert.Equal(t, uint32(1343), ledgerRange.LastLedger.Sequence)

	// Phase 2: ingest 5 more (sequences 1344..1348). With retention 10 and
	// latest 1348, the cutoff is 1339, trimming 1334..1338 -- which includes the
	// cached oldest (1334), so the cache must invalidate and the next read must
	// report the new oldest (1339), not the stale 1334.
	ingest(1244, 5)
	ledgerRange, err = reader.GetLedgerRange(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint32(1339), ledgerRange.FirstLedger.Sequence)
	assert.Equal(t, ledgerCloseTime(1339), ledgerRange.FirstLedger.CloseTime)
	assert.Equal(t, uint32(1348), ledgerRange.LastLedger.Sequence)
	assert.Equal(t, ledgerCloseTime(1348), ledgerRange.LastLedger.CloseTime)
}

func TestGetLedgerRange_EmptyDB(t *testing.T) {
	db := NewTestDB(t)
	ctx := context.TODO()

	reader := NewLedgerReader(db)
	ledgerRange, err := reader.GetLedgerRange(ctx)
	assert.Equal(t, store.ErrEmptyDB, err)
	assert.Equal(t, uint32(0), ledgerRange.FirstLedger.Sequence)
	assert.Equal(t, int64(0), ledgerRange.FirstLedger.CloseTime)
	assert.Equal(t, uint32(0), ledgerRange.LastLedger.Sequence)
	assert.Equal(t, int64(0), ledgerRange.LastLedger.CloseTime)
}

// TestWithLedgerRaw covers both lend outcomes: a hit lends the stored meta
// blob verbatim, and a miss reports found=false without running fn.
func TestWithLedgerRaw(t *testing.T) {
	db := NewTestDB(t)
	tx, err := NewReadWriter(logger, db, host.MakeNoOpDaemon(), 15, passphrase).NewTx(t.Context())
	require.NoError(t, err)
	lcm := createLedger(42)
	require.NoError(t, tx.LedgerWriter().InsertLedger(lcm))
	require.NoError(t, tx.Commit(lcm, nil))
	want, err := lcm.MarshalBinary()
	require.NoError(t, err)

	reader := NewLedgerReader(db)
	var got []byte
	found, err := reader.WithLedgerRaw(t.Context(), 42, func(raw []byte) error {
		got = bytes.Clone(raw)
		return nil
	})
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, want, got)

	ran := false
	found, err = reader.WithLedgerRaw(t.Context(), 43, func([]byte) error {
		ran = true
		return nil
	})
	require.NoError(t, err)
	assert.False(t, found)
	assert.False(t, ran)
}

func BenchmarkGetLedgerRange(b *testing.B) {
	testDB, lcms := setupBenchmarkingDB(b)
	reader := NewLedgerReader(testDB)

	for b.Loop() {
		ledgerRange, err := reader.GetLedgerRange(context.TODO())
		require.NoError(b, err)
		assert.Equal(b, lcms[0].LedgerSequence(), ledgerRange.FirstLedger.Sequence)
		assert.Equal(b, lcms[len(lcms)-1].LedgerSequence(), ledgerRange.LastLedger.Sequence)
	}
}

func BenchmarkBatchGetLedgers(b *testing.B) {
	testDB, lcms := setupBenchmarkingDB(b)
	reader := NewLedgerReader(testDB)
	readTx, err := reader.NewTx(b.Context())
	require.NoError(b, err)
	batchSize := uint(200) // using the current maximum value for getLedgers endpoint

	start := uint32(1334)
	end := start + uint32(batchSize) - 1

	for b.Loop() {
		ledgers, err := readTx.BatchGetLedgers(b.Context(), start, end)
		require.NoError(b, err)

		var hdrFirst, hdrLast xdr.LedgerHeaderHistoryEntry
		require.NoError(b, hdrFirst.UnmarshalBinary(ledgers[0].HeaderRaw))
		require.NoError(b, hdrLast.UnmarshalBinary(ledgers[batchSize-1].HeaderRaw))
		assert.EqualValues(b, lcms[0].LedgerSequence(), hdrFirst.Header.LedgerSeq)
		assert.EqualValues(b, lcms[batchSize-1].LedgerSequence(), hdrLast.Header.LedgerSeq)
	}
}

// padLedger grows a txMeta ledger's meta to roughly size bytes via its soroban return value.
func padLedger(lcm xdr.LedgerCloseMeta, size int) xdr.LedgerCloseMeta {
	payload := xdr.ScBytes(make([]byte, size))
	lcm.V2.TxProcessing[0].TxApplyProcessing.V3.SorobanMeta.ReturnValue = xdr.ScVal{
		Type:  xdr.ScValTypeScvBytes,
		Bytes: &payload,
	}
	return lcm
}

// BenchmarkOldestLedgerRangeLookup measures the 1KiB prefix fetch in
// getLedgerRangeWithCache. The tx read path (getLedgers/getTransactions) runs
// this lookup once per request.
func BenchmarkOldestLedgerRangeLookup(b *testing.B) {
	for _, tc := range []struct {
		name string
		size int
	}{
		// min/avg/max meta blob sizes observed on a pubnet 7-day node
		{"512KiB", 512 << 10},
		{"2MiB", 2 << 20},
		{"4MiB", 4 << 20},
	} {
		ctx := b.Context()
		testDB := NewTestDB(b)
		writer := NewReadWriter(logger, testDB, host.MakeNoOpDaemon(), 1_000_000, passphrase)
		write, err := writer.NewTx(ctx)
		require.NoError(b, err)

		lcms := []xdr.LedgerCloseMeta{
			padLedger(txMeta(1000, true), tc.size),
			padLedger(txMeta(1001, true), tc.size),
		}
		ledgerW, txW := write.LedgerWriter(), write.TransactionWriter()
		for _, lcm := range lcms {
			require.NoError(b, ledgerW.InsertLedger(lcm))
			require.NoError(b, txW.InsertTransactions(lcm))
		}
		latest := lcms[len(lcms)-1]
		require.NoError(b, write.Commit(latest, nil))
		latestSeq, latestTime := latest.LedgerSequence(), latest.LedgerCloseTime()

		got, err := getLedgerRangeWithCache(ctx, testDB, latestSeq, latestTime)
		require.NoError(b, err)
		require.Equal(b, lcms[0].LedgerSequence(), got.FirstLedger.Sequence)

		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_, err := getLedgerRangeWithCache(ctx, testDB, latestSeq, latestTime)
				require.NoError(b, err)
			}
		})
	}
}

func NewTestDB(tb testing.TB) *DB {
	tmp := tb.TempDir()
	dbPath := path.Join(tmp, "db.sqlite")
	db, err := OpenSQLiteDB(dbPath)
	require.NoError(tb, err)
	tb.Cleanup(func() {
		require.NoError(tb, db.Close())
	})
	return db
}

func setupBenchmarkingDB(b *testing.B) (*DB, []xdr.LedgerCloseMeta) {
	testDB := NewTestDB(b)
	logger := log.DefaultLogger
	logger.SetOutput(io.Discard)

	writer := NewReadWriter(logger, testDB, host.MakeNoOpDaemon(),
		1_000_000, passphrase)
	write, err := writer.NewTx(b.Context())
	require.NoError(b, err)

	lcms := make([]xdr.LedgerCloseMeta, 0, 100_000)
	for i := range cap(lcms) {
		lcms = append(lcms, txMeta(uint32(1234+i), i%2 == 0))
	}

	ledgerW, txW := write.LedgerWriter(), write.TransactionWriter()
	for _, lcm := range lcms {
		require.NoError(b, ledgerW.InsertLedger(lcm))
		require.NoError(b, txW.InsertTransactions(lcm))
	}
	require.NoError(b, write.Commit(lcms[len(lcms)-1], nil))
	return testDB, lcms
}

// TestWithLedgerRaw pins the raw accessor against the decoding one: the blob it
// lends must re-marshal to exactly what GetLedger decodes, an absent ledger
// must report found=false without running fn, and a callback error must come
// back verbatim with found=true.
func TestWithLedgerRaw(t *testing.T) {
	db := NewTestDB(t)
	daemon := host.MakeNoOpDaemon()
	write := NewReadWriter(logger, db, daemon, 10, passphrase)
	for seq := uint32(1); seq <= 3; seq++ {
		tx, err := write.NewTx(t.Context())
		require.NoError(t, err)
		lcm := createLedger(seq)
		require.NoError(t, tx.LedgerWriter().InsertLedger(lcm))
		require.NoError(t, tx.Commit(lcm, nil))
	}

	reader := NewLedgerReader(db)
	readTx, err := reader.NewTx(t.Context())
	require.NoError(t, err)
	defer func() { _ = readTx.Done() }()

	for seq := uint32(1); seq <= 3; seq++ {
		lcm, ok, err := readTx.GetLedger(t.Context(), seq)
		require.NoError(t, err)
		require.True(t, ok)
		want, err := lcm.MarshalBinary()
		require.NoError(t, err)

		var got []byte
		found, err := readTx.WithLedgerRaw(t.Context(), seq, func(raw []byte) error {
			got = append([]byte(nil), raw...) // the loan forbids retaining raw
			return nil
		})
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, want, got, "ledger %d", seq)
	}

	ran := false
	found, err := readTx.WithLedgerRaw(t.Context(), 99, func([]byte) error {
		ran = true
		return nil
	})
	require.NoError(t, err)
	assert.False(t, found)
	assert.False(t, ran, "fn must not run for an absent ledger")

	boom := errors.New("boom")
	found, err = readTx.WithLedgerRaw(t.Context(), 1, func([]byte) error { return boom })
	assert.ErrorIs(t, err, boom)
	assert.True(t, found, "the ledger was there; only the callback failed")
}
