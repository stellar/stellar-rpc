package sqlitedb

import (
	"bytes"
	"context"
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
	var allLedgers [][]byte
	for l, err := range reader.ScanLedgers(ctx, start-1, end+1) {
		require.NoError(t, err)
		allLedgers = append(allLedgers, bytes.Clone(l.Raw)) // the loan forbids retaining Raw
	}
	for i := start - 1; i <= end+1; i++ {
		ledger, exists, err := store.GetLedger(ctx, reader, i)
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

		assert.Equal(t, expectedBinary, allLedgers[0])
		allLedgers = allLedgers[1:]
	}
	assert.Empty(t, allLedgers)
}

func TestLedgers(t *testing.T) {
	db := NewTestDB(t)
	daemon := host.MakeNoOpDaemon()

	reader := NewLedgerReader(db)
	_, exists, err := store.GetLedger(t.Context(), reader, 1)
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

// TestGetLedgerRange_OldestCachePublishedOnTrim verifies that a trimming
// commit itself publishes the new oldest ledger's scalars, so GetLedgerRange
// reports the true oldest without ever decoding it on the read path.
func TestGetLedgerRange_OldestCachePublishedOnTrim(t *testing.T) {
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

	cachedOldest := func() (uint32, int64) {
		db.cache.RLock()
		defer db.cache.RUnlock()
		return db.cache.firstLedgerSeq, db.cache.firstLedgerCloseTime
	}

	// Phase 1: ingest exactly the retention window (sequences 1334..1343); the
	// trim removes nothing, and the commit publishes oldest = 1334.
	ingest(1234, retentionWindow)
	seq, closeTime := cachedOldest()
	assert.Equal(t, uint32(1334), seq)
	assert.Equal(t, ledgerCloseTime(1334), closeTime)
	ledgerRange, err := reader.GetLedgerRange(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint32(1334), ledgerRange.FirstLedger.Sequence)
	assert.Equal(t, ledgerCloseTime(1334), ledgerRange.FirstLedger.CloseTime)
	assert.Equal(t, uint32(1343), ledgerRange.LastLedger.Sequence)

	// Phase 2: ingest 5 more (sequences 1344..1348). With retention 10 and
	// latest 1348, the cutoff is 1339, trimming 1334..1338 -- which includes the
	// cached oldest (1334), so the commit must publish 1339 before any read.
	ingest(1244, 5)
	seq, closeTime = cachedOldest()
	assert.Equal(t, uint32(1339), seq)
	assert.Equal(t, ledgerCloseTime(1339), closeTime)
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
	found, err := store.WithLedgerRaw(t.Context(), reader, 42, func(raw []byte) error {
		got = bytes.Clone(raw)
		return nil
	})
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, want, got)

	ran := false
	found, err = store.WithLedgerRaw(t.Context(), reader, 43, func([]byte) error {
		ran = true
		return nil
	})
	require.NoError(t, err)
	assert.False(t, found)
	assert.False(t, ran)
}

// TestScanLedgers pins what the handlers' gap checks rest on: ascending, duplicate-free,
// within [start, end], absent sequences skipped, and a start below the oldest row served from it.
func TestScanLedgers(t *testing.T) {
	db := NewTestDB(t)
	for _, seq := range []uint32{10, 11, 13, 14} { // 12 is missing
		tx, err := NewReadWriter(logger, db, host.MakeNoOpDaemon(), 15, passphrase).NewTx(t.Context())
		require.NoError(t, err)
		lcm := createLedger(seq)
		require.NoError(t, tx.LedgerWriter().InsertLedger(lcm))
		require.NoError(t, tx.Commit(lcm, nil))
	}
	readTx, err := NewLedgerReader(db).NewTx(t.Context())
	require.NoError(t, err)
	defer func() { _ = readTx.Done() }()

	scan := func(start, end uint32) []uint32 {
		var got []uint32
		for l, err := range readTx.ScanLedgers(t.Context(), start, end) {
			require.NoError(t, err)
			var lcm xdr.LedgerCloseMeta
			require.NoError(t, lcm.UnmarshalBinary(l.Raw))
			require.Equal(t, l.Sequence, lcm.LedgerSequence(), "Sequence must match the bytes")
			got = append(got, l.Sequence)
		}
		return got
	}
	assert.Equal(t, []uint32{10, 11, 13, 14}, scan(10, 14), "ascending, and the gap at 12 is silent")
	assert.Equal(t, []uint32{11, 13}, scan(11, 13), "bounded on both ends")
	assert.Equal(t, []uint32{10, 11}, scan(1, 11), "a start below the oldest row is served from it")
	assert.Empty(t, scan(12, 12), "an absent ledger yields nothing")
	assert.Empty(t, scan(14, 10), "start above end yields nothing")
	assert.Empty(t, scan(20, 30), "beyond the latest row yields nothing")
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

func BenchmarkScanLedgers(b *testing.B) {
	testDB, lcms := setupBenchmarkingDB(b)
	reader := NewLedgerReader(testDB)
	readTx, err := reader.NewTx(b.Context())
	require.NoError(b, err)
	batchSize := uint(200) // using the current maximum value for getLedgers endpoint

	start := uint32(1334)
	end := start + uint32(batchSize) - 1

	for b.Loop() {
		// The header slice is what getLedgers pulls off each scanned ledger.
		var first, last xdr.LedgerHeaderHistoryEntry
		count := 0
		for entry, err := range readTx.ScanLedgers(b.Context(), start, end) {
			require.NoError(b, err)
			headerView, herr := xdr.LedgerCloseMetaView(entry.Raw).LedgerHeader()
			require.NoError(b, herr)
			raw, rerr := headerView.Raw()
			require.NoError(b, rerr)
			switch count {
			case 0:
				require.NoError(b, first.UnmarshalBinary(raw))
			case int(batchSize) - 1:
				require.NoError(b, last.UnmarshalBinary(raw))
			}
			count++
		}
		require.Equal(b, int(batchSize), count)
		assert.EqualValues(b, lcms[0].LedgerSequence(), first.Header.LedgerSeq)
		assert.EqualValues(b, lcms[batchSize-1].LedgerSequence(), last.Header.LedgerSeq)
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
