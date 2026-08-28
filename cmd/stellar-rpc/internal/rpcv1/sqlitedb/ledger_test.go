package sqlitedb

import (
	"context"
	"fmt"
	"io"
	"path"
	"runtime"
	"slices"
	"strings"
	"testing"

	sq "github.com/Masterminds/squirrel"
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
	var allLedgers []xdr.LedgerCloseMetaView
	err := reader.StreamLedgerRange(ctx, start-1, end+1, func(txmeta xdr.LedgerCloseMetaView) error {
		// the view is only valid for the duration of the callback
		allLedgers = append(allLedgers, slices.Clone(txmeta))
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

		ledgerBinary, err = allLedgers[0].Raw()
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

// fullBlobLedgerRange reproduces the pre-optimization oldest-ledger lookup
// (SELECT meta, i.e. the entire blob) as the benchmark baseline.
func fullBlobLedgerRange(ctx context.Context, db readDB,
	latestSeq uint32, latestTime int64,
) (store.LedgerRange, error) {
	query := sq.Select("meta").
		From(ledgerCloseMetaTableName).
		Where(
			fmt.Sprintf("sequence = (SELECT MIN(sequence) FROM %s)", ledgerCloseMetaTableName),
		)
	var lcmRaw []xdr.LedgerCloseMetaView
	if err := db.Select(ctx, &lcmRaw, query); err != nil {
		return store.LedgerRange{}, err
	}
	if len(lcmRaw) == 0 {
		return store.LedgerRange{}, store.ErrEmptyDB
	}
	firstSeq, err := lcmRaw[0].LedgerSequence()
	if err != nil {
		return store.LedgerRange{}, err
	}
	firstCloseTime, err := lcmRaw[0].LedgerCloseTime()
	if err != nil {
		return store.LedgerRange{}, err
	}
	return store.LedgerRange{
		FirstLedger: store.LedgerInfo{Sequence: firstSeq, CloseTime: firstCloseTime},
		LastLedger:  store.LedgerInfo{Sequence: latestSeq, CloseTime: latestTime},
	}, nil
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

// benchLookup times fn as a sub-benchmark and returns its result so the parent
// can summarize ratios across variants.
func benchLookup(b *testing.B, name string, fn func() error) testing.BenchmarkResult {
	var res testing.BenchmarkResult
	b.Run(name, func(b *testing.B) {
		b.ReportAllocs()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		for b.Loop() {
			require.NoError(b, fn())
		}
		runtime.ReadMemStats(&after)
		res = testing.BenchmarkResult{
			N:         b.N,
			T:         b.Elapsed(),
			MemAllocs: after.Mallocs - before.Mallocs,
			MemBytes:  after.TotalAlloc - before.TotalAlloc,
		}
	})
	return res
}

// BenchmarkOldestLedgerRangeLookup pits the 1KiB prefix fetch in
// getLedgerRangeWithCache against the full-blob query it replaced. The tx read
// path (getLedgers/getTransactions) runs this lookup once per request
func BenchmarkOldestLedgerRangeLookup(b *testing.B) {
	var summary strings.Builder
	for _, tc := range []struct {
		name string
		size int
	}{
		// min/avg/max meta blob sizes observed on a pubnet 7-day node
		{"512KiB", 512 << 10},
		{"2MiB", 2 << 20},
		{"4MiB", 4 << 20},
	} {
		testDB := NewTestDB(b)
		writer := NewReadWriter(logger, testDB, host.MakeNoOpDaemon(), 1_000_000, passphrase)
		write, err := writer.NewTx(b.Context())
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

		ctx := b.Context()
		latestSeq, latestTime := latest.LedgerSequence(), latest.LedgerCloseTime()
		want, err := getLedgerRangeWithCache(ctx, testDB, latestSeq, latestTime)
		require.NoError(b, err)
		got, err := fullBlobLedgerRange(ctx, testDB, latestSeq, latestTime)
		require.NoError(b, err)
		require.Equal(b, want, got)
		require.Equal(b, lcms[0].LedgerSequence(), got.FirstLedger.Sequence)

		prefix := benchLookup(b, tc.name+"/prefix", func() error {
			_, err := getLedgerRangeWithCache(ctx, testDB, latestSeq, latestTime)
			return err
		})
		full := benchLookup(b, tc.name+"/fullBlob", func() error {
			_, err := fullBlobLedgerRange(ctx, testDB, latestSeq, latestTime)
			return err
		})
		summary.WriteString(fmt.Sprintf("\n  %-8s %.2fx ns/op | %.3fx B/op | %.2fx allocs/op", tc.name+":",
			float64(prefix.NsPerOp())/float64(full.NsPerOp()),
			float64(prefix.AllocedBytesPerOp())/float64(full.AllocedBytesPerOp()),
			float64(prefix.AllocsPerOp())/float64(full.AllocsPerOp())))
	}
	fmt.Println("\nprefix / fullBlob:" + summary.String()) //nolint:forbidigo // b.Log would only show under -v
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
