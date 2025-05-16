package db

import (
	"context"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go/network"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/daemon/interfaces"
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
	var allLedgers []xdr.LedgerCloseMeta
	err := reader.StreamAllLedgers(context.Background(), func(txmeta xdr.LedgerCloseMeta) error {
		allLedgers = append(allLedgers, txmeta)
		return nil
	})
	require.NoError(t, err)
	for i := start - 1; i <= end+1; i++ {
		ledger, exists, err := reader.GetLedger(context.Background(), i)
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
	daemon := interfaces.MakeNoOpDeamon()

	reader := NewLedgerReader(db)
	_, exists, err := reader.GetLedger(context.Background(), 1)
	require.NoError(t, err)
	assert.False(t, exists)

	for i := 1; i <= 10; i++ {
		ledgerSequence := uint32(i)
		tx, err := NewReadWriter(logger, db, daemon, 150, 15, passphrase).NewTx(context.Background())
		require.NoError(t, err)

		ledgerCloseMeta := createLedger(ledgerSequence)
		require.NoError(t, tx.LedgerWriter().InsertLedger(ledgerCloseMeta))
		require.NoError(t, tx.Commit(ledgerCloseMeta))
		// rolling back after a commit is a no-op
		require.NoError(t, tx.Rollback())
	}

	assertLedgerRange(t, reader, 1, 10)

	ledgerSequence := uint32(11)
	tx, err := NewReadWriter(logger, db, daemon, 150, 15, passphrase).NewTx(context.Background())
	require.NoError(t, err)
	ledgerCloseMeta := createLedger(ledgerSequence)
	require.NoError(t, tx.LedgerWriter().InsertLedger(ledgerCloseMeta))
	require.NoError(t, tx.Commit(ledgerCloseMeta))

	assertLedgerRange(t, reader, 1, 11)

	ledgerSequence = uint32(12)
	tx, err = NewReadWriter(logger, db, daemon, 150, 5, passphrase).NewTx(context.Background())
	require.NoError(t, err)
	ledgerCloseMeta = createLedger(ledgerSequence)
	require.NoError(t, tx.LedgerWriter().InsertLedger(ledgerCloseMeta))
	require.NoError(t, tx.Commit(ledgerCloseMeta))

	assertLedgerRange(t, reader, 8, 12)
}

func TestGetLedgerRange_NonEmptyDB(t *testing.T) {
	db := NewTestDB(t)
	ctx := context.TODO()

	writer := NewReadWriter(logger, db, interfaces.MakeNoOpDeamon(), 10, 10, passphrase)
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
	require.NoError(t, write.Commit(lcms[len(lcms)-1]))

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
	ctx := context.TODO()

	writer := NewReadWriter(logger, db, interfaces.MakeNoOpDeamon(), 10, 10, passphrase)
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
	require.NoError(t, write.Commit(lcms[len(lcms)-1]))

	reader := NewLedgerReader(db)
	ledgerRange, err := reader.GetLedgerRange(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint32(1334), ledgerRange.FirstLedger.Sequence)
	assert.Equal(t, ledgerCloseTime(1334), ledgerRange.FirstLedger.CloseTime)
	assert.Equal(t, uint32(1334), ledgerRange.LastLedger.Sequence)
	assert.Equal(t, ledgerCloseTime(1334), ledgerRange.LastLedger.CloseTime)
}

func TestGetLedgerRange_EmptyDB(t *testing.T) {
	db := NewTestDB(t)
	ctx := context.TODO()

	reader := NewLedgerReader(db)
	ledgerRange, err := reader.GetLedgerRange(ctx)
	assert.Equal(t, ErrEmptyDB, err)
	assert.Equal(t, uint32(0), ledgerRange.FirstLedger.Sequence)
	assert.Equal(t, int64(0), ledgerRange.FirstLedger.CloseTime)
	assert.Equal(t, uint32(0), ledgerRange.LastLedger.Sequence)
	assert.Equal(t, int64(0), ledgerRange.LastLedger.CloseTime)
}

func BenchmarkGetLedgerRange(b *testing.B) {
	testDB, lcms := setupBenchmarkingDB(b)
	reader := NewLedgerReader(testDB)

	b.ResetTimer()
	for range b.N {
		ledgerRange, err := reader.GetLedgerRange(context.TODO())
		require.NoError(b, err)
		assert.Equal(b, lcms[0].LedgerSequence(), ledgerRange.FirstLedger.Sequence)
		assert.Equal(b, lcms[len(lcms)-1].LedgerSequence(), ledgerRange.LastLedger.Sequence)
	}
}

func BenchmarkBatchGetLedgers(b *testing.B) {
	testDB, lcms := setupBenchmarkingDB(b)
	reader := NewLedgerReader(testDB)
	readTx, err := reader.NewTx(context.Background())
	require.NoError(b, err)
	batchSize := uint(200) // using the current maximum value for getLedgers endpoint

	start := uint32(1334)
	end := start + uint32(batchSize) - 1
	b.ResetTimer()
	for range b.N {
		ledgers, err := readTx.BatchGetLedgers(context.TODO(), start, end)
		require.NoError(b, err)
		assert.Equal(b, lcms[0].LedgerSequence(), ledgers[0].LedgerSequence())
		assert.Equal(b, lcms[batchSize-1].LedgerSequence(), ledgers[batchSize-1].LedgerSequence())
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
	writer := NewReadWriter(logger, testDB, interfaces.MakeNoOpDeamon(),
		100, 1_000_000, passphrase)
	write, err := writer.NewTx(context.TODO())
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
	require.NoError(b, write.Commit(lcms[len(lcms)-1]))
	return testDB, lcms
}
