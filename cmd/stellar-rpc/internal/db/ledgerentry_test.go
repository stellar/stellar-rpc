package db

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/daemon/interfaces"
)

func getLedgerEntryAndLatestLedgerSequenceWithErr(db *DB, key xdr.LedgerKey) (bool, xdr.LedgerEntry,
	uint32, *uint32, error,
) {
	tx, err := NewLedgerEntryReader(db).NewTx(context.Background(), false)
	if err != nil {
		return false, xdr.LedgerEntry{}, 0, nil, err
	}
	var doneErr error
	defer func() {
		doneErr = tx.Done()
	}()

	latestSeq, err := tx.GetLatestLedgerSequence()
	if err != nil {
		return false, xdr.LedgerEntry{}, 0, nil, err
	}

	present, entry, expSeq, err := GetLedgerEntry(tx, key)
	if err != nil {
		return false, xdr.LedgerEntry{}, 0, nil, err
	}

	return present, entry, latestSeq, expSeq, doneErr
}

func getLedgerEntryAndLatestLedgerSequence(t require.TestingT, db *DB, key xdr.LedgerKey) (bool, xdr.LedgerEntry,
	uint32, *uint32,
) {
	present, entry, latestSeq, expSeq, err := getLedgerEntryAndLatestLedgerSequenceWithErr(db, key)
	require.NoError(t, err)
	return present, entry, latestSeq, expSeq
}

//nolint:unparam
func makeReadWriter(db *DB, batchSize, retentionWindow int) ReadWriter {
	return NewReadWriter(log.DefaultLogger, db, interfaces.MakeNoOpDeamon(),
		batchSize, uint32(retentionWindow), passphrase, false)
}

func TestGoldenPath(t *testing.T) {
	db := NewTestDB(t)

	t.Run("EmptyDB", func(t *testing.T) {
		testEmptyDB(t, db)
	})

	t.Run("InsertEntry", func(t *testing.T) {
		testInsertEntry(t, db)
	})

	t.Run("UpdateEntry", func(t *testing.T) {
		testUpdateEntry(t, db)
	})

	t.Run("DeleteEntry", func(t *testing.T) {
		testDeleteEntry(t, db)
	})
}

func testEmptyDB(t *testing.T, db *DB) {
	_, err := NewLedgerEntryReader(db).GetLatestLedgerSequence(context.Background())
	assert.Equal(t, ErrEmptyDB, err)
}

func testInsertEntry(t *testing.T, db *DB) {
	tx, err := makeReadWriter(db, 150, 15).NewTx(context.Background())
	require.NoError(t, err)
	writer := tx.LedgerEntryWriter()

	data := createTestContractDataEntry()
	key, entry := getContractDataLedgerEntry(t, data)
	require.NoError(t, writer.UpsertLedgerEntry(entry))

	expLedgerKey, err := entryKeyToTTLEntryKey(key)
	require.NoError(t, err)
	expLegerEntry := getTTLLedgerEntry(expLedgerKey)
	require.NoError(t, writer.UpsertLedgerEntry(expLegerEntry))

	ledgerSequence := uint32(23)
	ledgerCloseMeta := createLedger(ledgerSequence)
	require.NoError(t, tx.LedgerWriter().InsertLedger(ledgerCloseMeta))
	require.NoError(t, tx.Commit(ledgerCloseMeta))

	verifyInsertedEntry(t, db, key, ledgerSequence, expLegerEntry)
}

func testUpdateEntry(t *testing.T, db *DB) {
	tx, err := makeReadWriter(db, 150, 15).NewTx(context.Background())
	require.NoError(t, err)
	writer := tx.LedgerEntryWriter()

	data := createTestContractDataEntry()
	key, entry := getContractDataLedgerEntry(t, data)
	eight := xdr.Uint32(8)
	entry.Data.ContractData.Val.U32 = &eight

	require.NoError(t, writer.UpsertLedgerEntry(entry))

	ledgerSequence := uint32(24)
	ledgerCloseMeta := createLedger(ledgerSequence)
	require.NoError(t, tx.LedgerWriter().InsertLedger(ledgerCloseMeta))
	require.NoError(t, tx.Commit(ledgerCloseMeta))

	verifyUpdatedEntry(t, db, key, ledgerSequence, eight)
}

func testDeleteEntry(t *testing.T, db *DB) {
	tx, err := makeReadWriter(db, 150, 15).NewTx(context.Background())
	require.NoError(t, err)
	writer := tx.LedgerEntryWriter()

	data := createTestContractDataEntry()
	key, _ := getContractDataLedgerEntry(t, data)
	require.NoError(t, writer.DeleteLedgerEntry(key))

	ledgerSequence := uint32(25)
	ledgerCloseMeta := createLedger(ledgerSequence)
	require.NoError(t, tx.LedgerWriter().InsertLedger(ledgerCloseMeta))
	require.NoError(t, tx.Commit(ledgerCloseMeta))

	verifyDeletedEntry(t, db, key, ledgerSequence)
}

func createTestContractDataEntry() xdr.ContractDataEntry {
	four := xdr.Uint32(4)
	six := xdr.Uint32(6)
	return xdr.ContractDataEntry{
		Contract: xdr.ScAddress{
			Type:       xdr.ScAddressTypeScAddressTypeContract,
			ContractId: &xdr.Hash{0xca, 0xfe},
		},
		Key: xdr.ScVal{
			Type: xdr.ScValTypeScvU32,
			U32:  &four,
		},
		Durability: xdr.ContractDataDurabilityPersistent,
		Val: xdr.ScVal{
			Type: xdr.ScValTypeScvU32,
			U32:  &six,
		},
	}
}

func verifyInsertedEntry(t *testing.T, db *DB, key xdr.LedgerKey, ledgerSequence uint32,
	expLegerEntry xdr.LedgerEntry,
) {
	present, obtainedEntry, obtainedLedgerSequence, liveUntilSeq := getLedgerEntryAndLatestLedgerSequence(t, db, key)
	assert.True(t, present)
	assert.Equal(t, ledgerSequence, obtainedLedgerSequence)
	require.NotNil(t, liveUntilSeq)
	assert.Equal(t, uint32(expLegerEntry.Data.Ttl.LiveUntilLedgerSeq), *liveUntilSeq)
	assert.Equal(t, xdr.LedgerEntryTypeContractData, obtainedEntry.Data.Type)
	assert.Equal(t, xdr.Hash{0xca, 0xfe}, *obtainedEntry.Data.ContractData.Contract.ContractId)
	assert.Equal(t, xdr.Uint32(6), *obtainedEntry.Data.ContractData.Val.U32)

	obtainedLedgerSequence, err := NewLedgerEntryReader(db).GetLatestLedgerSequence(context.Background())
	require.NoError(t, err)
	assert.Equal(t, ledgerSequence, obtainedLedgerSequence)
}

func verifyUpdatedEntry(t *testing.T, db *DB, key xdr.LedgerKey, ledgerSequence uint32, expectedValue xdr.Uint32) {
	present, obtainedEntry, obtainedLedgerSequence, liveUntilSeq := getLedgerEntryAndLatestLedgerSequence(t, db, key)
	assert.True(t, present)
	require.NotNil(t, liveUntilSeq)
	assert.Equal(t, ledgerSequence, obtainedLedgerSequence)
	assert.Equal(t, expectedValue, *obtainedEntry.Data.ContractData.Val.U32)
}

func verifyDeletedEntry(t *testing.T, db *DB, key xdr.LedgerKey, ledgerSequence uint32) {
	present, _, obtainedLedgerSequence, liveUntilSeq := getLedgerEntryAndLatestLedgerSequence(t, db, key)
	assert.False(t, present)
	assert.Nil(t, liveUntilSeq)
	assert.Equal(t, ledgerSequence, obtainedLedgerSequence)

	obtainedLedgerSequence, err := NewLedgerEntryReader(db).GetLatestLedgerSequence(context.Background())
	require.NoError(t, err)
	assert.Equal(t, ledgerSequence, obtainedLedgerSequence)
}

func TestDeleteNonExistentLedgerEmpty(t *testing.T) {
	db := NewTestDB(t)

	// Simulate a ledger which creates and deletes a ledger entry
	// which would result in trying to delete a ledger entry which isn't there
	tx, err := makeReadWriter(db, 150, 15).NewTx(context.Background())
	require.NoError(t, err)
	writer := tx.LedgerEntryWriter()

	four := xdr.Uint32(4)
	six := xdr.Uint32(6)
	data := xdr.ContractDataEntry{
		Contract: xdr.ScAddress{
			Type:       xdr.ScAddressTypeScAddressTypeContract,
			ContractId: &xdr.Hash{0xca, 0xfe},
		},
		Key: xdr.ScVal{
			Type: xdr.ScValTypeScvU32,
			U32:  &four,
		},
		Durability: xdr.ContractDataDurabilityPersistent,
		Val: xdr.ScVal{
			Type: xdr.ScValTypeScvU32,
			U32:  &six,
		},
	}
	key, _ := getContractDataLedgerEntry(t, data)
	require.NoError(t, writer.DeleteLedgerEntry(key))
	ledgerSequence := uint32(23)
	ledgerCloseMeta := createLedger(ledgerSequence)
	assert.NoError(t, tx.LedgerWriter().InsertLedger(ledgerCloseMeta))
	assert.NoError(t, tx.Commit(ledgerCloseMeta))

	// Make sure that the ledger number was submitted
	obtainedLedgerSequence, err := NewLedgerEntryReader(db).GetLatestLedgerSequence(context.Background())
	require.NoError(t, err)
	assert.Equal(t, ledgerSequence, obtainedLedgerSequence)

	// And that the entry doesn't exist
	present, _, obtainedLedgerSequence, expSeq := getLedgerEntryAndLatestLedgerSequence(t, db, key)
	assert.False(t, present)
	require.Nil(t, expSeq)
	assert.Equal(t, ledgerSequence, obtainedLedgerSequence)
}

func getContractDataLedgerEntry(t require.TestingT, data xdr.ContractDataEntry) (xdr.LedgerKey, xdr.LedgerEntry) {
	entry := xdr.LedgerEntry{
		LastModifiedLedgerSeq: 1,
		Data: xdr.LedgerEntryData{
			Type:         xdr.LedgerEntryTypeContractData,
			ContractData: &data,
		},
		Ext: xdr.LedgerEntryExt{},
	}
	var key xdr.LedgerKey
	err := key.SetContractData(data.Contract, data.Key, data.Durability)
	require.NoError(t, err)
	return key, entry
}

func getTTLLedgerEntry(key xdr.LedgerKey) xdr.LedgerEntry {
	var expLegerEntry xdr.LedgerEntry
	expLegerEntry.Data.Ttl = &xdr.TtlEntry{
		KeyHash:            key.Ttl.KeyHash,
		LiveUntilLedgerSeq: 100,
	}
	expLegerEntry.Data.Type = key.Type
	return expLegerEntry
}

// Make sure that (multiple, simultaneous) read transactions can happen while a write-transaction is ongoing,
// and write is only visible once the transaction is committed
func TestReadTxsDuringWriteTx(t *testing.T) {
	db := NewTestDB(t)

	// Check that we get an empty DB error
	_, err := NewLedgerEntryReader(db).GetLatestLedgerSequence(context.Background())
	assert.Equal(t, ErrEmptyDB, err)

	// Start filling the DB with a single entry (enforce flushing right away)
	tx, err := makeReadWriter(db, 0, 15).NewTx(context.Background())
	require.NoError(t, err)
	writer := tx.LedgerEntryWriter()

	four := xdr.Uint32(4)
	six := xdr.Uint32(6)
	data := xdr.ContractDataEntry{
		Contract: xdr.ScAddress{
			Type:       xdr.ScAddressTypeScAddressTypeContract,
			ContractId: &xdr.Hash{0xca, 0xfe},
		},
		Key: xdr.ScVal{
			Type: xdr.ScValTypeScvU32,
			U32:  &four,
		},
		Val: xdr.ScVal{
			Type: xdr.ScValTypeScvU32,
			U32:  &six,
		},
	}
	key, entry := getContractDataLedgerEntry(t, data)
	require.NoError(t, writer.UpsertLedgerEntry(entry))

	expLedgerKey, err := entryKeyToTTLEntryKey(key)
	require.NoError(t, err)
	expLegerEntry := getTTLLedgerEntry(expLedgerKey)
	require.NoError(t, writer.UpsertLedgerEntry(expLegerEntry))

	// Before committing the changes, make sure multiple concurrent transactions can query the DB
	readTx1, err := NewLedgerEntryReader(db).NewTx(context.Background(), false)
	require.NoError(t, err)
	readTx2, err := NewLedgerEntryReader(db).NewTx(context.Background(), false)
	require.NoError(t, err)

	_, err = readTx1.GetLatestLedgerSequence()
	assert.Equal(t, ErrEmptyDB, err)
	present, _, expSeq, err := GetLedgerEntry(readTx1, key)
	require.Nil(t, expSeq)
	require.NoError(t, err)
	assert.False(t, present)
	require.NoError(t, readTx1.Done())

	_, err = readTx2.GetLatestLedgerSequence()
	assert.Equal(t, ErrEmptyDB, err)
	present, _, expSeq, err = GetLedgerEntry(readTx2, key)
	require.NoError(t, err)
	assert.False(t, present)
	assert.Nil(t, expSeq)
	require.NoError(t, readTx2.Done())
	// Finish the write transaction and check that the results are present
	ledgerSequence := uint32(23)
	ledgerCloseMeta := createLedger(ledgerSequence)
	require.NoError(t, tx.LedgerWriter().InsertLedger(ledgerCloseMeta))
	require.NoError(t, tx.Commit(ledgerCloseMeta))

	obtainedLedgerSequence, err := NewLedgerEntryReader(db).GetLatestLedgerSequence(context.Background())
	require.NoError(t, err)
	assert.Equal(t, ledgerSequence, obtainedLedgerSequence)

	present, obtainedEntry, obtainedLedgerSequence, expSeq := getLedgerEntryAndLatestLedgerSequence(t, db, key)
	assert.True(t, present)
	assert.Equal(t, ledgerSequence, obtainedLedgerSequence)
	assert.Equal(t, six, *obtainedEntry.Data.ContractData.Val.U32)
	assert.NotNil(t, expSeq)
}

// Make sure that a write transaction can happen while multiple read transactions are ongoing,
// and write is only visible once the transaction is committed
func TestWriteTxsDuringReadTxs(t *testing.T) {
	db := NewTestDB(t)

	// Check that we get an empty DB error
	_, err := NewLedgerEntryReader(db).GetLatestLedgerSequence(context.Background())
	assert.Equal(t, ErrEmptyDB, err)

	// Create a multiple read transactions, interleaved with the writing process

	// First read transaction, before the write transaction is created
	readTx1, err := NewLedgerEntryReader(db).NewTx(context.Background(), false)
	require.NoError(t, err)

	// Start filling the DB with a single entry (enforce flushing right away)
	tx, err := makeReadWriter(db, 0, 15).NewTx(context.Background())
	require.NoError(t, err)
	writer := tx.LedgerEntryWriter()

	// Second read transaction, after the write transaction is created
	readTx2, err := NewLedgerEntryReader(db).NewTx(context.Background(), false)
	require.NoError(t, err)

	four := xdr.Uint32(4)
	six := xdr.Uint32(6)
	data := xdr.ContractDataEntry{
		Contract: xdr.ScAddress{
			Type:       xdr.ScAddressTypeScAddressTypeContract,
			ContractId: &xdr.Hash{0xca, 0xfe},
		},
		Key: xdr.ScVal{
			Type: xdr.ScValTypeScvU32,
			U32:  &four,
		},
		Durability: xdr.ContractDataDurabilityPersistent,
		Val: xdr.ScVal{
			Type: xdr.ScValTypeScvU32,
			U32:  &six,
		},
	}
	key, entry := getContractDataLedgerEntry(t, data)
	require.NoError(t, writer.UpsertLedgerEntry(entry))

	expLedgerKey, err := entryKeyToTTLEntryKey(key)
	require.NoError(t, err)
	expLegerEntry := getTTLLedgerEntry(expLedgerKey)
	require.NoError(t, writer.UpsertLedgerEntry(expLegerEntry))

	// Third read transaction, after the first insert has happened in the write transaction
	readTx3, err := NewLedgerEntryReader(db).NewTx(context.Background(), false)
	require.NoError(t, err)

	// Make sure that all the read transactions get an emptyDB error before and after the write transaction is committed
	for _, readTx := range []LedgerEntryReadTx{readTx1, readTx2, readTx3} {
		_, err = readTx.GetLatestLedgerSequence()
		assert.Equal(t, ErrEmptyDB, err)
		present, _, _, err := GetLedgerEntry(readTx, key)
		require.NoError(t, err)
		assert.False(t, present)
	}

	// commit the write transaction
	ledgerSequence := uint32(23)
	ledgerCloseMeta := createLedger(ledgerSequence)
	require.NoError(t, tx.LedgerWriter().InsertLedger(ledgerCloseMeta))
	require.NoError(t, tx.Commit(ledgerCloseMeta))

	for _, readTx := range []LedgerEntryReadTx{readTx1, readTx2, readTx3} {
		present, _, _, err := GetLedgerEntry(readTx, key)
		require.NoError(t, err)
		assert.False(t, present)
	}

	// Check that the results are present in the transactions happening after the commit

	obtainedLedgerSequence, err := NewLedgerEntryReader(db).GetLatestLedgerSequence(context.Background())
	require.NoError(t, err)
	assert.Equal(t, ledgerSequence, obtainedLedgerSequence)

	present, obtainedEntry, obtainedLedgerSequence, expSeq := getLedgerEntryAndLatestLedgerSequence(t, db, key)
	assert.True(t, present)
	require.NotNil(t, expSeq)
	assert.Equal(t, ledgerSequence, obtainedLedgerSequence)
	assert.Equal(t, six, *obtainedEntry.Data.ContractData.Val.U32)

	for _, readTx := range []LedgerEntryReadTx{readTx1, readTx2, readTx3} {
		require.NoError(t, readTx.Done())
	}
}

// Check that we can have coexisting reader and writer goroutines without deadlocks or errors
func TestConcurrentReadersAndWriter(t *testing.T) {
	db := NewTestDB(t)
	contractID := xdr.Hash{0xca, 0xfe}
	done := make(chan struct{})
	var wg sync.WaitGroup
	logMessageCh := make(chan string, 1)

	wg.Add(1)
	go writer(t, db, contractID, done, &wg, logMessageCh)

	for i := 1; i <= 32; i++ {
		wg.Add(1)
		go reader(t, db, contractID, i, done, &wg, logMessageCh)
	}

	monitorWorkers(t, &wg, logMessageCh)
}

func writer(t *testing.T, db *DB, contractID xdr.Hash, done chan struct{},
	wg *sync.WaitGroup, logMessageCh chan<- string,
) {
	defer wg.Done()
	defer close(done)

	rw := makeReadWriter(db, 10, 15)
	for ledgerSequence := range 1000 {
		writeLedger(t, rw, contractID, uint32(ledgerSequence))
		logMessageCh <- fmt.Sprintf("Wrote ledger %d", ledgerSequence)
		time.Sleep(time.Duration(rand.Int31n(30)) * time.Millisecond)
	}
}

func writeLedger(t *testing.T, rw ReadWriter, contractID xdr.Hash, ledgerSequence uint32) {
	tx, err := rw.NewTx(context.Background())
	require.NoError(t, err)
	writer := tx.LedgerEntryWriter()

	for i := range 200 {
		writeEntry(t, writer, contractID, i)
	}

	ledgerCloseMeta := createLedger(ledgerSequence)
	require.NoError(t, tx.LedgerWriter().InsertLedger(ledgerCloseMeta))
	require.NoError(t, tx.Commit(ledgerCloseMeta))
}

func writeEntry(t *testing.T, writer LedgerEntryWriter, contractID xdr.Hash, i int) {
	key, entry := getContractDataLedgerEntry(t, createContractDataEntry(contractID, i))
	require.NoError(t, writer.UpsertLedgerEntry(entry))

	expLedgerKey, err := entryKeyToTTLEntryKey(key)
	require.NoError(t, err)
	expLegerEntry := getTTLLedgerEntry(expLedgerKey)
	require.NoError(t, writer.UpsertLedgerEntry(expLegerEntry))
}

func createContractDataEntry(contractID xdr.Hash, i int) xdr.ContractDataEntry {
	val := xdr.Uint32(i)
	return xdr.ContractDataEntry{
		Contract: xdr.ScAddress{
			Type:       xdr.ScAddressTypeScAddressTypeContract,
			ContractId: &contractID,
		},
		Key: xdr.ScVal{
			Type: xdr.ScValTypeScvU32,
			U32:  &val,
		},
		Durability: xdr.ContractDataDurabilityPersistent,
		Val: xdr.ScVal{
			Type: xdr.ScValTypeScvU32,
			U32:  &val,
		},
	}
}

func reader(t *testing.T, db *DB, contractID xdr.Hash, keyVal int, done <-chan struct{},
	wg *sync.WaitGroup, logMessageCh chan<- string,
) {
	defer wg.Done()
	key := createLedgerKey(contractID, keyVal)

	for {
		select {
		case <-done:
			return
		default:
			readAndVerifyEntry(t, db, key, keyVal, logMessageCh)
			time.Sleep(time.Duration(rand.Int31n(30)) * time.Millisecond)
		}
	}
}

func createLedgerKey(contractID xdr.Hash, keyVal int) xdr.LedgerKey {
	val := xdr.Uint32(keyVal)
	return xdr.LedgerKey{
		Type: xdr.LedgerEntryTypeContractData,
		ContractData: &xdr.LedgerKeyContractData{
			Contract: xdr.ScAddress{
				Type:       xdr.ScAddressTypeScAddressTypeContract,
				ContractId: &contractID,
			},
			Key: xdr.ScVal{
				Type: xdr.ScValTypeScvU32,
				U32:  &val,
			},
			Durability: xdr.ContractDataDurabilityPersistent,
		},
	}
}

func readAndVerifyEntry(t *testing.T, db *DB, key xdr.LedgerKey, keyVal int, logMessageCh chan<- string) {
	found, ledgerEntry, ledger, _, err := getLedgerEntryAndLatestLedgerSequenceWithErr(db, key)
	if err != nil {
		if !errors.Is(err, ErrEmptyDB) {
			t.Fatalf("reader %d failed with error %v\n", keyVal, err)
		}
	} else {
		assert.True(t, found)
		logMessageCh <- fmt.Sprintf("reader %d: for ledger %d", keyVal, ledger)
		assert.Equal(t, xdr.Uint32(keyVal), *ledgerEntry.Data.ContractData.Val.U32)
	}
}

func monitorWorkers(t *testing.T, wg *sync.WaitGroup, logMessageCh <-chan string) {
	workersExitCh := make(chan struct{})
	go func() {
		defer close(workersExitCh)
		wg.Wait()
	}()

	for {
		select {
		case <-workersExitCh:
			return
		case msg := <-logMessageCh:
			t.Log(msg)
		}
	}
}

func benchmarkLedgerEntry(b *testing.B, cached bool) {
	db := NewTestDB(b)
	keyUint32 := xdr.Uint32(0)
	data := xdr.ContractDataEntry{
		Contract: xdr.ScAddress{
			Type:       xdr.ScAddressTypeScAddressTypeContract,
			ContractId: &xdr.Hash{0xca, 0xfe},
		},
		Key: xdr.ScVal{
			Type: xdr.ScValTypeScvU32,
			U32:  &keyUint32,
		},
		Durability: xdr.ContractDataDurabilityPersistent,
		Val: xdr.ScVal{
			Type: xdr.ScValTypeScvU32,
			U32:  &keyUint32,
		},
	}
	key, entry := getContractDataLedgerEntry(b, data)
	tx, err := makeReadWriter(db, 150, 15).NewTx(context.Background())
	require.NoError(b, err)
	require.NoError(b, tx.LedgerEntryWriter().UpsertLedgerEntry(entry))
	expLedgerKey, err := entryKeyToTTLEntryKey(key)
	require.NoError(b, err)
	require.NoError(b, tx.LedgerEntryWriter().UpsertLedgerEntry(getTTLLedgerEntry(expLedgerKey)))
	require.NoError(b, tx.Commit(createLedger(2)))
	reader := NewLedgerEntryReader(db)
	const numQueriesPerOp = 15
	b.ResetTimer()
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		var readTx LedgerEntryReadTx
		var err error
		if cached {
			readTx, err = reader.NewTx(context.Background(), true)
		} else {
			readTx, err = reader.NewTx(context.Background(), false)
		}
		require.NoError(b, err)
		for range numQueriesPerOp {
			b.StartTimer()
			found, _, _, err := GetLedgerEntry(readTx, key)
			b.StopTimer()
			require.NoError(b, err)
			assert.True(b, found)
		}
		require.NoError(b, readTx.Done())
	}
}

func BenchmarkGetLedgerEntry(b *testing.B) {
	b.Run("With cache", func(b *testing.B) { benchmarkLedgerEntry(b, true) })
	b.Run("Without cache", func(b *testing.B) { benchmarkLedgerEntry(b, false) })
}

func BenchmarkLedgerUpdate(b *testing.B) {
	db := NewTestDB(b)
	keyUint32 := xdr.Uint32(0)
	data := xdr.ContractDataEntry{
		Contract: xdr.ScAddress{
			Type:       xdr.ScAddressTypeScAddressTypeContract,
			ContractId: &xdr.Hash{0xca, 0xfe},
		},
		Key: xdr.ScVal{
			Type: xdr.ScValTypeScvU32,
			U32:  &keyUint32,
		},
		Durability: xdr.ContractDataDurabilityPersistent,
		Val: xdr.ScVal{
			Type: xdr.ScValTypeScvU32,
			U32:  &keyUint32,
		},
	}
	_, entry := getContractDataLedgerEntry(b, data)
	const numEntriesPerOp = 3500
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, err := makeReadWriter(db, 150, 15).NewTx(context.Background())
		require.NoError(b, err)
		writer := tx.LedgerEntryWriter()
		for j := range numEntriesPerOp {
			keyUint32 = xdr.Uint32(j)
			require.NoError(b, writer.UpsertLedgerEntry(entry))
		}
		require.NoError(b, tx.Commit(createLedger(uint32(i+1))))
	}
}
