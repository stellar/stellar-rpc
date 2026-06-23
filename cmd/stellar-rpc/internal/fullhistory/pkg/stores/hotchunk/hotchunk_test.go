package hotchunk

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
)

func silentLogger() *supportlog.Entry {
	log := supportlog.New()
	log.SetLevel(logrus.ErrorLevel)
	return log
}

func openTestDB(t *testing.T, chunkID chunk.ID) *DB {
	t.Helper()
	db, err := Open(t.TempDir(), chunkID, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func allTypes() Ingest { return Ingest{Ledgers: true} }

func TestOpen_ValidatesInputs(t *testing.T) {
	_, err := Open("", chunk.ID(0), silentLogger())
	require.ErrorIs(t, err, stores.ErrInvalidConfig)

	_, err = Open(t.TempDir(), chunk.ID(0), nil)
	require.ErrorIs(t, err, stores.ErrInvalidConfig)
}

func TestColumnFamilies_IsLedgerCF(t *testing.T) {
	cfs := columnFamilies()
	require.Len(t, cfs, 1)
	require.Equal(t, ledger.LedgersCF, cfs[0])
}

// TestIngestLedger_LedgerCommittedAndWatermarkAdvances is the core decision-(a)
// happy path: one IngestLedger call writes the ledger into the hot DB, and the
// single watermark reaches exactly the committed seq.
func TestIngestLedger_LedgerCommittedAndWatermarkAdvances(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	db := openTestDB(t, chunkID)

	// Empty DB: no watermark.
	_, ok, err := db.MaxCommittedSeq()
	require.NoError(t, err)
	require.False(t, ok)

	rawA := zeroTxLCM(t, first)
	rawB := zeroTxLCM(t, first+1)

	counts, err := db.IngestLedger(first, xdr.LedgerCloseMetaView(rawA), allTypes())
	require.NoError(t, err)
	assert.Equal(t, LedgerCounts{Ledgers: 1}, counts)

	counts, err = db.IngestLedger(first+1, xdr.LedgerCloseMetaView(rawB), allTypes())
	require.NoError(t, err)
	assert.Equal(t, LedgerCounts{Ledgers: 1}, counts)

	// ledgers CF.
	gotA, err := db.Ledgers().GetLedgerRaw(first)
	require.NoError(t, err)
	assert.Equal(t, rawA, gotA)

	// The single authoritative watermark equals the last committed seq.
	maxSeq, ok, err := db.MaxCommittedSeq()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, first+1, maxSeq)
}

// TestIngestLedger_DurableAcrossReopen confirms a committed ledger survives a
// close/reopen (sync=true durability), and that a commit into a CLOSED store
// fails and leaves nothing behind — the single synced WriteBatch is
// all-or-nothing.
func TestIngestLedger_DurableAcrossReopen(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	dir := t.TempDir()

	db, err := Open(dir, chunkID, silentLogger())
	require.NoError(t, err)

	// Commit one good ledger so there is a known watermark, then close the DB.
	rawGood := zeroTxLCM(t, first)
	_, err = db.IngestLedger(first, xdr.LedgerCloseMetaView(rawGood), allTypes())
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// Reopen and confirm the watermark survived (sync=true durability).
	db2, err := Open(dir, chunkID, silentLogger())
	require.NoError(t, err)

	maxSeq, ok, err := db2.MaxCommittedSeq()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, first, maxSeq, "the committed ledger is durable across reopen")

	// Now close the DB and attempt to ingest the NEXT ledger into the closed
	// store: the commit fails, and nothing for that ledger persists anywhere.
	require.NoError(t, db2.Close())
	rawNext := zeroTxLCM(t, first+1)
	_, err = db2.IngestLedger(first+1, xdr.LedgerCloseMetaView(rawNext), allTypes())
	require.Error(t, err)

	// Reopen a third time: the failed ledger left NO trace, and the watermark is
	// still the last good seq.
	db3, err := Open(dir, chunkID, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db3.Close() })

	maxSeq, ok, err = db3.MaxCommittedSeq()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, first, maxSeq, "the failed ledger did not advance the watermark")

	// The good ledger's data is intact; the failed ledger's is wholly absent.
	_, gerr := db3.Ledgers().GetLedgerRaw(first + 1)
	require.ErrorIs(t, gerr, stores.ErrNotFound)

	gotGood, err := db3.Ledgers().GetLedgerRaw(first)
	require.NoError(t, err)
	assert.Equal(t, rawGood, gotGood)
}

// TestSharedBatch_DirectRocksAbort is the lower-level atomicity proof: queue a
// Put into the ledger CF of the store, then return an error from the batch
// callback — RocksDB applies NONE of it. Pins the property the IngestLedger
// path relies on (atomicity of one WriteBatch).
func TestSharedBatch_DirectRocksAbort(t *testing.T) {
	db := openTestDB(t, chunk.ID(0))

	sentinelErr := assert.AnError

	err := storeOf(db).Batch(func(b *rocksdb.BatchWriter) error {
		b.Put(ledger.LedgersCF, rocksdb.EncodeUint32(2), []byte("ledger-row"))
		return sentinelErr // abort: nothing should commit
	})
	require.ErrorIs(t, err, sentinelErr)

	// The CF did not receive the aborted write.
	_, gerr := db.Ledgers().GetLedgerRaw(2)
	require.ErrorIs(t, gerr, stores.ErrNotFound)
	_, ok, derr := db.MaxCommittedSeq()
	require.NoError(t, derr)
	require.False(t, ok)
}

// storeOf exposes the store for the direct-batch atomicity test (same package,
// so no production accessor is needed).
func storeOf(db *DB) *rocksdb.Store { return db.store }

// TestIngestLedger_ClosedDBFails confirms a closed DB rejects ingest.
func TestIngestLedger_ClosedDBFails(t *testing.T) {
	chunkID := chunk.ID(0)
	db, err := Open(t.TempDir(), chunkID, silentLogger())
	require.NoError(t, err)
	require.NoError(t, db.Close())

	raw := zeroTxLCM(t, chunkID.FirstLedger())
	_, err = db.IngestLedger(chunkID.FirstLedger(), xdr.LedgerCloseMetaView(raw), allTypes())
	require.ErrorIs(t, err, stores.ErrStoreClosed)
}

// ──────────────────────────── LCM fixtures ────────────────────────────

// zeroTxLCM builds a minimal V2 LCM with no transactions at the given sequence.
func zeroTxLCM(t *testing.T, seq uint32) []byte {
	t.Helper()
	lcm := xdr.LedgerCloseMeta{
		V: 2,
		V2: &xdr.LedgerCloseMetaV2{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					ScpValue:  xdr.StellarValue{CloseTime: xdr.TimePoint(0)},
					LedgerSeq: xdr.Uint32(seq),
				},
			},
			TxSet: xdr.GeneralizedTransactionSet{
				V:       1,
				V1TxSet: &xdr.TransactionSetV1{Phases: []xdr.TransactionPhase{}},
			},
			TxProcessing: []xdr.TransactionResultMetaV1{},
		},
	}
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	return raw
}
