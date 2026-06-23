package hotchunk

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/eventstore"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
)

const testPassphrase = "Public Global Stellar Network ; September 2015"

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

func allTypes() Ingest { return Ingest{Ledgers: true, Events: true} }

func TestOpen_ValidatesInputs(t *testing.T) {
	_, err := Open("", chunk.ID(0), silentLogger())
	require.ErrorIs(t, err, stores.ErrInvalidConfig)

	_, err = Open(t.TempDir(), chunk.ID(0), nil)
	require.ErrorIs(t, err, stores.ErrInvalidConfig)
}

func TestColumnFamilies_IsLedgerAndEventsCFs(t *testing.T) {
	cfs := columnFamilies()
	// 1 ledger CF + 3 events CFs.
	require.Len(t, cfs, 1+len(eventstore.CFNames()))
	require.Equal(t, ledger.LedgersCF, cfs[0])
	for _, cf := range eventstore.CFNames() {
		require.Contains(t, cfs, cf)
	}
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

// TestIngestLedger_EventsCommittedAcrossEventsCFs is the events decision-(a)
// proof: a ledger carrying one contract event commits the ledger AND the
// event in the SAME batch, so the events facade indexes the event's term and
// the event-id watermark advances alongside the ledger watermark.
func TestIngestLedger_EventsCommittedAcrossEventsCFs(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	db := openTestDB(t, chunkID)

	rawA, termA := lcmWithEvent(t, first)
	rawB, _ := lcmWithEvent(t, first+1)

	counts, err := db.IngestLedger(first, xdr.LedgerCloseMetaView(rawA), allTypes())
	require.NoError(t, err)
	assert.Equal(t, LedgerCounts{Ledgers: 1, Events: 1}, counts)

	counts, err = db.IngestLedger(first+1, xdr.LedgerCloseMetaView(rawB), allTypes())
	require.NoError(t, err)
	assert.Equal(t, LedgerCounts{Ledgers: 1, Events: 1}, counts)

	// events CFs: the shared term resolves to both ledgers' events, and the
	// event-id watermark advanced to 2.
	bm, err := db.Events().Lookup(context.Background(), termA)
	require.NoError(t, err)
	require.NotNil(t, bm)
	assert.Equal(t, uint64(2), bm.GetCardinality(), "both ledgers share the event term")
	assert.Equal(t, uint32(2), db.Events().NextEventID())

	// The single watermark equals the last committed ledger seq.
	maxSeq, ok, err := db.MaxCommittedSeq()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, first+1, maxSeq)
}

// TestIngestLedger_DisabledEventsUntouched confirms an Ingest selection without
// Events leaves the events CFs empty even when the ledger carries an event.
func TestIngestLedger_DisabledEventsUntouched(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	db := openTestDB(t, chunkID)

	raw, term := lcmWithEvent(t, first)
	counts, err := db.IngestLedger(first, xdr.LedgerCloseMetaView(raw), Ingest{Ledgers: true})
	require.NoError(t, err)
	assert.Equal(t, LedgerCounts{Ledgers: 1}, counts)

	_, lerr := db.Events().Lookup(context.Background(), term)
	require.ErrorIs(t, lerr, eventstore.ErrTermNotFound)
	assert.Equal(t, uint32(0), db.Events().NextEventID())
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

// lcmWithEvent builds a V2 LCM at seq carrying one transaction that emits a
// single contract event (topic="hotchunk_test"). Returns the wire bytes and
// the event's term key.
func lcmWithEvent(t *testing.T, seq uint32) ([]byte, events.TermKey) {
	t.Helper()
	ev := buildContractEvent("hotchunk_test")
	meta := xdr.TransactionMeta{
		V:  4,
		V4: &xdr.TransactionMetaV4{Operations: []xdr.OperationMetaV2{{Events: []xdr.ContractEvent{ev}}}},
	}
	lcm := buildLCMWithTx(t, seq, meta)
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)

	evBytes, err := ev.MarshalBinary()
	require.NoError(t, err)
	keys, err := events.TermsForBytes(evBytes)
	require.NoError(t, err)
	require.NotEmpty(t, keys)
	return raw, keys[0]
}

func buildContractEvent(topic string) xdr.ContractEvent {
	var contractID xdr.ContractId
	contractID[0] = 0xab
	contractID[1] = 0xcd
	sym := xdr.ScSymbol(topic)
	return xdr.ContractEvent{
		ContractId: &contractID,
		Type:       xdr.ContractEventTypeContract,
		Body: xdr.ContractEventBody{
			V: 0,
			V0: &xdr.ContractEventV0{
				Topics: []xdr.ScVal{{Type: xdr.ScValTypeScvSymbol, Sym: &sym}},
				Data:   xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym},
			},
		},
	}
}

func successResult() xdr.TransactionResult {
	opResults := []xdr.OperationResult{}
	return xdr.TransactionResult{
		FeeCharged: 100,
		Result: xdr.TransactionResultResult{
			Code:    xdr.TransactionResultCodeTxSuccess,
			Results: &opResults,
		},
	}
}

func buildLCMWithTx(t *testing.T, seq uint32, meta xdr.TransactionMeta) xdr.LedgerCloseMeta {
	t.Helper()
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
	hash, err := network.HashTransactionInEnvelope(envelope, testPassphrase)
	require.NoError(t, err)

	comp := []xdr.TxSetComponent{{
		Type: xdr.TxSetComponentTypeTxsetCompTxsMaybeDiscountedFee,
		TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{
			Txs: []xdr.TransactionEnvelope{envelope},
		},
	}}
	return xdr.LedgerCloseMeta{
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
				V1TxSet: &xdr.TransactionSetV1{Phases: []xdr.TransactionPhase{{V: 0, V0Components: &comp}}},
			},
			TxProcessing: []xdr.TransactionResultMetaV1{{
				TxApplyProcessing: meta,
				Result: xdr.TransactionResultPair{
					TransactionHash: hash,
					Result:          successResult(),
				},
			}},
		},
	}
}
