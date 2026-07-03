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
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash"
)

const testPassphrase = "Public Global Stellar Network ; September 2015"

func silentLogger() *supportlog.Entry {
	log := supportlog.New()
	log.SetLevel(logrus.ErrorLevel)
	return log
}

// openTestDB opens a fresh hot DB bound to chunk 0 (every test uses chunk 0).
func openTestDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(t.TempDir(), chunk.ID(0), silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestOpen_ValidatesInputs(t *testing.T) {
	_, err := Open("", chunk.ID(0), silentLogger())
	require.ErrorIs(t, err, stores.ErrInvalidConfig)

	_, err = Open(t.TempDir(), chunk.ID(0), nil)
	require.ErrorIs(t, err, stores.ErrInvalidConfig)
}

func TestColumnFamilies_UnionIsNonColliding(t *testing.T) {
	cfs := ColumnFamilies()
	// 1 ledger CF + 3 events CFs + 1 txhash CF = 5.
	require.Len(t, cfs, len(ledger.CFNames())+len(eventstore.CFNames())+len(txhash.CFNames()))
	seen := map[string]bool{}
	for _, cf := range cfs {
		require.False(t, seen[cf], "CF name %q collides across facades", cf)
		seen[cf] = true
	}
	require.Contains(t, seen, ledger.LedgersCF)
	for _, cf := range eventstore.CFNames() {
		require.Contains(t, seen, cf)
	}
	for _, cf := range txhash.CFNames() {
		require.Contains(t, seen, cf)
	}
}

// TestIngestLedger_AllCFsAdvanceTogether is the core decision-(a) happy path:
// one IngestLedger call writes the ledger, its tx hash, and its event into the
// ONE shared DB, and the single watermark reaches exactly the committed seq —
// every CF readable, every CF in lockstep.
func TestIngestLedger_AllCFsAdvanceTogether(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	db := openTestDB(t)

	// Empty DB: no watermark.
	_, ok, err := db.MaxCommittedSeq()
	require.NoError(t, err)
	require.False(t, ok)

	rawA, hashA, termA := lcmWithEvent(t, first)
	rawB, hashB, _ := lcmWithEvent(t, first+1)

	counts, _, err := db.IngestLedger(first, xdr.LedgerCloseMetaView(rawA))
	require.NoError(t, err)
	assert.Equal(t, LedgerCounts{Ledgers: 1, Txhash: 1, Events: 1}, counts)

	counts, _, err = db.IngestLedger(first+1, xdr.LedgerCloseMetaView(rawB))
	require.NoError(t, err)
	assert.Equal(t, LedgerCounts{Ledgers: 1, Txhash: 1, Events: 1}, counts)

	// ledgers CF.
	gotA, err := db.Ledgers().GetLedgerRaw(first)
	require.NoError(t, err)
	assert.Equal(t, rawA, gotA)
	// txhash CFs.
	seqA, err := db.Txhash().Get(hashA)
	require.NoError(t, err)
	assert.Equal(t, first, seqA)
	seqB, err := db.Txhash().Get(hashB)
	require.NoError(t, err)
	assert.Equal(t, first+1, seqB)
	// events CFs.
	bm, err := db.Events().Lookup(context.Background(), termA)
	require.NoError(t, err)
	require.NotNil(t, bm)
	assert.Equal(t, uint64(2), bm.GetCardinality(), "both ledgers share the event term")
	assert.Equal(t, uint32(2), eventCount(t, db.Events()))

	// The single authoritative watermark equals the last committed seq.
	maxSeq, ok, err := db.MaxCommittedSeq()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, first+1, maxSeq)
}

// TestIngestLedger_RejectedLedgerPersistsNothingAcrossAnyCF is the atomicity
// guarantee for decision (a): a ledger the events facade rejects (here an
// out-of-range seq) must leave EVERY CF untouched — the ledgers and txhash CFs
// included — because the whole ledger is one batch and the events facade's
// validation aborts that batch before commit. The single watermark must not
// advance.
func TestIngestLedger_RejectedLedgerPersistsNothingAcrossAnyCF(t *testing.T) {
	chunkID := chunk.ID(0)
	db := openTestDB(t)

	// A ledger seq ABOVE the chunk's range: the events facade rejects it
	// (ErrLedgerOutOfRange) from inside the batch callback, aborting the write.
	badSeq := chunkID.LastLedger() + 1
	raw, hash, term := lcmWithEvent(t, badSeq)

	_, _, err := db.IngestLedger(badSeq, xdr.LedgerCloseMetaView(raw))
	require.Error(t, err)
	require.ErrorIs(t, err, eventstore.ErrLedgerOutOfRange)

	// NOTHING persisted, across every CF:
	// ledgers CF — no row at badSeq.
	_, gerr := db.Ledgers().GetLedgerRaw(badSeq)
	require.ErrorIs(t, gerr, stores.ErrNotFound)
	// txhash CFs — the hash is absent.
	_, gerr = db.Txhash().Get(hash)
	require.ErrorIs(t, gerr, stores.ErrNotFound)
	// events CFs — no term indexed, no event committed.
	_, lerr := db.Events().Lookup(context.Background(), term)
	require.ErrorIs(t, lerr, eventstore.ErrTermNotFound)
	assert.Equal(t, uint32(0), eventCount(t, db.Events()))

	// The single watermark is still empty — nothing committed.
	_, ok, err := db.MaxCommittedSeq()
	require.NoError(t, err)
	require.False(t, ok, "a rejected ledger must not advance the watermark")
}

// TestIngestLedger_MidBatchCommitFailurePersistsNothing simulates a mid-batch
// COMMIT failure (the store closed under the writer) and asserts the partial
// batch persisted nothing across any CF after reopen — the single synced
// WriteBatch is all-or-nothing.
func TestIngestLedger_MidBatchCommitFailurePersistsNothing(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	dir := t.TempDir()

	db, err := Open(dir, chunkID, silentLogger())
	require.NoError(t, err)

	// Commit one good ledger so there is a known watermark, then close the DB.
	rawGood, hashGood, _ := lcmWithEvent(t, first)
	_, _, err = db.IngestLedger(first, xdr.LedgerCloseMetaView(rawGood))
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// Reopen and confirm the watermark survived (sync=true durability).
	db2, err := Open(dir, chunkID, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db2.Close() })

	maxSeq, ok, err := db2.MaxCommittedSeq()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, first, maxSeq, "the committed ledger is durable across reopen")

	// Now close the DB and attempt to ingest the NEXT ledger into the closed
	// store: the commit fails, and nothing for that ledger persists anywhere.
	require.NoError(t, db2.Close())
	rawNext, hashNext, _ := lcmWithEvent(t, first+1)
	_, _, err = db2.IngestLedger(first+1, xdr.LedgerCloseMetaView(rawNext))
	require.Error(t, err)

	// Reopen a third time: the failed ledger left NO trace in any CF, and the
	// watermark is still the last good seq.
	db3, err := Open(dir, chunkID, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db3.Close() })

	maxSeq, ok, err = db3.MaxCommittedSeq()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, first, maxSeq, "the failed ledger did not advance the watermark")

	// The events CF advanced for exactly the one good ledger — the failed
	// ledger's event was not committed (warmup reconstructed the offsets from
	// disk, which hold only the good ledger).
	assert.Equal(t, uint32(1), eventCount(t, db3.Events()),
		"the failed ledger's event must not be committed to the events CFs")

	// The good ledger's data is intact; the failed ledger's is wholly absent
	// across the ledgers and txhash CFs.
	_, gerr := db3.Ledgers().GetLedgerRaw(first + 1)
	require.ErrorIs(t, gerr, stores.ErrNotFound)
	_, gerr = db3.Txhash().Get(hashNext)
	require.ErrorIs(t, gerr, stores.ErrNotFound)

	gotGood, err := db3.Ledgers().GetLedgerRaw(first)
	require.NoError(t, err)
	assert.Equal(t, rawGood, gotGood)
	_, err = db3.Txhash().Get(hashGood)
	require.NoError(t, err)
}

// TestSharedBatch_DirectRocksAbortAcrossCFs is the lower-level atomicity proof:
// queue Puts into DIFFERENT CFs of the shared store, then return an error from
// the batch callback — RocksDB applies NONE of them. Pins the property the
// IngestLedger path relies on (intra-store cross-CF atomicity of one
// WriteBatch).
func TestSharedBatch_DirectRocksAbortAcrossCFs(t *testing.T) {
	db := openTestDB(t)

	var hash [32]byte
	hash[0] = 0xa0
	sentinelErr := assert.AnError

	err := storeOf(db).Batch(func(b *rocksdb.BatchWriter) error {
		b.Put(ledger.LedgersCF, rocksdb.EncodeUint32(2), []byte("ledger-row"))
		b.Put(txhash.CFNames()[0], hash[:], rocksdb.EncodeUint32(2))
		b.Put(eventstore.DataCF, []byte{0, 0, 0, 0}, []byte("event-row"))
		return sentinelErr // abort: nothing should commit
	})
	require.ErrorIs(t, err, sentinelErr)

	// None of the three CFs received the aborted writes.
	_, gerr := db.Ledgers().GetLedgerRaw(2)
	require.ErrorIs(t, gerr, stores.ErrNotFound)
	_, gerr = db.Txhash().Get(hash)
	require.ErrorIs(t, gerr, stores.ErrNotFound)
	_, ok, derr := db.MaxCommittedSeq()
	require.NoError(t, derr)
	require.False(t, ok)
}

// storeOf exposes the shared store for the direct-batch atomicity test (same
// package, so no production accessor is needed).
func storeOf(db *DB) *rocksdb.Store { return db.store }

// TestIngestLedger_WritesEveryHotType confirms the hot tier always writes all
// three hot data types; per-type disabling is not a supported hot DB mode.
func TestIngestLedger_WritesEveryHotType(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	db := openTestDB(t)

	raw, hash, term := lcmWithEvent(t, first)
	counts, _, err := db.IngestLedger(first, xdr.LedgerCloseMetaView(raw))
	require.NoError(t, err)
	assert.Equal(t, LedgerCounts{Ledgers: 1, Txhash: 1, Events: 1}, counts)

	got, err := db.Ledgers().GetLedgerRaw(first)
	require.NoError(t, err)
	assert.Equal(t, raw, got)

	seq, err := db.Txhash().Get(hash)
	require.NoError(t, err)
	assert.Equal(t, first, seq)
	bm, err := db.Events().Lookup(context.Background(), term)
	require.NoError(t, err)
	require.NotNil(t, bm)
	assert.Equal(t, uint64(1), bm.GetCardinality())
}

// TestIngestLedger_EventlessTxStillIndexesHash pins the post-merge txhash
// completeness invariant: after #18 folded the txhash and events walks into one
// ExtractLedgerEvents pass, txhash coverage rests entirely on that walk yielding
// an element per APPLIED tx — hash included — even for an event-less transaction
// (the common classic-only case). Every other hotchunk test uses one-tx-one-event
// ledgers, so nothing else pins it: an SDK change that dropped event-less txs from
// the walk would silently gut the txhash index for every classic-only transaction.
func TestIngestLedger_EventlessTxStillIndexesHash(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	db := openTestDB(t)

	// Two applied txs in one ledger: one carries a contract event, one carries none.
	eventful := xdr.TransactionMeta{V: 4, V4: &xdr.TransactionMetaV4{
		Operations: []xdr.OperationMetaV2{{Events: []xdr.ContractEvent{buildContractEvent("eventful")}}},
	}}
	eventless := xdr.TransactionMeta{V: 4, V4: &xdr.TransactionMetaV4{
		Operations: []xdr.OperationMetaV2{{}}, // one op, no events
	}}
	lcm, hashes := buildLCM(t, first, []xdr.TransactionMeta{eventful, eventless})
	require.Len(t, hashes, 2)
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)

	counts, _, err := db.IngestLedger(first, xdr.LedgerCloseMetaView(raw))
	require.NoError(t, err)
	assert.Equal(t, LedgerCounts{Ledgers: 1, Txhash: 2, Events: 1}, counts,
		"both applied txs' hashes indexed (event-less included); only the eventful tx contributed an event")

	// Both hashes resolve in the txhash CF to this ledger.
	for _, h := range hashes {
		seq, gerr := db.Txhash().Get(h)
		require.NoError(t, gerr, "event-less tx hash must still be indexed")
		assert.Equal(t, first, seq)
	}
	// The events CF holds exactly the one eventful tx's event.
	assert.Equal(t, uint32(1), eventCount(t, db.Events()))
}

// TestReopen_RecoversEventsMirror confirms the events facade's warmup runs over
// the shared store on reopen (the mirror/offsets are reconstructed from the
// events CFs), so a reopened DB assigns event IDs continuing from disk.
func TestReopen_RecoversEventsMirror(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	dir := t.TempDir()

	db, err := Open(dir, chunkID, silentLogger())
	require.NoError(t, err)
	raw, _, _ := lcmWithEvent(t, first)
	_, _, err = db.IngestLedger(first, xdr.LedgerCloseMetaView(raw))
	require.NoError(t, err)
	require.NoError(t, db.Close())

	db2, err := Open(dir, chunkID, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db2.Close() })
	assert.Equal(t, uint32(1), eventCount(t, db2.Events()), "warmup recovered the events offsets")
}

// TestOpenReadOnly_ReadsCommittedAndRejectsWrites pins the freeze source's
// read-only handle: it sees data a writer committed and cleanly closed (so the
// completeness gate is exact), and any write through it fails — a freeze can
// never mutate the hot DB it reads.
func TestOpenReadOnly_ReadsCommittedAndRejectsWrites(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	dir := t.TempDir()

	// Writer: ingest two ledgers, then close (flushes the WAL into SST).
	db, err := Open(dir, chunkID, silentLogger())
	require.NoError(t, err)
	for _, seq := range []uint32{first, first + 1} {
		_, _, ierr := db.IngestLedger(seq, xdr.LedgerCloseMetaView(zeroTxLCM(t, seq)))
		require.NoError(t, ierr)
	}
	require.NoError(t, db.Close())

	// Reader: a read-only open sees the committed watermark; Close must not flush.
	ro, err := OpenReadOnly(dir, chunkID, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ro.Close()) })

	seq, ok, err := ro.MaxCommittedSeq()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, first+1, seq, "read-only handle sees the committed data")

	// A write through the read-only handle must fail — the freeze never mutates.
	_, _, err = ro.IngestLedger(first+2, xdr.LedgerCloseMetaView(zeroTxLCM(t, first+2)))
	require.Error(t, err, "read-only DB must reject writes")
}

// TestIngestLedger_ClosedDBFails confirms a closed shared DB rejects ingest. The
// closed-store guard is Store.Batch's authoritative lifecycle RLock + checkOpen
// (the per-facade pre-checks were dropped in #30), so the surfaced sentinel is
// rocksdb.ErrStoreClosed.
func TestIngestLedger_ClosedDBFails(t *testing.T) {
	chunkID := chunk.ID(0)
	db, err := Open(t.TempDir(), chunkID, silentLogger())
	require.NoError(t, err)
	require.NoError(t, db.Close())

	raw := zeroTxLCM(t, chunkID.FirstLedger())
	_, _, err = db.IngestLedger(chunkID.FirstLedger(), xdr.LedgerCloseMetaView(raw))
	require.ErrorIs(t, err, rocksdb.ErrStoreClosed)
}

// ──────────────────────────── LCM fixtures ────────────────────────────

// lcmWithEvent builds a V2 LCM with one transaction carrying one contract event
// (topic="hotchunk_test"). Returns the wire bytes, the tx hash, and the event's
// term key.
func lcmWithEvent(t *testing.T, seq uint32) ([]byte, [32]byte, events.TermKey) {
	t.Helper()
	ev := buildContractEvent("hotchunk_test")
	meta := xdr.TransactionMeta{
		V:  4,
		V4: &xdr.TransactionMetaV4{Operations: []xdr.OperationMetaV2{{Events: []xdr.ContractEvent{ev}}}},
	}
	lcm, hash := buildLCMWithTx(t, seq, meta)
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)

	evBytes, err := ev.MarshalBinary()
	require.NoError(t, err)
	keys, err := events.TermsForBytes(evBytes)
	require.NoError(t, err)
	require.NotEmpty(t, keys)
	return raw, hash, keys[0]
}

func zeroTxLCM(t *testing.T, seq uint32) []byte {
	t.Helper()
	lcm, _ := buildLCM(t, seq, nil)
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	return raw
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

func buildLCMWithTx(t *testing.T, seq uint32, meta xdr.TransactionMeta) (xdr.LedgerCloseMeta, [32]byte) {
	t.Helper()
	lcm, hashes := buildLCM(t, seq, []xdr.TransactionMeta{meta})
	require.Len(t, hashes, 1)
	return lcm, hashes[0]
}

func buildLCM(t *testing.T, seq uint32, txMetas []xdr.TransactionMeta) (xdr.LedgerCloseMeta, [][32]byte) {
	t.Helper()
	phases := make([]xdr.TransactionPhase, 0, len(txMetas))
	txProcessing := make([]xdr.TransactionResultMetaV1, 0, len(txMetas))
	hashes := make([][32]byte, 0, len(txMetas))

	for _, meta := range txMetas {
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
		hashes = append(hashes, hash)

		txProcessing = append(txProcessing, xdr.TransactionResultMetaV1{
			TxApplyProcessing: meta,
			Result: xdr.TransactionResultPair{
				TransactionHash: hash,
				Result:          successResult(),
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
					ScpValue:  xdr.StellarValue{CloseTime: xdr.TimePoint(0)},
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
	return lcm, hashes
}

// eventCount reads the hot events store's committed event count, failing the
// test on the (close-only) error the Reader contract allows.
func eventCount(t *testing.T, r interface{ EventCount() (uint32, error) }) uint32 {
	t.Helper()
	n, err := r.EventCount()
	require.NoError(t, err)
	return n
}
