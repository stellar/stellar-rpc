package hotchunk

import (
	"context"
	"testing"

	"github.com/linxGnu/grocksdb"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/eventstore"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/ledger"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/txhash"
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

// assertWriteItems checks the per-type write volume the report carries on the
// write phases (the item counts that used to be LedgerCounts). Every fixture
// commits exactly one ledger with one event, so only the txhash count (one per
// applied tx) varies across callers.
func assertWriteItems(t *testing.T, rep LedgerReport, txhash int) {
	t.Helper()
	assert.Equal(t, 1, rep.Phases[PhaseLedgers].Items, "ledgers items")
	assert.Equal(t, txhash, rep.Phases[PhaseTxhash].Items, "txhash items")
	assert.Equal(t, 1, rep.Phases[PhaseEvents].Items, "events items")
}

// TestConfig_PerCFOptionRouting asserts the txhash calibration reaches ONLY the
// txhash CF: the ledger CF gets no bloom (it is never probed for missing keys)
// and the events CFs keep their own compression/block-size overrides. Asserted
// at the config-construction seam because grocksdb options can't be read back
// off a live DB.
func TestConfig_PerCFOptionRouting(t *testing.T) {
	perCF := config(t.TempDir(), silentLogger(), false, false).PerCFOptions

	txCF := txhash.CFNames()[0]
	assert.Equal(t, 12, perCF[txCF].BloomFilterBitsPerKey, "txhash CF keeps its bloom")
	assert.Equal(t, 64, perCF[txCF].WriteBufferMB, "txhash CF keeps its write buffer")
	assert.True(t, perCF[txCF].DisableAutoCompactions, "txhash CF keeps compaction off")

	ledgerCF := ledger.CFNames()[0]
	assert.Zero(t, perCF[ledgerCF].BloomFilterBitsPerKey, "ledger CF gets no bloom")
	assert.Zero(t, perCF[ledgerCF].WriteBufferMB, "ledger CF rides on RocksDB defaults")
	assert.False(t, perCF[ledgerCF].DisableAutoCompactions, "ledger CF keeps auto-compaction")

	assert.Equal(t, grocksdb.ZSTDCompression, perCF[eventstore.DataCF].Compression,
		"events data CF keeps its ZSTD override")
	assert.NotZero(t, perCF[eventstore.DataCF].BlockSize, "events data CF keeps its block-size override")
	assert.Zero(t, perCF[eventstore.DataCF].BloomFilterBitsPerKey, "events data CF gets no bloom")
}

func TestConfig_DBWideTuningStaysShared(t *testing.T) {
	tuning := config(t.TempDir(), silentLogger(), false, false).Tuning
	assert.Equal(t, 512, tuning.BlockCacheMB)
	assert.Equal(t, 1024, tuning.MaxTotalWalSizeMB)
	assert.Equal(t, 8, tuning.MaxBackgroundJobs)
	assert.Equal(t, 10_000, tuning.MaxOpenFiles)
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
// ONE shared DB, and the single committed frontier reaches exactly the committed seq —
// every CF readable, every CF in lockstep.
func TestIngestLedger_AllCFsAdvanceTogether(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	db := openTestDB(t)

	// Empty DB: no last committed seq.
	_, ok, err := db.MaxCommittedSeq()
	require.NoError(t, err)
	require.False(t, ok)

	rawA, hashA, termA := lcmWithEvent(t, first)
	rawB, hashB, _ := lcmWithEvent(t, first+1)

	repA, err := db.IngestLedger(first, xdr.LedgerCloseMetaView(rawA))
	require.NoError(t, err)
	assertWriteItems(t, repA, 1)

	repB, err := db.IngestLedger(first+1, xdr.LedgerCloseMetaView(rawB))
	require.NoError(t, err)
	assertWriteItems(t, repB, 1)

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
	bms, err := db.Events().LookupKeys(context.Background(), []events.TermKey{termA})
	require.NoError(t, err)
	require.NotNil(t, bms[0])
	assert.Equal(t, uint64(2), bms[0].GetCardinality(), "both ledgers share the event term")
	assert.Equal(t, uint32(2), eventCount(t, db.Events()))

	// The single authoritative committed frontier equals the last committed seq.
	maxSeq, ok, err := db.MaxCommittedSeq()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, first+1, maxSeq)
}

// TestIngestLedger_RejectedLedgerPersistsNothingAcrossAnyCF is the atomicity
// guarantee for decision (a): a ledger the events facade rejects (here an
// out-of-range seq) must leave EVERY CF untouched — the ledgers and txhash CFs
// included — because the whole ledger is one batch and the events facade's
// validation aborts that batch before commit. The single committed frontier must not
// advance.
func TestIngestLedger_RejectedLedgerPersistsNothingAcrossAnyCF(t *testing.T) {
	chunkID := chunk.ID(0)
	db := openTestDB(t)

	// A ledger seq ABOVE the chunk's range: the events facade rejects it
	// (ErrLedgerOutOfRange) from inside the batch callback, aborting the write.
	badSeq := chunkID.LastLedger() + 1
	raw, hash, term := lcmWithEvent(t, badSeq)

	_, err := db.IngestLedger(badSeq, xdr.LedgerCloseMetaView(raw))
	require.Error(t, err)
	require.ErrorIs(t, err, eventstore.ErrLedgerOutOfRange)

	// NOTHING persisted, across every CF:
	// ledgers CF — no row at badSeq.
	_, gerr := db.Ledgers().GetLedgerRaw(badSeq)
	require.ErrorIs(t, gerr, stores.ErrNotFound)
	// txhash CFs — the hash is absent.
	_, gerr = db.Txhash().Get(hash)
	require.ErrorIs(t, gerr, stores.ErrNotFound)
	// events CFs — no term indexed, no event committed (clean miss = nil bitmap).
	bms, lerr := db.Events().LookupKeys(context.Background(), []events.TermKey{term})
	require.NoError(t, lerr)
	require.Nil(t, bms[0])
	assert.Equal(t, uint32(0), eventCount(t, db.Events()))

	// The single committed frontier is still empty — nothing committed.
	_, ok, err := db.MaxCommittedSeq()
	require.NoError(t, err)
	require.False(t, ok, "a rejected ledger must not advance the last committed seq")
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

	// Commit one good ledger so there is a known last committed seq, then close the DB.
	rawGood, hashGood, _ := lcmWithEvent(t, first)
	_, err = db.IngestLedger(first, xdr.LedgerCloseMetaView(rawGood))
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// Reopen and confirm the last committed seq survived (sync=true durability).
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
	_, err = db2.IngestLedger(first+1, xdr.LedgerCloseMetaView(rawNext))
	require.Error(t, err)

	// Reopen a third time: the failed ledger left NO trace in any CF, and the
	// last committed seq is still the last good seq.
	db3, err := Open(dir, chunkID, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db3.Close() })

	maxSeq, ok, err = db3.MaxCommittedSeq()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, first, maxSeq, "the failed ledger did not advance the last committed seq")

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

// TestSource_SelfBoundsUnboundedRange confirms the freeze source (hotLedgerStream)
// yields the store's committed ledgers in order and self-bounds an UNBOUNDED range
// at the committed frontier (mirroring packStream), so drain can pass
// UnboundedRange(from) rather than a pre-computed bound.
func TestSource_SelfBoundsUnboundedRange(t *testing.T) {
	db := openTestDB(t)
	first := chunk.ID(0).FirstLedger()
	for i := range uint32(3) {
		_, err := db.IngestLedger(first+i, xdr.LedgerCloseMetaView(zeroTxLCM(t, first+i)))
		require.NoError(t, err)
	}

	var got []uint32
	for raw, err := range db.Source().RawLedgers(context.Background(), ledgerbackend.UnboundedRange(first)) {
		require.NoError(t, err)
		seq, serr := xdr.LedgerCloseMetaView(raw).LedgerSequence()
		require.NoError(t, serr)
		got = append(got, seq)
	}
	require.Equal(t, []uint32{first, first + 1, first + 2}, got, "self-bounds at the frontier, in order")
}

// TestSource_RejectsGap pins the source-side in-order guard that replaced the
// shared cursor: a gap in the hot store's keyspace (the sole writer of recent
// history) is a real defect and must surface as an error, not a silent skip.
func TestSource_RejectsGap(t *testing.T) {
	db := openTestDB(t)
	first := chunk.ID(0).FirstLedger()
	// Seed the ledgers CF directly with a GAP (first, first+2), bypassing
	// IngestLedger's contiguity so the source-level guard is what's exercised.
	require.NoError(t, storeOf(db).Batch(func(b *rocksdb.BatchWriter) error {
		for _, s := range []uint32{first, first + 2} {
			if err := db.Ledgers().AddLedgerToBatch(b, ledger.Entry{Seq: s, Bytes: []byte("x")}); err != nil {
				return err
			}
		}
		return nil
	}))

	var lastErr error
	for _, err := range db.Source().RawLedgers(context.Background(), ledgerbackend.BoundedRange(first, first+2)) {
		if err != nil {
			lastErr = err
			break
		}
	}
	require.Error(t, lastErr)
	require.Contains(t, lastErr.Error(), "gap")
}

// TestIngestLedger_WritesEveryHotType confirms the hot tier always writes all
// three hot data types; per-type disabling is not a supported hot DB mode.
func TestIngestLedger_WritesEveryHotType(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	db := openTestDB(t)

	raw, hash, term := lcmWithEvent(t, first)
	rep, err := db.IngestLedger(first, xdr.LedgerCloseMetaView(raw))
	require.NoError(t, err)
	assertWriteItems(t, rep, 1)

	got, err := db.Ledgers().GetLedgerRaw(first)
	require.NoError(t, err)
	assert.Equal(t, raw, got)

	seq, err := db.Txhash().Get(hash)
	require.NoError(t, err)
	assert.Equal(t, first, seq)
	bms, err := db.Events().LookupKeys(context.Background(), []events.TermKey{term})
	require.NoError(t, err)
	require.NotNil(t, bms[0])
	assert.Equal(t, uint64(1), bms[0].GetCardinality())
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

	rep, err := db.IngestLedger(first, xdr.LedgerCloseMetaView(raw))
	require.NoError(t, err)
	assertWriteItems(t, rep, 2) // both hashes indexed (event-less included); one event

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
	_, err = db.IngestLedger(first, xdr.LedgerCloseMetaView(raw))
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
		_, ierr := db.IngestLedger(seq, xdr.LedgerCloseMetaView(zeroTxLCM(t, seq)))
		require.NoError(t, ierr)
	}
	require.NoError(t, db.Close())

	// Reader: a read-only open sees the committed frontier; Close must not flush.
	ro, err := OpenReadOnly(dir, chunkID, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ro.Close()) })

	seq, ok, err := ro.MaxCommittedSeq()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, first+1, seq, "read-only handle sees the committed data")

	// A write through the read-only handle must fail — the freeze never mutates.
	_, err = ro.IngestLedger(first+2, xdr.LedgerCloseMetaView(zeroTxLCM(t, first+2)))
	require.Error(t, err, "read-only DB must reject writes")
}

// TestOpenReadOnly_SkipsEventsWarmup pins #834: a read-only (freeze/probe) open is
// a ledgers-only view that never runs eventstore's index-CF warmup scan, while a
// read-WRITE open still warms. The proof is a poisoned events-index row that
// warmup's key-length check rejects: the write open fails on it (warmup ran), the
// read-only open ignores it (warmup skipped) and still serves the ledgers-only
// surface — MaxCommittedSeq and Source() byte-for-byte — that freeze/probe use.
func TestOpenReadOnly_SkipsEventsWarmup(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	dir := t.TempDir()

	// Writer: ingest two ledgers, capture the exact wire bytes, then close.
	db, err := Open(dir, chunkID, silentLogger())
	require.NoError(t, err)
	want := map[uint32][]byte{}
	for _, seq := range []uint32{first, first + 1} {
		raw := zeroTxLCM(t, seq)
		want[seq] = raw
		_, ierr := db.IngestLedger(seq, xdr.LedgerCloseMetaView(raw))
		require.NoError(t, ierr)
	}
	require.NoError(t, db.Close())

	// Poison the events index with a malformed row (wrong key length). warmup's
	// index scan rejects it; a scan-skipping open never sees it. Written through a
	// bare read-write open of the same multi-CF DB, closed to free the LOCK.
	raw, err := rocksdb.New(config(dir, silentLogger(), false, true))
	require.NoError(t, err)
	require.NoError(t, raw.Put(eventstore.IndexCF, []byte("bad"), nil))
	require.NoError(t, raw.Close())

	// Read-WRITE open warms → the poisoned index row fails the scan.
	_, werr := OpenExisting(dir, chunkID, silentLogger())
	require.ErrorContains(t, werr, "events_index key length",
		"a read-write open must run the events warmup scan and reject the poison")

	// Read-only open skips the warmup → opens clean despite the poison.
	ro, err := OpenReadOnly(dir, chunkID, silentLogger())
	require.NoError(t, err, "read-only open must skip the events warmup and ignore the poison")
	t.Cleanup(func() { require.NoError(t, ro.Close()) })

	// Probe surface: MaxCommittedSeq resolves through the skipped open.
	seq, ok, err := ro.MaxCommittedSeq()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, first+1, seq)

	// Freeze surface: Source() yields the committed ledgers byte-for-byte.
	got := map[uint32][]byte{}
	for b, err := range ro.Source().RawLedgers(context.Background(), ledgerbackend.UnboundedRange(first)) {
		require.NoError(t, err)
		s, serr := xdr.LedgerCloseMetaView(b).LedgerSequence()
		require.NoError(t, serr)
		got[s] = append([]byte(nil), b...) // b is borrowed; clone before the next step
	}
	assert.Equal(t, want, got, "freeze reads are byte-identical through the warmup-skipped open")

	// Structural safety: the ledgers-only view has no events facade to hand out.
	assert.Panics(t, func() { ro.Events() }, "Events() on a ledgers-only view must fail loudly")
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
	_, err = db.IngestLedger(chunkID.FirstLedger(), xdr.LedgerCloseMetaView(raw))
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
