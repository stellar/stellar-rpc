package lifecycle

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/backfill"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/fhtest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
)

// lifecyclePassphrase is the network passphrase the one-tx fixture hashes
// against (any stable value works; the index only needs deterministic hashes).
const lifecyclePassphrase = network.PublicNetworkPassphrase

// oneTxLCMRand builds the wire bytes of a V2 LedgerCloseMeta carrying ONE
// transaction for seq, so a chunk ingested with at least one such ledger yields
// a NON-empty txhash .bin, which exercises the real merge-and-fold index path
// (a fully zero-tx chunk now builds a valid but trivial empty index instead).
// Mirrors ingest_test's buildLCMReturningHashes, trimmed to one tx.
func oneTxLCMRand(t *testing.T, seq uint32) []byte {
	t.Helper()
	envelope := xdr.TransactionEnvelope{
		Type: xdr.EnvelopeTypeEnvelopeTypeTx,
		V1: &xdr.TransactionV1Envelope{
			Tx: xdr.Transaction{
				SourceAccount: xdr.MustMuxedAddress(keypair.MustRandom().Address()),
				Ext:           xdr.TransactionExt{V: 1, SorobanData: &xdr.SorobanTransactionData{}},
			},
		},
	}
	hash, err := network.HashTransactionInEnvelope(envelope, lifecyclePassphrase)
	require.NoError(t, err)

	comp := []xdr.TxSetComponent{{
		Type: xdr.TxSetComponentTypeTxsetCompTxsMaybeDiscountedFee,
		TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{
			Txs: []xdr.TransactionEnvelope{envelope},
		},
	}}
	opResults := []xdr.OperationResult{}
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
				V1TxSet: &xdr.TransactionSetV1{Phases: []xdr.TransactionPhase{{V: 0, V0Components: &comp}}},
			},
			TxProcessing: []xdr.TransactionResultMetaV1{{
				TxApplyProcessing: xdr.TransactionMeta{
					V:  4,
					V4: &xdr.TransactionMetaV4{Operations: []xdr.OperationMetaV2{}},
				},
				Result: xdr.TransactionResultPair{
					TransactionHash: hash,
					Result: xdr.TransactionResult{
						FeeCharged: 100,
						Result:     xdr.TransactionResultResult{Code: xdr.TransactionResultCodeTxSuccess, Results: &opResults},
					},
				},
			}},
		},
	}
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	return raw
}

// ingestFullHotChunk creates a "ready" hot DB for chunk c and ingests every
// ledger in the chunk (all CFs, contiguous from FirstLedger), then closes the
// write handle — the post-boundary state the lifecycle freezes from. The hot
// key is left "ready" and the dir is on disk, as the boundary handoff leaves it.
func ingestFullHotChunk(t *testing.T, cat *catalog.Catalog, c chunk.ID) {
	t.Helper()
	db := openLiveHotDB(t, cat, c)
	for seq := c.FirstLedger(); seq <= c.LastLedger(); seq++ {
		// The first ledger carries one tx so the chunk's txhash .bin is non-empty
		// (streamhash refuses a zero-key index); the rest stay zero-tx for speed.
		var raw []byte
		if seq == c.FirstLedger() {
			raw = oneTxLCMRand(t, seq)
		} else {
			raw = fhtest.ZeroTxLCMBytes(t, seq)
		}
		_, err := db.IngestLedger(seq, xdr.LedgerCloseMetaView(raw))
		require.NoError(t, err)
	}
	require.NoError(t, db.Close()) // release the write handle (boundary handoff)
}

// lifecycleTestConfig wires a Config over the real production primitives. The
// freeze reads the hot tier by opening the chunk's real on-disk DB (created by
// ingestFullHotChunk) straight from its Layout path — the same open production
// does after #22. A tick failure now surfaces as runLifecycle's returned error
// (no Fatalf), so tests assert on that error rather than a recorder.
func lifecycleTestConfig(t *testing.T, cat *catalog.Catalog, retentionChunks uint32) Config {
	t.Helper()
	return Config{
		ExecConfig: backfill.ExecConfig{
			Catalog: cat,
			Logger:  silentLogger(),
			Workers: 2,
			Process: backfill.ProcessConfig{},
		},
		Retention: fhtest.RetentionFor(t, cat, retentionChunks),
	}
}

// lastCompleteChunkAtID maps geometry.LastCompleteChunkAt to a chunk.ID (ok=false
// on a negative result). Was a production helper until #25 (the tick now plans
// [floor, lastChunk] without it); it lives here for the tick-mirroring helpers.
func lastCompleteChunkAtID(ledger uint32) (chunk.ID, bool) {
	c := geometry.LastCompleteChunkAt(ledger)
	if c < 0 {
		return 0, false
	}
	return chunk.ID(c), true
}

// makeReadyHotDirNoData opens and closes a real (empty) hot DB for c so its dir
// exists on disk and its key is "ready" — the state a discard scan inspects
// without needing a full ingest.
func makeReadyHotDirNoData(t *testing.T, cat *catalog.Catalog, c chunk.ID) {
	t.Helper()
	db, err := openHotDBForChunk(cat, c, silentLogger())
	require.NoError(t, err)
	require.NoError(t, db.Close())
}

// floorFor is the retention floor the tick derives for the given through ledger:
// FloorAt over the last complete chunk (-1 when none).
func floorFor(t *testing.T, cfg Config, through uint32) chunk.ID {
	t.Helper()
	return cfg.Retention.FloorAt(geometry.LastCompleteChunkAt(through))
}

// assertQuiescent re-runs the tick's three derivations against the SAME through
// snapshot and asserts none schedule work — the quiescence postcondition.
func assertQuiescent(t *testing.T, cfg Config, cat *catalog.Catalog, through uint32) {
	t.Helper()
	floor := floorFor(t, cfg, through)
	// through is derived from the catalog's own last-committed ledger, so it always
	// resolves to the last complete chunk — the same lastChunk runLifecycle keys its
	// scans off (0 with ok=false only when nothing is complete, which the discard
	// scan's per-chunk predicates then correctly treat as no work).
	lastChunk, ok := lastCompleteChunkAtID(through)
	if ok && floor <= lastChunk {
		// At quiescence resolve finds an empty plan, so RunBackfill (resolve +
		// executePlan) is a no-op that returns nil — even with no Backend wired,
		// since an empty plan never reaches backfillSource.
		perr := backfill.RunBackfill(context.Background(), cfg.ExecConfig, floor, lastChunk)
		require.NoError(t, perr, "re-running backfill schedules no work at quiescence")
	}
	dchunks, err := eligibleDiscardChunks(cat, floor, lastChunk)
	require.NoError(t, err)
	assert.Empty(t, dchunks, "re-scan finds no discard work at quiescence")
	pops, _, err := eligiblePruneOps(cat, floor)
	require.NoError(t, err)
	assert.Empty(t, pops, "re-scan finds no prune work at quiescence")
}
