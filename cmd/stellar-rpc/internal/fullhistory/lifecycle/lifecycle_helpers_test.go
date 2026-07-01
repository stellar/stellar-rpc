package lifecycle

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/backfill"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// lifecyclePassphrase is the network passphrase the one-tx fixture hashes
// against (any stable value works; the index only needs deterministic hashes).
const lifecyclePassphrase = network.PublicNetworkPassphrase

// oneTxLCMRand builds the wire bytes of a V2 LedgerCloseMeta carrying ONE
// transaction for seq, so a chunk ingested with at least one such ledger yields
// a NON-empty txhash .bin — streamhash refuses to build a cold index over zero
// keys (txhash.ErrEmptyBuildSet), so a fully zero-tx chunk cannot exercise the
// real index fold. Mirrors ingest_test's buildLCMReturningHashes, trimmed to one
// tx.
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
			raw = zeroTxLCMBytes(t, seq)
		}
		_, err := db.IngestLedger(seq, xdr.LedgerCloseMetaView(raw))
		require.NoError(t, err)
	}
	require.NoError(t, db.Close()) // release the write handle (boundary handoff)
}

// lifecycleTestConfig wires a Config over the real production primitives
// (a real RocksHotProbe over the catalog's hot layout) plus a fatal recorder so a
// tick abort is observable instead of killing the test process.
func lifecycleTestConfig(t *testing.T, cat *catalog.Catalog, retentionChunks uint32) (Config, *fatalRecorder) {
	t.Helper()
	rec := &fatalRecorder{}
	cfg := Config{
		ExecConfig: backfill.ExecConfig{
			Catalog: cat,
			Logger:  silentLogger(),
			Workers: 2,
			Process: backfill.ProcessConfig{
				HotProbe: NewRocksHotProbe(cat.Layout().HotChunkPath, silentLogger()),
			},
		},
		RetentionChunks: retentionChunks,
		Fatalf:          rec.fatalf,
	}
	return cfg, rec
}

// fatalRecorder captures Fatalf calls so a test can assert a tick did (or did
// NOT) abort the daemon.
type fatalRecorder struct {
	count atomic.Int32
	last  atomic.Value // string
}

func (r *fatalRecorder) fatalf(format string, args ...any) {
	r.count.Add(1)
	r.last.Store(fmt.Sprintf(format, args...))
}

func (r *fatalRecorder) fired() bool { return r.count.Load() > 0 }

// runTickForCatalog runs one lifecycle tick the way ingestion would drive it:
// it derives the highest complete chunk from the catalog (the chunk id ingestion
// hands over at a boundary) and passes it as lastChunk. A negative result (young
// network, no complete chunk) is passed as chunk 0 — the resolve range guard
// then makes the plan empty, matching the design's young-network no-op.
func runTickForCatalog(ctx context.Context, t *testing.T, cfg Config, cat *catalog.Catalog) {
	t.Helper()
	through, err := deriveCompleteThrough(cat)
	require.NoError(t, err)
	last, ok := lastCompleteChunkAtID(through)
	if !ok {
		last = 0
	}
	runLifecycle(ctx, cfg, cat, last)
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

// assertQuiescent re-runs the tick's three derivations against the SAME through
// snapshot and asserts none schedule work — the quiescence postcondition.
func assertQuiescent(t *testing.T, cfg Config, cat *catalog.Catalog, through uint32) {
	t.Helper()
	earliest, _, err := cat.EarliestLedger()
	require.NoError(t, err)
	floor := EffectiveRetentionFloor(through, cfg.RetentionChunks, earliest)
	start := ChunkIDOfLedger(floor)
	low, hasLow, err := lowestMaterializedChunk(cat)
	require.NoError(t, err)
	if hasLow && int64(low) > start {
		start = int64(low)
	}
	if rangeEnd, ok := lastCompleteChunkAtID(through); ok && start >= 0 {
		// At quiescence resolve finds an empty plan, so RunBackfill (resolve +
		// executePlan) is a no-op that returns nil — even with no Backend wired,
		// since an empty plan never reaches backfillSource.
		perr := backfill.RunBackfill(context.Background(), cfg.ExecConfig, chunk.ID(start), rangeEnd)
		assert.NoError(t, perr, "re-running backfill schedules no work at quiescence")
	}
	dops, err := eligibleDiscardOps(cfg, cat, through)
	require.NoError(t, err)
	assert.Empty(t, dops, "re-scan finds no discard work at quiescence")
	pops, err := eligiblePruneOps(cfg, cat, through)
	require.NoError(t, err)
	assert.Empty(t, pops, "re-scan finds no prune work at quiescence")
}
