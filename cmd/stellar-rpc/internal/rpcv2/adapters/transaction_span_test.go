package adapters

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rpcv2test"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/hotchunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/txhash"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/txspan"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

// noTablePassphrase hashes the fixtures' envelopes to something no apply result
// claims, so every span-table build refuses the ledger and the chunk is
// ingested without tables. It is how these tests seed the pre-table tier
// without reaching into the store.
const noTablePassphrase = network.TestNetworkPassphrase

// TestSpanTableServesEveryHotShape is the hot round trip: the same ledgers
// ingested with and without span tables must serve identical transactions, and
// the tabled tier must serve them THROUGH the table.
func TestSpanTableServesEveryHotShape(t *testing.T) {
	feeBump, outerHash, innerHash := feeBumpLCM(t, testChunk.FirstLedger()+2)
	transfer := rpcv2test.SymbolContractEvent(xdr.ContractId{0xab}, "transfer", "transfer")
	withEvents, eventTxs := lcmWithTxs(t, testChunk.FirstLedger()+3,
		txSpec{events: []xdr.ContractEvent{transfer}},
		txSpec{failed: true})
	v1LCM, v1Hash := lcmV1WithClassicTx(t, testChunk.FirstLedger()+4)

	// One chunk-aligned run covering every shape the fixtures can build. The
	// two empty ledgers prove a transaction-less ledger still gets a table —
	// and, because their table needs no transaction hashes, they are the one
	// shape the wrong-passphrase tier still builds.
	lcms := []seededLedger{
		{raw: lcmBytes(t, testChunk.FirstLedger())},
		{raw: lcmBytes(t, testChunk.FirstLedger()+1)},
		{raw: feeBump, txs: 1},
		{raw: withEvents, txs: 2},
		{raw: v1LCM, txs: 1},
	}
	last := testChunk.FirstLedger() + uint32(len(lcms)) - 1

	for name, hash := range map[string]xdr.Hash{
		"fee bump by the outer hash": outerHash,
		"fee bump by the inner hash": innerHash,
		"soroban tx with events":     eventTxs[0].hash,
		"failed tx":                  eventTxs[1].hash,
		"V1 ledger close meta":       v1Hash,
	} {
		t.Run(name, func(t *testing.T) {
			tabled, tabledCtx := seedTier(t, network.PublicNetworkPassphrase, last, lcms...)
			walked, walkedCtx := seedTier(t, noTablePassphrase, last, lcms...)

			// Every ingested ledger of the tabled tier carries a table; in the
			// other tier no ledger holding a transaction does.
			for i, seeded := range lcms {
				seq := testChunk.FirstLedger() + uint32(i)
				assert.True(t, hasTable(t, tabled, seq), "ledger %d has no span table", seq)
				if seeded.txs > 0 {
					assert.False(t, hasTable(t, walked, seq), "ledger %d should have no span table", seq)
				}
			}

			reader := NewTransactionReader(network.PublicNetworkPassphrase, nil, nil)

			tables, walks := txhash.TableServedLookups(), txhash.WalkServedLookups()
			fromTable, err := reader.GetTransaction(tabledCtx, hash)
			require.NoError(t, err)
			assert.Equal(t, tables+1, txhash.TableServedLookups(), "lookup was not table-served")
			assert.Equal(t, walks, txhash.WalkServedLookups(), "lookup also walked the ledger")

			tables, walks = txhash.TableServedLookups(), txhash.WalkServedLookups()
			fromWalk, err := reader.GetTransaction(walkedCtx, hash)
			require.NoError(t, err)
			assert.Equal(t, walks+1, txhash.WalkServedLookups(), "lookup was not walk-served")
			assert.Equal(t, tables, txhash.TableServedLookups(), "lookup used a table")

			assert.Equal(t, fromWalk, fromTable, "the table and the walk served different transactions")
		})
	}
}

// TestSpanTableCollidingPrefixIsACleanMiss pins the confirmation step: a hash
// sharing a stored transaction's four-byte index prefix reaches its entry and
// must be rejected against the element, as a miss rather than an error or a
// wrong transaction.
func TestSpanTableCollidingPrefixIsACleanMiss(t *testing.T) {
	lcm, txs := lcmWithTxs(t, testChunk.FirstLedger(), txSpec{})
	_, ctx := seedTier(t, network.PublicNetworkPassphrase, testChunk.FirstLedger(),
		seededLedger{raw: lcm, txs: 1})
	reader := NewTransactionReader(network.PublicNetworkPassphrase, nil, nil)

	collides := txs[0].hash
	collides[31] ^= 0xff
	_, err := reader.GetTransaction(ctx, collides)
	assert.ErrorIs(t, err, store.ErrNoTransaction)
}

// seededLedger is one fixture ledger plus its transaction count, which decides
// whether a span table can be built for it without the right passphrase.
type seededLedger struct {
	raw []byte
	txs int
}

// seedTier stands up a catalog and registry of its own holding lcms from
// testChunk's first ledger, with span tables keyed under passphrase, and
// returns the hot DB plus a request context over the seeded view.
func seedTier(
	t *testing.T, passphrase string, latest uint32, lcms ...seededLedger,
) (*hotchunk.DB, context.Context) {
	t.Helper()
	raws := make([][]byte, len(lcms))
	for i := range lcms {
		raws[i] = lcms[i].raw
	}
	cat := openTestCatalog(t)
	r := query.NewRegistry(cat, geometry.NewRetention(0, testChunk))
	db := seedHotChunkLCMsAs(t, cat, r, testChunk, passphrase, raws...)
	r.SetLatestLedger(latest, query.CloseTimeAt(closeTimeFor(latest)))
	return db, viewCtx(t, r)
}

// hasTable reports whether seq's ledger carries a span table in db.
func hasTable(t *testing.T, db *hotchunk.DB, seq uint32) bool {
	t.Helper()
	err := db.Ledgers().WithTxTable(seq, func(txspan.Table, txspan.LedgerHeader, txspan.PieceReader) error { return nil })
	if errors.Is(err, stores.ErrNoTable) {
		return false
	}
	require.NoError(t, err)
	return true
}
