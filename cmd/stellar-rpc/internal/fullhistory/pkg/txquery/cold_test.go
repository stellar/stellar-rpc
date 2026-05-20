package txquery

import (
	"context"
	"encoding/hex"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tamirms/streamhash"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash"
)

// fixture wires up a small cold ledger pack and matching MPHF index
// in a tempdir so the test can exercise the full lookup chain. ledgers
// is the per-ledger hashes (outer index = ledger offset from firstSeq;
// inner index = tx position in that ledger).
type fixture struct {
	firstSeq   uint32
	ledgers    [][]xdr.Hash
	packPath   string
	idxPath    string
	passphrase string
}

func newFixture(t *testing.T, firstSeq uint32, nLedgers, txPerLedger int) *fixture {
	t.Helper()
	const passphrase = network.TestNetworkPassphrase

	dir := t.TempDir()
	packPath := filepath.Join(dir, "ledgers.pack")
	idxPath := filepath.Join(dir, "txhash.idx")

	// Build ledgers + collect (hash, seq) pairs for the MPHF.
	pw, err := ledger.NewColdStoreWriter(packPath, firstSeq, ledger.ColdWriterOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pw.Close() })

	type hp struct {
		hash xdr.Hash
		seq  uint32
	}
	pairs := make([]hp, 0, nLedgers*txPerLedger)
	ledgers := make([][]xdr.Hash, nLedgers)
	for i := range nLedgers {
		seq := firstSeq + uint32(i)
		lcm, hashes := makeTestLCM(seq, txPerLedger, passphrase)
		raw, mErr := lcm.MarshalBinary()
		require.NoError(t, mErr)
		require.NoError(t, pw.AppendLedger(seq, raw))
		hh := make([]xdr.Hash, len(hashes))
		for j, h := range hashes {
			hh[j] = xdr.Hash(h)
			pairs = append(pairs, hp{hash: xdr.Hash(h), seq: seq})
		}
		ledgers[i] = hh
	}
	require.NoError(t, pw.Commit())

	// Build the MPHF via streamhash directly. The production path uses
	// SortedBuilder fed by sorted .bin files; tests don't need that
	// pipeline, so we go through UnsortedBuilder with the same cold
	// option set (payload/fingerprint/MinLedger metadata).
	iw, err := streamhash.NewUnsortedBuilder(
		context.Background(), idxPath, uint64(len(pairs)), t.TempDir(),
		txhash.ColdBuildOptions(firstSeq)...,
	)
	require.NoError(t, err)
	for _, p := range pairs {
		require.NoError(t, iw.AddKey(p.hash[:], uint64(p.seq-firstSeq)))
	}
	require.NoError(t, iw.Finish())
	require.NoError(t, iw.Close())

	return &fixture{
		firstSeq:   firstSeq,
		ledgers:    ledgers,
		packPath:   packPath,
		idxPath:    idxPath,
		passphrase: passphrase,
	}
}

func (f *fixture) open(t *testing.T) *ColdLookup {
	t.Helper()
	mph, err := txhash.OpenColdReader(f.idxPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = mph.Close() })

	cr, err := ledger.NewColdStoreReader(f.packPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close() })

	return NewColdLookup(mph, cr, f.passphrase)
}

func TestColdLookup_GetTransaction_ReturnsParsedTx(t *testing.T) {
	const nLedgers = 4
	const txPerLedger = 3
	f := newFixture(t, 1000, nLedgers, txPerLedger)
	cl := f.open(t)

	for i, hashes := range f.ledgers {
		wantSeq := f.firstSeq + uint32(i)
		for j, hash := range hashes {
			tx, err := cl.GetTransaction(context.Background(), hash)
			require.NoErrorf(t, err, "ledger=%d tx=%d hash=%x", wantSeq, j, hash[:8])

			assert.Equal(t, wantSeq, tx.Ledger.Sequence, "ledger seq mismatch")
			assert.Equal(t, hex.EncodeToString(hash[:]), tx.TransactionHash, "hash mismatch")
			assert.True(t, tx.Successful, "fixture tx should be successful")
			assert.False(t, tx.FeeBump, "fixture tx is not a fee bump")
			assert.NotEmpty(t, tx.Envelope, "Envelope XDR should be populated")
			assert.NotEmpty(t, tx.Result, "Result XDR should be populated")
			assert.NotEmpty(t, tx.Meta, "Meta XDR should be populated")
		}
	}
}

func TestColdLookup_GetTransaction_UnknownHashIsNotFound(t *testing.T) {
	f := newFixture(t, 1000, 2, 2)
	cl := f.open(t)

	// A hash that was not in the MPHF build set. Most random hashes
	// fail the fingerprint; the residual ~1/256 that survive then
	// fail the in-LCM scan inside GetTransaction. Either path maps
	// to db.ErrNoTransaction.
	var unseen xdr.Hash
	copy(unseen[:], []byte("this-32-byte-hash-was-not-seeded"))

	_, err := cl.GetTransaction(context.Background(), unseen)
	assert.ErrorIs(t, err, db.ErrNoTransaction)
}

// makeTestLCM builds a barebones V1 LedgerCloseMeta with txCount
// transactions and returns it alongside the per-tx outer hashes.
// Mirrors makeRandomLedgerCloseMeta in pkg/stores/ledger (which lives
// in a _test.go file and so can't be imported here).
func makeTestLCM(ledgerSeq uint32, txCount int, passphrase string) (xdr.LedgerCloseMeta, [][32]byte) {
	envs := make([]xdr.TransactionEnvelope, 0, txCount)
	hashes := make([][32]byte, 0, txCount)
	metas := make([]xdr.TransactionResultMeta, 0, txCount)
	const seqBase = 123_456
	for i := range txCount {
		txEnv := xdr.TransactionEnvelope{
			Type: xdr.EnvelopeTypeEnvelopeTypeTx,
			V1: &xdr.TransactionV1Envelope{
				Tx: xdr.Transaction{
					SourceAccount: xdr.MustMuxedAddress(keypair.MustRandom().Address()),
					Operations:    []xdr.Operation{},
					Fee:           xdr.Uint32(seqBase + i),
					SeqNum:        xdr.SequenceNumber(seqBase + i),
				},
			},
		}
		hash, err := network.HashTransactionInEnvelope(txEnv, passphrase)
		if err != nil {
			panic(err)
		}
		envs = append(envs, txEnv)
		hashes = append(hashes, hash)
		metas = append(metas, xdr.TransactionResultMeta{
			Result: xdr.TransactionResultPair{
				TransactionHash: xdr.Hash(hash),
				Result: xdr.TransactionResult{
					FeeCharged: 100,
					Result: xdr.TransactionResultResult{
						Code:    xdr.TransactionResultCodeTxSuccess,
						Results: &[]xdr.OperationResult{},
					},
				},
			},
			TxApplyProcessing: xdr.TransactionMeta{V: 3, V3: &xdr.TransactionMetaV3{}},
		})
	}
	lcm := xdr.LedgerCloseMeta{
		V: 1,
		V1: &xdr.LedgerCloseMetaV1{
			TxProcessing: metas,
			TxSet: xdr.GeneralizedTransactionSet{
				V: 1,
				V1TxSet: &xdr.TransactionSetV1{
					Phases: []xdr.TransactionPhase{{
						V: 0,
						V0Components: &[]xdr.TxSetComponent{{
							TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{
								Txs: envs,
							},
						}},
					}},
				},
			},
		},
	}
	lcm.V1.LedgerHeader.Header.LedgerSeq = xdr.Uint32(ledgerSeq)
	return lcm, hashes
}
