package rocksdb

import (
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
)

var _ stores.LedgerHotStore = (*LedgerHotStore)(nil)

func openTestLedgerHotStore(t *testing.T) *LedgerHotStore {
	t.Helper()
	l, err := NewLedgerHotStore(t.TempDir(), silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })
	return l
}

func TestNewLedgerHotStore_ValidatesInputs(t *testing.T) {
	_, err := NewLedgerHotStore("", silentLogger())
	require.ErrorIs(t, err, ErrInvalidConfig)

	_, err = NewLedgerHotStore(t.TempDir(), nil)
	require.ErrorIs(t, err, ErrInvalidConfig)
}

func TestNewLedgerHotStore_CreatesMissingDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "subdir-never-created")
	l, err := NewLedgerHotStore(path, silentLogger())
	require.NoError(t, err)
	require.NotNil(t, l)
	t.Cleanup(func() { _ = l.Close() })
}

func TestLedgerHotStore_CloseIsIdempotent(t *testing.T) {
	l, err := NewLedgerHotStore(t.TempDir(), silentLogger())
	require.NoError(t, err)

	require.NoError(t, l.Close())
	require.NoError(t, l.Close())
}

func TestLedgerHotStore_DefaultTuningInstallsNoCacheOrFilter(t *testing.T) {
	l := openTestLedgerHotStore(t)
	assert.Nil(t, l.store.cache)
	assert.Nil(t, l.store.filter)
}

func TestLedgerHotStore_AddGetRoundTripVerbatim(t *testing.T) {
	l := openTestLedgerHotStore(t)

	// Miss.
	_, err := l.GetLedgerRaw(42)
	require.ErrorIs(t, err, stores.ErrNotFound)

	// Single-entry write.
	payload := []byte("arbitrary opaque bytes the store has no opinion about")
	require.NoError(t, l.AddLedgers([]stores.LedgerEntry{{Seq: 42, Bytes: payload}}))
	got, err := l.GetLedgerRaw(42)
	require.NoError(t, err)
	assert.Equal(t, payload, got)

	// Overwrite.
	updated := []byte("different bytes")
	require.NoError(t, l.AddLedgers([]stores.LedgerEntry{{Seq: 42, Bytes: updated}}))
	got, err = l.GetLedgerRaw(42)
	require.NoError(t, err)
	assert.Equal(t, updated, got)

	// Empty slice — no-op, no error.
	require.NoError(t, l.AddLedgers(nil))
	require.NoError(t, l.AddLedgers([]stores.LedgerEntry{}))
}

func TestLedgerHotStore_AddLedgersMultipleEntries(t *testing.T) {
	l := openTestLedgerHotStore(t)

	entries := []stores.LedgerEntry{
		{Seq: 100, Bytes: []byte("ledger 100 payload")},
		{Seq: 101, Bytes: []byte("ledger 101 payload")},
		{Seq: 102, Bytes: []byte("ledger 102 payload")},
	}
	require.NoError(t, l.AddLedgers(entries))
	for _, e := range entries {
		got, err := l.GetLedgerRaw(e.Seq)
		require.NoError(t, err)
		assert.Equal(t, e.Bytes, got)
	}
}

func TestLedgerHotStore_IterateLedgers(t *testing.T) {
	l := openTestLedgerHotStore(t)
	for _, seq := range []uint32{10, 20, 30, 40, 50} {
		require.NoError(t, l.AddLedgers([]stores.LedgerEntry{{Seq: seq, Bytes: []byte("v")}}))
	}

	// Full window.
	var seen []uint32
	for e, err := range l.IterateLedgers(10, 50) {
		require.NoError(t, err)
		seen = append(seen, e.Seq)
	}
	assert.Equal(t, []uint32{10, 20, 30, 40, 50}, seen)

	// Partial window starting mid-keyspace.
	seen = nil
	for e, err := range l.IterateLedgers(20, 40) {
		require.NoError(t, err)
		seen = append(seen, e.Seq)
	}
	assert.Equal(t, []uint32{20, 30, 40}, seen)

	// Window below the store's min — empty.
	seen = nil
	for e, err := range l.IterateLedgers(0, 5) {
		require.NoError(t, err)
		seen = append(seen, e.Seq)
	}
	assert.Empty(t, seen)

	// start > end — no-op, no error.
	seen = nil
	for e, err := range l.IterateLedgers(40, 20) {
		require.NoError(t, err)
		seen = append(seen, e.Seq)
	}
	assert.Empty(t, seen)

	// Mid-walk break — caller controls when to stop.
	seen = nil
	for e, err := range l.IterateLedgers(10, 50) {
		require.NoError(t, err)
		seen = append(seen, e.Seq)
		if len(seen) == 2 {
			break
		}
	}
	assert.Equal(t, []uint32{10, 20}, seen)
}

func TestLedgerHotStore_IterateLedgersVisibleGap(t *testing.T) {
	l := openTestLedgerHotStore(t)
	// Non-contiguous keyspace: missing 30.
	for _, seq := range []uint32{10, 20, 40, 50} {
		require.NoError(t, l.AddLedgers([]stores.LedgerEntry{{Seq: seq, Bytes: []byte("v")}}))
	}

	var seen []uint32
	for e, err := range l.IterateLedgers(10, 50) {
		require.NoError(t, err)
		seen = append(seen, e.Seq)
	}
	assert.Equal(t, []uint32{10, 20, 40, 50}, seen)
}

func TestLedgerHotStore_GracefulCloseAndReopen(t *testing.T) {
	path := t.TempDir()

	seeded := []stores.LedgerEntry{
		{Seq: 5, Bytes: []byte("payload-5")},
		{Seq: 10, Bytes: []byte("payload-10")},
		{Seq: 15, Bytes: []byte("payload-15")},
	}

	first, err := NewLedgerHotStore(path, silentLogger())
	require.NoError(t, err)
	require.NoError(t, first.AddLedgers(seeded))
	require.NoError(t, first.Close())

	second, err := NewLedgerHotStore(path, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = second.Close() })

	for _, want := range seeded {
		got, err := second.GetLedgerRaw(want.Seq)
		require.NoError(t, err)
		assert.Equal(t, want.Bytes, got)
	}
}

func TestLedgerHotStore_PostCloseOps(t *testing.T) {
	l, err := NewLedgerHotStore(t.TempDir(), silentLogger())
	require.NoError(t, err)
	require.NoError(t, l.Close())

	require.ErrorIs(t, l.AddLedgers([]stores.LedgerEntry{{Seq: 1, Bytes: []byte("v")}}), stores.ErrStoreClosed)
	_, err = l.GetLedgerRaw(1)
	require.ErrorIs(t, err, stores.ErrStoreClosed)
	var iterErr error
	for _, e := range l.IterateLedgers(0, 100) {
		iterErr = e
	}
	require.ErrorIs(t, iterErr, stores.ErrStoreClosed)

	require.ErrorIs(t, l.AddLedgers(nil), stores.ErrStoreClosed)
	require.ErrorIs(t, l.AddLedgers([]stores.LedgerEntry{}), stores.ErrStoreClosed)

	iterErr = nil
	for _, e := range l.IterateLedgers(100, 50) {
		iterErr = e
	}
	require.ErrorIs(t, iterErr, stores.ErrStoreClosed)
}

func TestLedgerHotStore_ConcurrentOpsAndCloseRaceFree(t *testing.T) {
	l := openTestLedgerHotStore(t)
	for i := range uint32(50) {
		require.NoError(t, l.AddLedgers([]stores.LedgerEntry{{Seq: i, Bytes: []byte("v")}}))
	}

	var wg sync.WaitGroup
	var stop atomic.Bool
	const workers = 4
	for w := range workers {
		wg.Go(func() {
			for i := uint32(0); !stop.Load(); i++ {
				_ = l.AddLedgers([]stores.LedgerEntry{{Seq: uint32(w)*1_000_000 + i, Bytes: []byte("v")}})
			}
		})
		wg.Go(func() {
			for i := uint32(0); !stop.Load(); i++ {
				_, _ = l.GetLedgerRaw(i % 50)
			}
		})
		wg.Go(func() {
			for !stop.Load() {
				for _, err := range l.IterateLedgers(0, 49) {
					if err != nil {
						break
					}
				}
			}
		})
	}

	time.Sleep(50 * time.Millisecond)
	require.NoError(t, l.Close())
	stop.Store(true)
	wg.Wait()

	postClose := []stores.LedgerEntry{{Seq: 1, Bytes: []byte("v")}}
	require.ErrorIs(t, l.AddLedgers(postClose), stores.ErrStoreClosed)
}

func TestLedgerHotStore_XDRRoundTrip(t *testing.T) {
	const ledgerSeq uint32 = 12_345_678
	const txCount = 5

	lcm, wantHashes := makeRandomLedgerCloseMeta(ledgerSeq, txCount, network.TestNetworkPassphrase)
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)

	l := openTestLedgerHotStore(t)
	require.NoError(t, l.AddLedgers([]stores.LedgerEntry{{Seq: ledgerSeq, Bytes: raw}}))

	gotRaw, err := l.GetLedgerRaw(ledgerSeq)
	require.NoError(t, err)
	assert.Equal(t, raw, gotRaw, "stored bytes must come back verbatim")

	var decoded xdr.LedgerCloseMeta
	require.NoError(t, decoded.UnmarshalBinary(gotRaw))

	// In-LCM ledger sequence round-trips through marshal / unmarshal.
	require.NotNil(t, decoded.V1)
	assert.Equal(t, xdr.Uint32(ledgerSeq), decoded.V1.LedgerHeader.Header.LedgerSeq)

	// Each transaction's hash is identical after unmarshal — the
	// test computes each hash from the decoded envelope and
	// compares against the expected hashes the fixture returned.
	require.NotNil(t, decoded.V1.TxSet.V1TxSet)
	require.Len(t, decoded.V1.TxSet.V1TxSet.Phases, 1)
	comps := decoded.V1.TxSet.V1TxSet.Phases[0].V0Components
	require.NotNil(t, comps)
	require.Len(t, *comps, 1)
	gotEnvs := (*comps)[0].TxsMaybeDiscountedFee.Txs
	require.Len(t, gotEnvs, txCount)

	gotHashes := make([][32]byte, len(gotEnvs))
	for i, env := range gotEnvs {
		h, err := network.HashTransactionInEnvelope(env, network.TestNetworkPassphrase)
		require.NoError(t, err)
		gotHashes[i] = h
	}
	assert.Equal(t, wantHashes, gotHashes, "tx hashes must match across marshal/unmarshal")
}

// makeRandomLedgerCloseMeta builds a barebones LedgerCloseMetaV1
// carrying txCount random transactions and returns it plus the
// per-tx envelope hashes under networkPassphrase. Used only by the
// XDR round-trip test below — inline rather than in a shared
// testutil package so the fixture stays next to its single caller.
func makeRandomLedgerCloseMeta(
	ledgerSeq uint32,
	txCount int,
	networkPassphrase string,
) (xdr.LedgerCloseMeta, [][32]byte) {
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
		h, err := network.HashTransactionInEnvelope(txEnv, networkPassphrase)
		if err != nil {
			panic(err)
		}
		envs = append(envs, txEnv)
		hashes = append(hashes, h)
		metas = append(metas, xdr.TransactionResultMeta{
			Result: xdr.TransactionResultPair{
				TransactionHash: xdr.Hash(h),
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
