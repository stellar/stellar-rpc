package ledger

import (
	"bytes"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
)

func silentLogger() *supportlog.Entry {
	var buf bytes.Buffer
	log := supportlog.New()
	log.SetLevel(logrus.DebugLevel)
	log.SetOutput(&buf)
	return log
}

func openTestHotStore(t *testing.T) *HotStore {
	t.Helper()
	h, err := NewHotStore(t.TempDir(), silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = h.Close() })
	return h
}

func TestNewHotStore_ValidatesInputs(t *testing.T) {
	_, err := NewHotStore("", silentLogger())
	require.ErrorIs(t, err, rocksdb.ErrInvalidConfig)

	_, err = NewHotStore(t.TempDir(), nil)
	require.ErrorIs(t, err, rocksdb.ErrInvalidConfig)
}

func TestNewHotStore_CreatesMissingDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "subdir-never-created")
	h, err := NewHotStore(path, silentLogger())
	require.NoError(t, err)
	require.NotNil(t, h)
	t.Cleanup(func() { _ = h.Close() })
}

func TestHotStore_CloseIsIdempotent(t *testing.T) {
	h, err := NewHotStore(t.TempDir(), silentLogger())
	require.NoError(t, err)

	require.NoError(t, h.Close())
	require.NoError(t, h.Close())
}

func TestHotStore_AddGetRoundTripVerbatim(t *testing.T) {
	h := openTestHotStore(t)

	// Miss.
	_, err := h.GetLedgerRaw(42)
	require.ErrorIs(t, err, stores.ErrNotFound)

	// Single-entry write.
	payload := []byte("arbitrary opaque bytes the store has no opinion about")
	require.NoError(t, h.AddLedgers([]Entry{{Seq: 42, Bytes: payload}}))
	got, err := h.GetLedgerRaw(42)
	require.NoError(t, err)
	assert.Equal(t, payload, got)

	// Overwrite.
	updated := []byte("different bytes")
	require.NoError(t, h.AddLedgers([]Entry{{Seq: 42, Bytes: updated}}))
	got, err = h.GetLedgerRaw(42)
	require.NoError(t, err)
	assert.Equal(t, updated, got)

	// Empty slice — no-op, no error.
	require.NoError(t, h.AddLedgers(nil))
	require.NoError(t, h.AddLedgers([]Entry{}))
}

func TestHotStore_AddLedgersMultipleEntries(t *testing.T) {
	h := openTestHotStore(t)

	entries := []Entry{
		{Seq: 100, Bytes: []byte("ledger 100 payload")},
		{Seq: 101, Bytes: []byte("ledger 101 payload")},
		{Seq: 102, Bytes: []byte("ledger 102 payload")},
	}
	require.NoError(t, h.AddLedgers(entries))
	for _, e := range entries {
		got, err := h.GetLedgerRaw(e.Seq)
		require.NoError(t, err)
		assert.Equal(t, e.Bytes, got)
	}
}

func TestHotStore_IterateLedgers(t *testing.T) {
	h := openTestHotStore(t)
	for _, seq := range []uint32{10, 20, 30, 40, 50} {
		require.NoError(t, h.AddLedgers([]Entry{{Seq: seq, Bytes: []byte("v")}}))
	}

	// Full window.
	var seen []uint32
	for e, err := range h.IterateLedgers(10, 50) {
		require.NoError(t, err)
		seen = append(seen, e.Seq)
	}
	assert.Equal(t, []uint32{10, 20, 30, 40, 50}, seen)

	// Partial window starting mid-keyspace.
	seen = nil
	for e, err := range h.IterateLedgers(20, 40) {
		require.NoError(t, err)
		seen = append(seen, e.Seq)
	}
	assert.Equal(t, []uint32{20, 30, 40}, seen)

	// Window below the store's min — empty.
	seen = nil
	for e, err := range h.IterateLedgers(0, 5) {
		require.NoError(t, err)
		seen = append(seen, e.Seq)
	}
	assert.Empty(t, seen)

	// start > end — no-op, no error.
	seen = nil
	for e, err := range h.IterateLedgers(40, 20) {
		require.NoError(t, err)
		seen = append(seen, e.Seq)
	}
	assert.Empty(t, seen)

	// Mid-walk break — caller controls when to stop.
	seen = nil
	for e, err := range h.IterateLedgers(10, 50) {
		require.NoError(t, err)
		seen = append(seen, e.Seq)
		if len(seen) == 2 {
			break
		}
	}
	assert.Equal(t, []uint32{10, 20}, seen)
}

func TestHotStore_IterateLedgersVisibleGap(t *testing.T) {
	h := openTestHotStore(t)
	// Non-contiguous keyspace: missing 30.
	for _, seq := range []uint32{10, 20, 40, 50} {
		require.NoError(t, h.AddLedgers([]Entry{{Seq: seq, Bytes: []byte("v")}}))
	}

	var seen []uint32
	for e, err := range h.IterateLedgers(10, 50) {
		require.NoError(t, err)
		seen = append(seen, e.Seq)
	}
	assert.Equal(t, []uint32{10, 20, 40, 50}, seen)
}

func TestHotStore_GracefulCloseAndReopen(t *testing.T) {
	path := t.TempDir()

	seeded := []Entry{
		{Seq: 5, Bytes: []byte("payload-5")},
		{Seq: 10, Bytes: []byte("payload-10")},
		{Seq: 15, Bytes: []byte("payload-15")},
	}

	first, err := NewHotStore(path, silentLogger())
	require.NoError(t, err)
	require.NoError(t, first.AddLedgers(seeded))
	require.NoError(t, first.Close())

	second, err := NewHotStore(path, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = second.Close() })

	for _, want := range seeded {
		got, err := second.GetLedgerRaw(want.Seq)
		require.NoError(t, err)
		assert.Equal(t, want.Bytes, got)
	}
}

func TestHotStore_PostCloseOps(t *testing.T) {
	h, err := NewHotStore(t.TempDir(), silentLogger())
	require.NoError(t, err)
	require.NoError(t, h.Close())

	require.ErrorIs(t, h.AddLedgers([]Entry{{Seq: 1, Bytes: []byte("v")}}), rocksdb.ErrStoreClosed)
	_, err = h.GetLedgerRaw(1)
	require.ErrorIs(t, err, rocksdb.ErrStoreClosed)
	var iterErr error
	for _, e := range h.IterateLedgers(0, 100) {
		iterErr = e
	}
	require.ErrorIs(t, iterErr, rocksdb.ErrStoreClosed)

	require.ErrorIs(t, h.AddLedgers(nil), rocksdb.ErrStoreClosed)
	require.ErrorIs(t, h.AddLedgers([]Entry{}), rocksdb.ErrStoreClosed)

	iterErr = nil
	for _, e := range h.IterateLedgers(100, 50) {
		iterErr = e
	}
	require.ErrorIs(t, iterErr, rocksdb.ErrStoreClosed)
}

func TestHotStore_ConcurrentOpsAndCloseRaceFree(t *testing.T) {
	h := openTestHotStore(t)
	for i := range uint32(50) {
		require.NoError(t, h.AddLedgers([]Entry{{Seq: i, Bytes: []byte("v")}}))
	}

	var wg sync.WaitGroup
	var stop atomic.Bool
	const workers = 4
	for w := range workers {
		wg.Go(func() {
			for i := uint32(0); !stop.Load(); i++ {
				_ = h.AddLedgers([]Entry{{Seq: uint32(w)*1_000_000 + i, Bytes: []byte("v")}})
			}
		})
		wg.Go(func() {
			for i := uint32(0); !stop.Load(); i++ {
				_, _ = h.GetLedgerRaw(i % 50)
			}
		})
		wg.Go(func() {
			for !stop.Load() {
				for _, err := range h.IterateLedgers(0, 49) {
					if err != nil {
						break
					}
				}
			}
		})
	}

	time.Sleep(50 * time.Millisecond)
	require.NoError(t, h.Close())
	stop.Store(true)
	wg.Wait()

	postClose := []Entry{{Seq: 1, Bytes: []byte("v")}}
	require.ErrorIs(t, h.AddLedgers(postClose), rocksdb.ErrStoreClosed)
}

func TestHotStore_XDRRoundTrip(t *testing.T) {
	const ledgerSeq uint32 = 12_345_678
	const txCount = 5

	lcm, wantHashes := makeRandomLedgerCloseMeta(ledgerSeq, txCount)
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)

	h := openTestHotStore(t)
	require.NoError(t, h.AddLedgers([]Entry{{Seq: ledgerSeq, Bytes: raw}}))

	gotRaw, err := h.GetLedgerRaw(ledgerSeq)
	require.NoError(t, err)
	assert.Equal(t, raw, gotRaw, "stored bytes must come back verbatim")

	var decoded xdr.LedgerCloseMeta
	require.NoError(t, decoded.UnmarshalBinary(gotRaw))

	require.NotNil(t, decoded.V1)
	assert.Equal(t, xdr.Uint32(ledgerSeq), decoded.V1.LedgerHeader.Header.LedgerSeq)

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
// per-tx envelope hashes under the test-network passphrase.
// Shared fixture for hot + cold store tests in this package.
func makeRandomLedgerCloseMeta(
	ledgerSeq uint32,
	txCount int,
) (xdr.LedgerCloseMeta, [][32]byte) {
	const networkPassphrase = network.TestNetworkPassphrase
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
		hash, err := network.HashTransactionInEnvelope(txEnv, networkPassphrase)
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
