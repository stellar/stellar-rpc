package txhash

import (
	"context"
	"errors"
	"maps"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
)

var (
	_ HashIndex    = (*HotStore)(nil)
	_ HashIndex    = (*ColdReader)(nil)
	_ LedgerSource = mapLedgerSource(nil)
)

// mapLedgerSource is an in-memory LedgerSource; an unheld seq returns ErrOutOfRange.
type mapLedgerSource map[uint32][]byte

func (m mapLedgerSource) GetLedgerRaw(seq uint32) ([]byte, error) {
	raw, ok := m[seq]
	if !ok {
		return nil, stores.ErrOutOfRange
	}
	return raw, nil
}

// errLedgerSource always fails GetLedgerRaw with a fixed error.
type errLedgerSource struct{ err error }

func (e errLedgerSource) GetLedgerRaw(uint32) ([]byte, error) { return nil, e.err }

// fakeIndex is a scripted HashIndex for driving the assembly without a real index.
type fakeIndex struct {
	out map[[32]byte]uint32
	err error
}

func (f fakeIndex) Get(hash [32]byte) (uint32, error) {
	if f.err != nil {
		return 0, f.err
	}
	seq, ok := f.out[hash]
	if !ok {
		return 0, stores.ErrNotFound
	}
	return seq, nil
}

type fixtureLedgers struct {
	src     mapLedgerSource
	entries []fixtureEntry
	byHash  map[[32]byte]uint32
}

func buildLedgers(t *testing.T, seqs []uint32, txPerLedger int) fixtureLedgers {
	t.Helper()
	fl := fixtureLedgers{src: mapLedgerSource{}, byHash: map[[32]byte]uint32{}}
	for _, seq := range seqs {
		raw, hashes := buildLedgerRaw(t, seq, txPerLedger)
		fl.src[seq] = raw
		for _, h := range hashes {
			fl.entries = append(fl.entries, fixtureEntry{hash: h, seq: seq})
			fl.byHash[h] = seq
		}
	}
	return fl
}

// buildLedgerRaw builds a V2 LedgerCloseMeta, returning its bytes and the tx
// hashes computed as LedgerTransactionViewByHash recomputes them (so it verifies).
func buildLedgerRaw(t *testing.T, seq uint32, txPerLedger int) ([]byte, [][32]byte) {
	t.Helper()
	phases := make([]xdr.TransactionPhase, 0, txPerLedger)
	txProcessing := make([]xdr.TransactionResultMetaV1, 0, txPerLedger)
	hashes := make([][32]byte, 0, txPerLedger)

	for range txPerLedger {
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
		hash, err := network.HashTransactionInEnvelope(envelope, network.TestNetworkPassphrase)
		require.NoError(t, err)
		hashes = append(hashes, hash)

		opResults := []xdr.OperationResult{}
		txProcessing = append(txProcessing, xdr.TransactionResultMetaV1{
			TxApplyProcessing: xdr.TransactionMeta{V: 3, V3: &xdr.TransactionMetaV3{}},
			Result: xdr.TransactionResultPair{
				TransactionHash: hash,
				Result: xdr.TransactionResult{
					FeeCharged: 100,
					Result: xdr.TransactionResultResult{
						Code:    xdr.TransactionResultCodeTxSuccess,
						Results: &opResults,
					},
				},
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
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	return raw, hashes
}

func buildColdReader(t *testing.T, baseChunk chunk.ID, entries []fixtureEntry) *ColdReader {
	t.Helper()
	require.NotEmpty(t, entries)
	dir := t.TempDir()
	minSeq, maxSeq := entries[0].seq, entries[0].seq
	for _, e := range entries {
		minSeq = min(minSeq, e.seq)
		maxSeq = max(maxSeq, e.seq)
	}
	inputs := writeFixtureBins(t, dir, entries)
	idxPath := filepath.Join(dir, IndexFileName(baseChunk))
	require.NoError(t, BuildColdIndex(context.Background(), inputs, idxPath, minSeq, maxSeq))
	rd, err := OpenColdReader(idxPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = rd.Close() })
	return rd
}

func coldTier(t *testing.T, fl fixtureLedgers) []HashIndex {
	t.Helper()
	return []HashIndex{buildColdReader(t, chunk.ID(5), fl.entries)}
}

func TestNewTxReader_ValidatesInputs(t *testing.T) {
	_, err := NewTxReader(nil, nil, nil, "passphrase")
	require.ErrorIs(t, err, stores.ErrInvalidConfig)

	_, err = NewTxReader(nil, nil, mapLedgerSource{}, "")
	require.ErrorIs(t, err, stores.ErrInvalidConfig)
}

func TestTxReader_ColdHitResolves(t *testing.T) {
	base := chunk.ID(5).FirstLedger()
	fl := buildLedgers(t, []uint32{base, base + 1, base + 2}, 2)
	reader, err := NewTxReader(nil, coldTier(t, fl), fl.src, network.TestNetworkPassphrase)
	require.NoError(t, err)

	require.NotEmpty(t, fl.byHash)
	for h, seq := range fl.byHash {
		txv, found, err := reader.GetTransaction(h)
		require.NoError(t, err)
		require.Truef(t, found, "indexed hash %x should resolve", h)
		assert.Equal(t, h, txv.Hash)
		assert.Equal(t, seq, txv.LedgerSequence)
		assert.True(t, txv.Successful)

		var env xdr.TransactionEnvelope
		require.NoError(t, env.UnmarshalBinary(txv.Envelope))
		var res xdr.TransactionResult
		require.NoError(t, res.UnmarshalBinary(txv.Result))
		var meta xdr.TransactionMeta
		require.NoError(t, meta.UnmarshalBinary(txv.Meta))
	}
}

func TestTxReader_Miss(t *testing.T) {
	base := chunk.ID(5).FirstLedger()
	fl := buildLedgers(t, []uint32{base, base + 1}, 1)
	reader, err := NewTxReader(nil, coldTier(t, fl), fl.src, network.TestNetworkPassphrase)
	require.NoError(t, err)

	// Never indexed; verification rejects any cold false positive, so this is deterministic.
	var absent [32]byte
	for i := range absent {
		absent[i] = 0xAB
	}
	_, found, err := reader.GetTransaction(absent)
	require.NoError(t, err)
	assert.False(t, found)
}

func TestTxReader_RejectsCandidateNotInLedger(t *testing.T) {
	// An inexact candidate at a real ledger lacking the hash must be rejected as a miss.
	base := chunk.ID(5).FirstLedger()
	fl := buildLedgers(t, []uint32{base}, 2)

	var queried [32]byte // not among the ledger's transactions
	queried[0] = 0x01
	cold := []HashIndex{fakeIndex{out: map[[32]byte]uint32{queried: base}}}
	reader, err := NewTxReader(nil, cold, fl.src, network.TestNetworkPassphrase)
	require.NoError(t, err)

	_, found, err := reader.GetTransaction(queried)
	require.NoError(t, err)
	assert.False(t, found)
}

func TestTxReader_SkipsUnservableCandidateThenResolves(t *testing.T) {
	base := chunk.ID(5).FirstLedger()
	fl := buildLedgers(t, []uint32{base}, 1)
	var h [32]byte
	var realSeq uint32
	for hh, seq := range fl.byHash {
		h, realSeq = hh, seq
	}

	// First index points at an unservable ledger (skipped); the second has the real seq.
	cold := []HashIndex{
		fakeIndex{out: map[[32]byte]uint32{h: 999_999}},
		fakeIndex{out: map[[32]byte]uint32{h: realSeq}},
	}
	reader, err := NewTxReader(nil, cold, fl.src, network.TestNetworkPassphrase)
	require.NoError(t, err)

	txv, found, err := reader.GetTransaction(h)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, h, txv.Hash)
	assert.Equal(t, realSeq, txv.LedgerSequence)
}

func TestTxReader_UnavailableColdCandidateIsIncomplete(t *testing.T) {
	// A cold candidate whose ledger can't be served isn't a provable miss, so a
	// lookup that resolves nowhere surfaces as incomplete, not not-found.
	h := [32]byte{0x07}
	cold := []HashIndex{fakeIndex{out: map[[32]byte]uint32{h: 555}}}
	reader, err := NewTxReader(nil, cold, mapLedgerSource{}, network.TestNetworkPassphrase)
	require.NoError(t, err)

	_, found, err := reader.GetTransaction(h)
	assert.False(t, found)
	require.ErrorIs(t, err, stores.ErrOutOfRange)
}

func TestTxReader_ColdCandidateReadErrorIsIncomplete(t *testing.T) {
	// A corrupt/transient ledger error on a cold candidate is soft, not fatal.
	h := [32]byte{0x09}
	cold := []HashIndex{fakeIndex{out: map[[32]byte]uint32{h: 7}}}
	reader, err := NewTxReader(nil, cold, errLedgerSource{err: stores.ErrCorrupt}, network.TestNetworkPassphrase)
	require.NoError(t, err)

	_, found, err := reader.GetTransaction(h)
	assert.False(t, found)
	require.ErrorIs(t, err, stores.ErrCorrupt)
}

func TestTxReader_SurfacesSourceErrorOnMiss(t *testing.T) {
	// A transient index error with nothing else to resolve surfaces as an error, not a false miss.
	sentinel := errors.New("index down")
	cold := []HashIndex{fakeIndex{err: sentinel}}
	reader, err := NewTxReader(nil, cold, mapLedgerSource{}, network.TestNetworkPassphrase)
	require.NoError(t, err)

	_, found, err := reader.GetTransaction([32]byte{0x01})
	assert.False(t, found)
	require.ErrorIs(t, err, sentinel)
}

func TestTxReader_SourceErrorFallsThroughToCold(t *testing.T) {
	// A transient hot-store error must not block a cold-resident transaction.
	coldSeq := chunk.ID(5).FirstLedger()
	fl := buildLedgers(t, []uint32{coldSeq}, 1)
	var h [32]byte
	for hh := range fl.byHash {
		h = hh
	}

	hot := []HashIndex{fakeIndex{err: errors.New("hot blip")}}
	reader, err := NewTxReader(hot, coldTier(t, fl), fl.src, network.TestNetworkPassphrase)
	require.NoError(t, err)

	txv, found, err := reader.GetTransaction(h)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, coldSeq, txv.LedgerSequence)
}

func TestTxReader_ExactSourceNotInLedgerErrors(t *testing.T) {
	// An exact index naming a ledger that lacks the tx → ErrInconsistent.
	base := chunk.ID(5).FirstLedger()
	fl := buildLedgers(t, []uint32{base}, 2)

	var queried [32]byte
	queried[0] = 0x01
	hot := []HashIndex{fakeIndex{out: map[[32]byte]uint32{queried: base}}}
	reader, err := NewTxReader(hot, nil, fl.src, network.TestNetworkPassphrase)
	require.NoError(t, err)

	_, found, err := reader.GetTransaction(queried)
	assert.False(t, found)
	require.ErrorIs(t, err, ErrInconsistent)
}

func TestTxReader_ExactSourceUnavailableLedgerErrors(t *testing.T) {
	// An exact index naming a ledger that can't be served is also ErrInconsistent.
	queried := [32]byte{0x02}
	hot := []HashIndex{fakeIndex{out: map[[32]byte]uint32{queried: 424242}}}
	reader, err := NewTxReader(hot, nil, mapLedgerSource{}, network.TestNetworkPassphrase)
	require.NoError(t, err)

	_, found, err := reader.GetTransaction(queried)
	assert.False(t, found)
	require.ErrorIs(t, err, ErrInconsistent)
}

func TestTxReader_HotAndColdFederation(t *testing.T) {
	hotSeq := chunk.ID(10).FirstLedger()
	flHot := buildLedgers(t, []uint32{hotSeq}, 1)
	hotStore := openTestHotStore(t)
	for h, seq := range flHot.byHash {
		require.NoError(t, addEntries(hotStore, []Entry{{Hash: h, LedgerSeq: seq}}))
	}

	coldSeq := chunk.ID(5).FirstLedger()
	flCold := buildLedgers(t, []uint32{coldSeq}, 1)

	src := mapLedgerSource{}
	maps.Copy(src, flHot.src)
	maps.Copy(src, flCold.src)

	reader, err := NewTxReader(
		[]HashIndex{hotStore}, coldTier(t, flCold), src, network.TestNetworkPassphrase)
	require.NoError(t, err)

	for h, seq := range flHot.byHash {
		txv, found, err := reader.GetTransaction(h)
		require.NoError(t, err)
		require.Truef(t, found, "hot hash %x should resolve", h)
		assert.Equal(t, seq, txv.LedgerSequence)
	}
	for h, seq := range flCold.byHash {
		txv, found, err := reader.GetTransaction(h)
		require.NoError(t, err)
		require.Truef(t, found, "cold hash %x should resolve", h)
		assert.Equal(t, seq, txv.LedgerSequence)
	}
}

func TestTxReader_FanOutAcrossColdIndexes(t *testing.T) {
	flA := buildLedgers(t, []uint32{chunk.ID(5).FirstLedger()}, 1)
	flB := buildLedgers(t, []uint32{chunk.ID(2000).FirstLedger()}, 1)

	cold := []HashIndex{
		buildColdReader(t, chunk.ID(5), flA.entries),
		buildColdReader(t, chunk.ID(2000), flB.entries),
	}
	src := mapLedgerSource{}
	maps.Copy(src, flA.src)
	maps.Copy(src, flB.src)

	reader, err := NewTxReader(nil, cold, src, network.TestNetworkPassphrase)
	require.NoError(t, err)

	// A transaction in the second cold index resolves via the fan-out.
	for h, seq := range flB.byHash {
		txv, found, err := reader.GetTransaction(h)
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, seq, txv.LedgerSequence)
	}
}
