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
	_ CandidateSource = (*HotStore)(nil)
	_ CandidateSource = (*ColdReader)(nil)
	_ CandidateSource = (*ColdReaderSet)(nil)
	_ LedgerSource    = mapLedgerSource(nil)
)

// mapLedgerSource is an in-memory LedgerSource; an unheld seq returns
// ErrOutOfRange, as a real reader would for a ledger outside its coverage.
type mapLedgerSource map[uint32][]byte

func (m mapLedgerSource) GetLedgerRaw(seq uint32) ([]byte, error) {
	raw, ok := m[seq]
	if !ok {
		return nil, stores.ErrOutOfRange
	}
	return raw, nil
}

// fakeCandidateSource returns scripted candidates (or an error) to drive the
// assembly's paths without relying on a real fingerprint collision.
type fakeCandidateSource struct {
	out   map[[32]byte][]uint32
	err   error
	exact bool
}

func (f fakeCandidateSource) Candidates(hash [32]byte) ([]uint32, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.out[hash], nil
}

func (f fakeCandidateSource) Exact() bool { return f.exact }

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

// buildLedgerRaw builds a V2 LedgerCloseMeta and returns its wire bytes plus
// the tx hashes, computed the way LedgerTransactionViewByHash recomputes them
// so the view path pairs and verifies them.
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

func TestTxReader_ColdHitResolves(t *testing.T) {
	base := chunk.ID(5).FirstLedger()
	fl := buildLedgers(t, []uint32{base, base + 1, base + 2}, 2)
	set := NewColdReaderSet([]*ColdReader{buildColdReader(t, chunk.ID(5), fl.entries)})
	reader := NewTxReader([]CandidateSource{set}, fl.src, network.TestNetworkPassphrase)

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
	set := NewColdReaderSet([]*ColdReader{buildColdReader(t, chunk.ID(5), fl.entries)})
	reader := NewTxReader([]CandidateSource{set}, fl.src, network.TestNetworkPassphrase)

	// Never indexed: a cold false positive may surface but verification rejects
	// it, so the miss is deterministic.
	var absent [32]byte
	for i := range absent {
		absent[i] = 0xAB
	}
	_, found, err := reader.GetTransaction(absent)
	require.NoError(t, err)
	assert.False(t, found)
}

func TestTxReader_RejectsCandidateNotInLedger(t *testing.T) {
	// A candidate (standing in for a false positive) pointing at a real ledger
	// that lacks the hash must be rejected as a clean miss.
	base := chunk.ID(5).FirstLedger()
	fl := buildLedgers(t, []uint32{base}, 2)

	var queried [32]byte // not among the ledger's transactions
	queried[0] = 0x01
	fake := fakeCandidateSource{out: map[[32]byte][]uint32{queried: {base}}}
	reader := NewTxReader([]CandidateSource{fake}, fl.src, network.TestNetworkPassphrase)

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

	// The bogus seq has no ledger (ErrOutOfRange) and must be skipped, not fatal.
	const bogusSeq = uint32(999_999)
	fake := fakeCandidateSource{out: map[[32]byte][]uint32{h: {bogusSeq, realSeq}}}
	reader := NewTxReader([]CandidateSource{fake}, fl.src, network.TestNetworkPassphrase)

	txv, found, err := reader.GetTransaction(h)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, h, txv.Hash)
	assert.Equal(t, realSeq, txv.LedgerSequence)
}

func TestTxReader_PropagatesCandidateError(t *testing.T) {
	sentinel := errors.New("candidate source down")
	fake := fakeCandidateSource{err: sentinel}
	reader := NewTxReader([]CandidateSource{fake}, mapLedgerSource{}, network.TestNetworkPassphrase)

	_, _, err := reader.GetTransaction([32]byte{0x01})
	require.ErrorIs(t, err, sentinel)
}

func TestTxReader_ExactSourceNotInLedgerErrors(t *testing.T) {
	// An exact source naming a ledger that lacks the tx → ErrInconsistent.
	base := chunk.ID(5).FirstLedger()
	fl := buildLedgers(t, []uint32{base}, 2)

	var queried [32]byte
	queried[0] = 0x01
	exact := fakeCandidateSource{exact: true, out: map[[32]byte][]uint32{queried: {base}}}
	reader := NewTxReader([]CandidateSource{exact}, fl.src, network.TestNetworkPassphrase)

	_, found, err := reader.GetTransaction(queried)
	assert.False(t, found)
	require.ErrorIs(t, err, ErrInconsistent)
}

func TestTxReader_ExactSourceUnavailableLedgerErrors(t *testing.T) {
	// An exact source naming a ledger that can't be served is also ErrInconsistent.
	queried := [32]byte{0x02}
	exact := fakeCandidateSource{exact: true, out: map[[32]byte][]uint32{queried: {424242}}}
	reader := NewTxReader([]CandidateSource{exact}, mapLedgerSource{}, network.TestNetworkPassphrase)

	_, found, err := reader.GetTransaction(queried)
	assert.False(t, found)
	require.ErrorIs(t, err, ErrInconsistent)
}

func TestTxReader_HotAndColdFederation(t *testing.T) {
	hotSeq := chunk.ID(10).FirstLedger()
	flHot := buildLedgers(t, []uint32{hotSeq}, 1)
	hot := openTestHotStore(t)
	for h, seq := range flHot.byHash {
		require.NoError(t, hot.AddEntries([]Entry{{Hash: h, LedgerSeq: seq}}))
	}

	coldSeq := chunk.ID(5).FirstLedger()
	flCold := buildLedgers(t, []uint32{coldSeq}, 1)
	cold := NewColdReaderSet([]*ColdReader{buildColdReader(t, chunk.ID(5), flCold.entries)})

	src := mapLedgerSource{}
	maps.Copy(src, flHot.src)
	maps.Copy(src, flCold.src)

	// Cold-first on purpose: NewTxReader partitions by Exact(), so the exact hot
	// source is consulted first regardless of argument order.
	reader := NewTxReader([]CandidateSource{cold, hot}, src, network.TestNetworkPassphrase)

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

func TestColdReaderSet_FanOutAcrossReaders(t *testing.T) {
	seqA := chunk.ID(5).FirstLedger()
	seqB := chunk.ID(2000).FirstLedger()
	flA := buildLedgers(t, []uint32{seqA}, 1)
	flB := buildLedgers(t, []uint32{seqB}, 1)

	set := NewColdReaderSet([]*ColdReader{
		buildColdReader(t, chunk.ID(5), flA.entries),
		buildColdReader(t, chunk.ID(2000), flB.entries),
	})

	// A hash in the second reader is found via the fan-out.
	for h, seq := range flB.byHash {
		got, err := set.Candidates(h)
		require.NoError(t, err)
		assert.Contains(t, got, seq)
	}
}
