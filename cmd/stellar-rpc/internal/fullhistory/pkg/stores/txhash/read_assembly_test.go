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

// The hot store, a single cold index, and a fan-out over many cold indexes
// must all satisfy the one federation seam.
var (
	_ CandidateSource = (*HotStore)(nil)
	_ CandidateSource = (*ColdReader)(nil)
	_ CandidateSource = (*ColdReaderSet)(nil)
	_ LedgerSource    = mapLedgerSource(nil)
)

// ──────────────────────────────────────────────────────────────────
// Test doubles + fixtures.
// ──────────────────────────────────────────────────────────────────

// mapLedgerSource is an in-memory LedgerSource: seq → raw LCM wire bytes. A
// seq it doesn't hold returns ErrOutOfRange, mirroring a real cold ledger
// reader asked for a ledger outside its coverage.
type mapLedgerSource map[uint32][]byte

func (m mapLedgerSource) GetLedgerRaw(seq uint32) ([]byte, error) {
	raw, ok := m[seq]
	if !ok {
		return nil, stores.ErrOutOfRange
	}
	return raw, nil
}

// fakeCandidateSource returns scripted candidates (or a scripted error) so the
// assembly's verification, dedup, skip, and error paths can be driven without
// relying on a real fingerprint collision. exact controls which tier it lands
// in and whether a non-verifying candidate is an error or a skipped false
// positive.
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

// fixtureLedgers bundles built ledgers: a LedgerSource over them, the cold
// index entries to feed BuildColdIndex, and a hash→seq map for assertions.
type fixtureLedgers struct {
	src     mapLedgerSource
	entries []fixtureEntry
	byHash  map[[32]byte]uint32
}

// buildLedgers builds one V2 LedgerCloseMeta per seq, each carrying txPerLedger
// real transactions, and records each tx's true hash against its ledger.
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

// buildLedgerRaw assembles a V2 LedgerCloseMeta with txPerLedger transactions
// and returns its marshaled wire bytes plus the transactions' hashes, computed
// the same way LedgerTransactionViewByHash recomputes them (so the view path
// pairs and verifies them). Mirrors the ingest package's LCM test builder.
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

// buildColdReader builds a real cold txhash index over entries and opens a
// reader on it. The index's coverage is the entries' own seq span.
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

// ──────────────────────────────────────────────────────────────────
// TxReader.GetTransaction.
// ──────────────────────────────────────────────────────────────────

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

		// Envelope/result/meta are present and decode as real XDR.
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

	// A hash never indexed. The cold index may still surface a fingerprint
	// false positive, but verification against the real ledger rejects it, so
	// the miss is deterministic.
	var absent [32]byte
	for i := range absent {
		absent[i] = 0xAB
	}
	_, found, err := reader.GetTransaction(absent)
	require.NoError(t, err)
	assert.False(t, found)
}

func TestTxReader_RejectsCandidateNotInLedger(t *testing.T) {
	// A real ledger that does NOT contain the queried hash, reached via a
	// candidate source standing in for a fingerprint false positive. The
	// tx-details view must report not-found, and the assembly must surface that
	// as a clean miss — this is the downstream false-positive rejection.
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

	// A bogus candidate (no ledger in the source) precedes the real one. The
	// bogus seq comes back ErrOutOfRange and must be skipped, not fatal.
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
	// An exact source must never name a ledger that lacks the tx. When it does,
	// the hot index and the ledger store disagree, so the assembly errors
	// rather than masking it as a miss.
	base := chunk.ID(5).FirstLedger()
	fl := buildLedgers(t, []uint32{base}, 2)

	var queried [32]byte // not among the ledger's transactions
	queried[0] = 0x01
	exact := fakeCandidateSource{exact: true, out: map[[32]byte][]uint32{queried: {base}}}
	reader := NewTxReader([]CandidateSource{exact}, fl.src, network.TestNetworkPassphrase)

	_, found, err := reader.GetTransaction(queried)
	assert.False(t, found)
	require.ErrorIs(t, err, ErrInconsistent)
}

func TestTxReader_ExactSourceUnavailableLedgerErrors(t *testing.T) {
	// An exact source naming a ledger the ledger source can't produce is also a
	// consistency violation, not a skippable false positive.
	queried := [32]byte{0x02}
	exact := fakeCandidateSource{exact: true, out: map[[32]byte][]uint32{queried: {424242}}}
	reader := NewTxReader([]CandidateSource{exact}, mapLedgerSource{}, network.TestNetworkPassphrase)

	_, found, err := reader.GetTransaction(queried)
	assert.False(t, found)
	require.ErrorIs(t, err, ErrInconsistent)
}

func TestTxReader_HotAndColdFederation(t *testing.T) {
	// Hot tier: a real RocksDB store with one ledger's tx.
	hotSeq := chunk.ID(10).FirstLedger()
	flHot := buildLedgers(t, []uint32{hotSeq}, 1)
	hot := openTestHotStore(t)
	for h, seq := range flHot.byHash {
		require.NoError(t, hot.AddEntries([]Entry{{Hash: h, LedgerSeq: seq}}))
	}

	// Cold tier: a real cold index over a different ledger.
	coldSeq := chunk.ID(5).FirstLedger()
	flCold := buildLedgers(t, []uint32{coldSeq}, 1)
	cold := NewColdReaderSet([]*ColdReader{buildColdReader(t, chunk.ID(5), flCold.entries)})

	src := mapLedgerSource{}
	maps.Copy(src, flHot.src)
	maps.Copy(src, flCold.src)

	// Passed cold-first on purpose: NewTxReader partitions by Exact(), so the
	// exact hot source is consulted first regardless of argument order.
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

// ──────────────────────────────────────────────────────────────────
// ColdReaderSet.
// ──────────────────────────────────────────────────────────────────

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
