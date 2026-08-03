package txhash

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

var (
	_ HashIndex    = (*HotStore)(nil)
	_ HashIndex    = (*ColdReader)(nil)
	_ LedgerSource = mapLedgerSource(nil)
	_ LedgerSource = (*poisoningLedgerSource)(nil)
)

// mapLedgerSource is an in-memory LedgerSource shaped like the cold tier: it
// lends storage it already holds, so the release is a no-op. An unheld seq
// returns ErrOutOfRange.
type mapLedgerSource map[uint32][]byte

func (m mapLedgerSource) WithLedger(seq uint32, fn func(raw []byte) error) error {
	raw, ok := m[seq]
	if !ok {
		return stores.ErrOutOfRange
	}
	return fn(raw[:len(raw):len(raw)])
}

// errLedgerSource always fails the borrow with a fixed error.
type errLedgerSource struct{ err error }

func (e errLedgerSource) WithLedger(uint32, func([]byte) error) error { return e.err }

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
	idxPath := filepath.Join(dir, indexFileName(baseChunk))
	require.NoError(t, BuildColdIndex(
		context.Background(), inputs, idxPath, minSeq, maxSeq))
	rd, err := OpenColdReader(idxPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = rd.Close() })
	return rd
}

func coldTier(t *testing.T, fl fixtureLedgers) func() ([]HashIndex, error) {
	t.Helper()
	return fixedCold([]HashIndex{buildColdReader(t, chunk.ID(5), fl.entries)})
}

func fixedCold(idxs []HashIndex) func() ([]HashIndex, error) {
	return func() ([]HashIndex, error) { return idxs, nil }
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
	cold := fixedCold([]HashIndex{fakeIndex{out: map[[32]byte]uint32{queried: base}}})
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
	cold := fixedCold([]HashIndex{
		fakeIndex{out: map[[32]byte]uint32{h: 999_999}},
		fakeIndex{out: map[[32]byte]uint32{h: realSeq}},
	})
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
	cold := fixedCold([]HashIndex{fakeIndex{out: map[[32]byte]uint32{h: 555}}})
	reader, err := NewTxReader(nil, cold, mapLedgerSource{}, network.TestNetworkPassphrase)
	require.NoError(t, err)

	_, found, err := reader.GetTransaction(h)
	assert.False(t, found)
	require.ErrorIs(t, err, stores.ErrOutOfRange)
}

func TestTxReader_ColdCandidateReadErrorIsIncomplete(t *testing.T) {
	// A corrupt/transient ledger error on a cold candidate is soft, not fatal.
	h := [32]byte{0x09}
	cold := fixedCold([]HashIndex{fakeIndex{out: map[[32]byte]uint32{h: 7}}})
	reader, err := NewTxReader(nil, cold, errLedgerSource{err: stores.ErrCorrupt}, network.TestNetworkPassphrase)
	require.NoError(t, err)

	_, found, err := reader.GetTransaction(h)
	assert.False(t, found)
	require.ErrorIs(t, err, stores.ErrCorrupt)
}

func TestTxReader_SurfacesSourceErrorOnMiss(t *testing.T) {
	// A transient index error with nothing else to resolve surfaces as an error, not a false miss.
	sentinel := errors.New("index down")
	cold := fixedCold([]HashIndex{fakeIndex{err: sentinel}})
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

func TestTxReader_HotHitSkipsColdEnumeration(t *testing.T) {
	hotSeq := chunk.ID(10).FirstLedger()
	fl := buildLedgers(t, []uint32{hotSeq}, 1)
	var h [32]byte
	for hh := range fl.byHash {
		h = hh
	}

	hot := []HashIndex{fakeIndex{out: map[[32]byte]uint32{h: hotSeq}}}
	cold := func() ([]HashIndex, error) {
		return nil, errors.New("cold tier enumerated despite a hot hit")
	}
	reader, err := NewTxReader(hot, cold, fl.src, network.TestNetworkPassphrase)
	require.NoError(t, err)

	txv, found, err := reader.GetTransaction(h)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, hotSeq, txv.LedgerSequence)
}

func TestTxReader_ColdProviderErrorIsHard(t *testing.T) {
	sentinel := errors.New("cold enumeration failed")
	cold := func() ([]HashIndex, error) { return nil, sentinel }
	reader, err := NewTxReader(nil, cold, mapLedgerSource{}, network.TestNetworkPassphrase)
	require.NoError(t, err)

	_, found, err := reader.GetTransaction([32]byte{0x01})
	assert.False(t, found)
	require.ErrorIs(t, err, sentinel)
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
	hotStore, _ := openPackedStoreAt(t, t.TempDir(), chunk.ID(10), windowLedgers)
	hotHashes := make([][32]byte, 0, len(flHot.byHash))
	for h := range flHot.byHash {
		hotHashes = append(hotHashes, h)
	}
	ingestLedger(t, hotStore, hotSeq, hotHashes)

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

	cold := fixedCold([]HashIndex{
		buildColdReader(t, chunk.ID(5), flA.entries),
		buildColdReader(t, chunk.ID(2000), flB.entries),
	})
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

// poisoningLedgerSource mirrors the hot store's shape — a pool of decode
// buffers, lent and returned — and scribbles over every buffer before filling
// it. A view that outlived its borrow, or two lookups sharing one buffer, then
// shows up as corrupt bytes rather than as bytes that happen to still be right.
// It also lends deliberately fat buffers, so anything reading past the ledger's
// length reads poison.
type poisoningLedgerSource struct {
	src   mapLedgerSource
	pool  sync.Pool
	loans atomic.Int64
}

func newPoisoningSource(src mapLedgerSource) *poisoningLedgerSource {
	p := &poisoningLedgerSource{src: src}
	p.pool.New = func() any { return new([]byte) }
	return p
}

func (p *poisoningLedgerSource) WithLedger(seq uint32, fn func(raw []byte) error) error {
	raw, ok := p.src[seq]
	if !ok {
		return stores.ErrOutOfRange
	}
	p.loans.Add(1)
	buf, _ := p.pool.Get().(*[]byte)
	defer p.pool.Put(buf)
	// Scribble over the whole buffer, including the slack a previous, longer
	// ledger left behind, then refill only the ledger's own length.
	full := (*buf)[:cap(*buf)]
	for i := range full {
		full[i] = 0xEE
	}
	*buf = append((*buf)[:0], raw...)
	lent := *buf
	return fn(lent[:len(lent):len(lent)])
}

// directView is the pre-fix extraction: the SDK reading the whole ledger.
func directView(t *testing.T, fl fixtureLedgers, hash [32]byte) ingest.LedgerTransactionView {
	t.Helper()
	seq, ok := fl.byHash[hash]
	require.True(t, ok)
	v, found, err := ingest.LedgerTransactionViewByHash(
		xdr.LedgerCloseMetaView(fl.src[seq]), hash, network.TestNetworkPassphrase)
	require.NoError(t, err)
	require.True(t, found)
	return v
}

// mergeLedgers folds several fixtures into one probe-able set, so a test can
// mix ledgers of very different sizes.
func mergeLedgers(fls ...fixtureLedgers) fixtureLedgers {
	out := fixtureLedgers{src: mapLedgerSource{}, byHash: map[[32]byte]uint32{}}
	for _, fl := range fls {
		maps.Copy(out.src, fl.src)
		maps.Copy(out.byHash, fl.byHash)
		out.entries = append(out.entries, fl.entries...)
	}
	return out
}

// TestTxReader_LookupsMatchDirectExtraction is the differential guard on the
// whole read: whatever the ledger source does with its buffers, the transaction
// a lookup returns must be byte-for-byte what the SDK extracts straight from the
// ledger. The cases differ only in the source's buffer behavior and in what
// each additionally proves, so they run as one table over the same corpus.
func TestTxReader_LookupsMatchDirectExtraction(t *testing.T) {
	for _, tc := range []struct {
		name string
		// fixture builds the ledgers; lopsided ones exercise a buffer grown by
		// a big ledger and then handed to a small one.
		fixture func(t *testing.T) fixtureLedgers
		// source is the probe's ledger source over that fixture.
		source func(fl fixtureLedgers) LedgerSource
		// before runs lookups whose results are discarded, to leave the source
		// in the state the case is really about.
		before func(t *testing.T, r *TxReader, fl fixtureLedgers)
		// after asserts whatever the case adds beyond byte-equality.
		after func(t *testing.T, fl fixtureLedgers, src LedgerSource)
	}{
		{
			name:    "source lends its own storage",
			fixture: func(t *testing.T) fixtureLedgers { return buildLedgers(t, []uint32{100, 200}, 3) },
			source:  func(fl fixtureLedgers) LedgerSource { return fl.src },
		},
		{
			name:    "source recycles a poisoned buffer",
			fixture: func(t *testing.T) fixtureLedgers { return buildLedgers(t, []uint32{100, 200}, 3) },
			source:  func(fl fixtureLedgers) LedgerSource { return newPoisoningSource(fl.src) },
			// Churn the pool first: every buffer a lookup returns is poisoned
			// and refilled several times over, so a result that still aliased
			// one comes back corrupt rather than coincidentally intact.
			before: func(t *testing.T, r *TxReader, fl fixtureLedgers) {
				t.Helper()
				for range 4 {
					for hash := range fl.byHash {
						_, _, err := r.GetTransaction(hash)
						require.NoError(t, err)
					}
				}
			},
			after: func(t *testing.T, _ fixtureLedgers, src LedgerSource) {
				t.Helper()
				p, ok := src.(*poisoningLedgerSource)
				require.True(t, ok)
				assert.Positive(t, p.loans.Load(), "the probe must have borrowed")
			},
		},
		{
			name: "small ledger read through a buffer a big one grew",
			fixture: func(t *testing.T) fixtureLedgers {
				t.Helper()
				fl := mergeLedgers(buildLedgers(t, []uint32{100}, 40), buildLedgers(t, []uint32{200}, 1))
				require.Greater(t, len(fl.src[100]), 4*len(fl.src[200]),
					"the fixture must actually be lopsided")
				return fl
			},
			source: func(fl fixtureLedgers) LedgerSource { return newPoisoningSource(fl.src) },
			// Read the big ledger first so the pooled buffer is grown, and its
			// slack still holds the big ledger's bytes.
			before: func(t *testing.T, r *TxReader, fl fixtureLedgers) {
				t.Helper()
				for hash, seq := range fl.byHash {
					if seq == 100 {
						_, found, err := r.GetTransaction(hash)
						require.NoError(t, err)
						require.True(t, found)
					}
				}
			},
		},
		{
			name: "lopsided ledgers from a source that lends its own storage",
			fixture: func(t *testing.T) fixtureLedgers {
				t.Helper()
				return mergeLedgers(buildLedgers(t, []uint32{100}, 40), buildLedgers(t, []uint32{200}, 1))
			},
			source: func(fl fixtureLedgers) LedgerSource { return fl.src },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fl := tc.fixture(t)
			source := tc.source(fl)
			reader, err := NewTxReader(
				[]HashIndex{fakeIndex{out: fl.byHash}}, nil, source, network.TestNetworkPassphrase)
			require.NoError(t, err)

			want := map[[32]byte]ingest.LedgerTransactionView{}
			for hash := range fl.byHash {
				want[hash] = directView(t, fl, hash)
			}
			// Nothing may write back into the fixture: a source that lends its
			// own storage must get it back untouched, and one that lends a copy
			// must never reach past it.
			untouched := map[uint32][]byte{}
			for seq, raw := range fl.src {
				untouched[seq] = bytes.Clone(raw)
			}
			if tc.before != nil {
				tc.before(t, reader, fl)
			}
			for hash := range fl.byHash {
				got, found, err := reader.GetTransaction(hash)
				require.NoError(t, err)
				require.Truef(t, found, "hash %x should resolve", hash)
				assert.Equalf(t, want[hash], got, "lookup differs for hash %x", hash)
			}
			for seq, raw := range fl.src {
				assert.Equalf(t, untouched[seq], raw, "ledger %d was written through", seq)
			}
			if tc.after != nil {
				tc.after(t, fl, source)
			}
		})
	}
}

// TestCompactView_CopiesOutOfTheLedgerBuffer pins the copy-out contract
// directly: after it, no field points into the buffer the view was read from,
// the bytes are unchanged, an absent field stays absent, and no slice can be
// appended into its neighbor.
func TestCompactView_CopiesOutOfTheLedgerBuffer(t *testing.T) {
	buf := []byte("ENVELOPERESULTMETADIAG1DIAG2TXEVOPAOPB")
	at := func(s string) []byte {
		i := bytes.Index(buf, []byte(s))
		require.GreaterOrEqual(t, i, 0)
		return buf[i : i+len(s)]
	}
	in := ingest.LedgerTransactionView{
		Hash:              [32]byte{1, 2, 3},
		ApplicationOrder:  7,
		FeeBump:           true,
		Successful:        true,
		Envelope:          at("ENVELOPE"),
		Result:            at("RESULT"),
		Meta:              at("META"),
		DiagnosticEvents:  [][]byte{at("DIAG1"), at("DIAG2")},
		TransactionEvents: [][]byte{at("TXEV")},
		ContractEvents:    [][][]byte{{at("OPA")}, nil, {at("OPB"), buf[:0]}},
		LedgerSequence:    99,
		LedgerCloseTime:   1234,
	}
	want := ingest.LedgerTransactionView{
		Hash:              in.Hash,
		ApplicationOrder:  in.ApplicationOrder,
		FeeBump:           in.FeeBump,
		Successful:        in.Successful,
		Envelope:          []byte("ENVELOPE"),
		Result:            []byte("RESULT"),
		Meta:              []byte("META"),
		DiagnosticEvents:  [][]byte{[]byte("DIAG1"), []byte("DIAG2")},
		TransactionEvents: [][]byte{[]byte("TXEV")},
		ContractEvents:    [][][]byte{{[]byte("OPA")}, nil, {[]byte("OPB"), {}}},
		LedgerSequence:    in.LedgerSequence,
		LedgerCloseTime:   in.LedgerCloseTime,
	}

	out := compactView(in)
	// Destroy the source; anything the view still aliased goes with it.
	for i := range buf {
		buf[i] = 0xFF
	}
	assert.Equal(t, want, out)

	// Every slice is capped to its own length, so appending to one cannot reach
	// the next field in the shared backing array.
	for name, b := range map[string][]byte{
		"envelope": out.Envelope, "result": out.Result, "meta": out.Meta,
		"diag0": out.DiagnosticEvents[0], "txev0": out.TransactionEvents[0],
		"op0": out.ContractEvents[0][0],
	} {
		assert.Equalf(t, len(b), cap(b), "%s must not be appendable into its neighbor", name)
	}

	// A view with nothing to copy stays a view with nothing to copy.
	assert.Equal(t, ingest.LedgerTransactionView{}, compactView(ingest.LedgerTransactionView{}))
}

// TestTxReader_ConcurrentBorrowsAreIsolated runs many lookups at once
// through the pool. Each borrows a buffer the previous borrower poisoned, so a
// view that outlived its borrow — or two lookups sharing one buffer — comes back
// as bytes that do not match the ledger. Run under -race this also covers the
// pool handoff itself.
func TestTxReader_ConcurrentBorrowsAreIsolated(t *testing.T) {
	fl := buildLedgers(t, []uint32{100, 200, 300}, 4)
	poisoning := newPoisoningSource(fl.src)
	reader, err := NewTxReader(
		[]HashIndex{fakeIndex{out: fl.byHash}}, nil, poisoning, network.TestNetworkPassphrase)
	require.NoError(t, err)

	hashes := make([][32]byte, 0, len(fl.byHash))
	want := make(map[[32]byte]ingest.LedgerTransactionView, len(fl.byHash))
	for hash := range fl.byHash {
		hashes = append(hashes, hash)
		want[hash] = directView(t, fl, hash)
	}

	const goroutines, rounds = 8, 30
	failures := make(chan error, goroutines)
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Go(func() {
			for i := range rounds {
				hash := hashes[(g+i)%len(hashes)]
				got, found, err := reader.GetTransaction(hash)
				switch {
				case err != nil:
					failures <- fmt.Errorf("hash %x: %w", hash, err)
				case !found:
					failures <- fmt.Errorf("hash %x: not found", hash)
				case !assert.ObjectsAreEqual(want[hash], got):
					failures <- fmt.Errorf("hash %x: view differs under concurrency", hash)
				default:
					continue
				}
				return
			}
		})
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		require.NoError(t, err)
	}
	assert.EqualValues(t, goroutines*rounds, poisoning.loans.Load())
}
