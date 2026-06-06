package ingest

import (
	"context"
	"iter"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
)

// testPassphrase is a network passphrase literal used only by the tests; the
// package never hardcodes one.
const testPassphrase = "Public Global Stellar Network ; September 2015"

// fakeStream is an in-memory ledgerbackend.LedgerStream. RawLedgers yields the
// seeded raw bytes for each seq in [r.From(), r.From()+len(seqs)) in order,
// stopping at the first seq it has no bytes for (so a chunk's full
// [First,Last] range need not be seeded — tests seed only a small prefix).
type fakeStream struct {
	raws map[uint32][]byte
}

var _ ledgerbackend.LedgerStream = (*fakeStream)(nil)

func (f *fakeStream) RawLedgers(_ context.Context, r ledgerbackend.Range, _ ...ledgerbackend.StreamOption) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		for seq := r.From(); ; seq++ {
			raw, ok := f.raws[seq]
			if !ok {
				return
			}
			if !yield(raw, nil) {
				return
			}
		}
	}
}

// marshalLCM builds a minimal valid zero-transaction LedgerCloseMeta (V2) for
// the given ledger seq and returns its wire bytes. The shape mirrors the
// known-good fixture in internal/events/ingest_test.go; zero transactions is
// acceptable to events.LCMToPayloads (its tx reader opens it and finds none).
func marshalLCM(t *testing.T, seq uint32) []byte {
	t.Helper()
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
				V1TxSet: &xdr.TransactionSetV1{Phases: nil},
			},
			TxProcessing: nil,
		},
	}
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	return raw
}

func testLogger() *supportlog.Entry {
	l := supportlog.New()
	l.SetLevel(logrus.ErrorLevel)
	return l
}

// seedChunkPrefix seeds n contiguous ledgers starting at chunkID.FirstLedger().
func seedChunkPrefix(t *testing.T, chunkID chunk.ID, n int) *fakeStream {
	t.Helper()
	raws := make(map[uint32][]byte, n)
	first := chunkID.FirstLedger()
	for i := range n {
		seq := first + uint32(i)
		raws[seq] = marshalLCM(t, seq)
	}
	return &fakeStream{raws: raws}
}

func TestRunHot_RoundTrip(t *testing.T) {
	const n = 5
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	stream := seedChunkPrefix(t, chunkID, n)

	hotDir := t.TempDir()
	logger := testLogger()
	cfg := Config{Ledgers: true, Txhash: true, Events: true, Passphrase: testPassphrase}

	require.NoError(t, RunHot(context.Background(), logger, stream, chunkID, hotDir, cfg))

	// Reopen the ledgers hot store and assert each seeded ledger reads back.
	store, err := ledger.OpenHotStore(filepath.Join(hotDir, "ledgers"), logger)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()

	for i := range n {
		seq := first + uint32(i)
		raw, gerr := store.GetLedgerRaw(seq)
		require.NoError(t, gerr)
		require.NotEmpty(t, raw, "ledger %d read back empty", seq)
	}
}

func TestRunCold_RoundTrip(t *testing.T) {
	const n = 5
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	stream := seedChunkPrefix(t, chunkID, n)

	coldDir := t.TempDir()
	logger := testLogger()
	cfg := Config{Ledgers: true}

	factory := func(_ chunk.ID) (ledgerbackend.LedgerStream, error) { return stream, nil }

	require.NoError(t, runColdWithStreamFactory(
		context.Background(), logger, factory, coldDir, chunkID, 1, 1, cfg,
	))

	// Reopen the chunk's cold ledger packfile and read back the first ledger.
	path := packPath(filepath.Join(coldDir, "ledgers"), uint32(chunkID))
	cr, err := ledger.OpenColdReader(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, cr.Close()) }()

	raw, err := cr.GetLedgerRaw(first)
	require.NoError(t, err)
	require.NotEmpty(t, raw)

	// The round-tripped bytes match what we seeded.
	require.Equal(t, stream.raws[first], raw)
}
