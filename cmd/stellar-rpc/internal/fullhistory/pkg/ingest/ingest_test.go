package ingest

import (
	"context"
	"iter"
	"os"
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

// fakeStream is an in-memory ledgerbackend.LedgerStream. It yields a synthetic
// LedgerCloseMeta for every seq in [r.From(), r.From()+count) — i.e. count
// ledgers from the start of the requested range. When count covers the whole
// requested range the driver's chunk-completeness check passes; a smaller count
// models a backend that ends early (used by the short-stream negative test).
//
// LCMs are generated lazily so a full 10k-ledger chunk costs no up-front map.
type fakeStream struct {
	t     *testing.T
	count uint32
}

var _ ledgerbackend.LedgerStream = (*fakeStream)(nil)

func (f *fakeStream) RawLedgers(_ context.Context, r ledgerbackend.Range, _ ...ledgerbackend.StreamOption) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		for i := uint32(0); i < f.count; i++ {
			seq := r.From() + i
			if !yield(marshalLCM(f.t, seq), nil) {
				return
			}
		}
	}
}

// fullStream yields exactly the requested chunk's full [First,Last] range.
func fullStream(t *testing.T, chunkID chunk.ID) *fakeStream {
	t.Helper()
	return &fakeStream{t: t, count: chunkID.LastLedger() - chunkID.FirstLedger() + 1}
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

// TestRunHot_AllTypes_ShortStream exercises the ledgers+txhash+events hot
// ingesters together (the parsed-decode + passphrase path) over a short stream.
// Because the stream ends before the chunk's full range, RunHot must return the
// chunk-completeness error — but only after fanning out to all three ingesters
// for the ledgers it did process, so this also covers the events/txhash hot
// write path and the parsed-LCM seq assertion.
func TestRunHot_AllTypes_ShortStream(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	stream := &fakeStream{t: t, count: 4}

	hotDir := t.TempDir()
	logger := testLogger()
	cfg := Config{Ledgers: true, Txhash: true, Events: true, Passphrase: testPassphrase}

	err := RunHot(context.Background(), logger, stream, chunkID, hotDir, cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ended at")

	// The 4 processed ledgers landed in the ledgers hot store before the
	// completeness check fired.
	store, oerr := ledger.OpenHotStore(filepath.Join(hotDir, "ledgers"), logger)
	require.NoError(t, oerr)
	defer func() { require.NoError(t, store.Close()) }()
	for i := uint32(0); i < 4; i++ {
		raw, gerr := store.GetLedgerRaw(first + i)
		require.NoError(t, gerr)
		require.NotEmpty(t, raw)
	}
}

func TestRunCold_RoundTrip(t *testing.T) {
	chunkID := chunk.ID(0)
	first, last := chunkID.FirstLedger(), chunkID.LastLedger()
	stream := fullStream(t, chunkID)

	coldDir := t.TempDir()
	logger := testLogger()
	cfg := Config{Ledgers: true}

	factory := func(_ chunk.ID) (ledgerbackend.LedgerStream, error) { return stream, nil }

	require.NoError(t, runColdWithStreamFactory(
		context.Background(), logger, factory, coldDir, chunkID, 1, 1, cfg,
	))

	// Reopen the chunk's cold ledger packfile and read back the boundary ledgers.
	path := packPath(filepath.Join(coldDir, "ledgers"), chunkID)
	cr, err := ledger.OpenColdReader(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, cr.Close()) }()

	rawFirst, err := cr.GetLedgerRaw(first)
	require.NoError(t, err)
	require.Equal(t, marshalLCM(t, first), rawFirst)

	rawLast, err := cr.GetLedgerRaw(last)
	require.NoError(t, err)
	require.Equal(t, marshalLCM(t, last), rawLast)
}

// TestRunCold_ShortStream_NoArtifact verifies that a stream which ends before
// the chunk's full range makes RunCold return an error AND never produces a
// finalized cold artifact (the completeness check runs before Finalize).
func TestRunCold_ShortStream_NoArtifact(t *testing.T) {
	chunkID := chunk.ID(0)
	coldDir := t.TempDir()
	logger := testLogger()
	cfg := Config{Ledgers: true}

	// Yield only 3 ledgers of a 10k-ledger chunk.
	short := &fakeStream{t: t, count: 3}
	factory := func(_ chunk.ID) (ledgerbackend.LedgerStream, error) { return short, nil }

	err := runColdWithStreamFactory(
		context.Background(), logger, factory, coldDir, chunkID, 1, 1, cfg,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ended at")

	// No finalized cold packfile must exist for the chunk: Close removed the
	// partial file because Finalize never ran.
	path := packPath(filepath.Join(coldDir, "ledgers"), chunkID)
	_, statErr := os.Stat(path)
	require.True(t, os.IsNotExist(statErr), "expected no cold artifact at %s, stat err: %v", path, statErr)
}
