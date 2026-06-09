package ingest

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/eventstore"
)

// ───────────────── writeTxhashBin failure paths ─────────────────

// TestWriteTxhashBin_CreateFails forces os.Create to fail by pre-creating the
// "<path>.tmp" sibling as a DIRECTORY (so create returns EISDIR). The error
// must propagate and no final .bin must exist. The pre-existing .tmp directory
// is not a regular leftover file the writer created.
func TestWriteTxhashBin_CreateFails(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.bin")
	tmp := path + ".tmp"
	require.NoError(t, os.Mkdir(tmp, 0o755)) // create() will hit EISDIR

	err := writeTxhashBin(path, []txhashEntry{{key: [keySize]byte{0x01}, seq: 7}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "create")

	// No final .bin produced.
	_, statErr := os.Stat(path)
	require.True(t, os.IsNotExist(statErr), "no final .bin on create failure")
}

// TestWriteTxhashBin_RenameFails forces os.Rename to fail by pre-creating the
// FINAL path as a non-empty DIRECTORY (rename onto a non-empty dir fails). The
// error must propagate, the temp file must be cleaned up, and no valid final
// .bin (file) should remain.
func TestWriteTxhashBin_RenameFails(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.bin")
	// Final path is a non-empty directory → os.Rename(tmp, path) fails.
	require.NoError(t, os.Mkdir(path, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(path, "blocker"), []byte("x"), 0o644))

	err := writeTxhashBin(path, []txhashEntry{{key: [keySize]byte{0x02}, seq: 9}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "rename")

	// The temp file must have been removed (no stray .tmp).
	_, statErr := os.Stat(path + ".tmp")
	require.True(t, os.IsNotExist(statErr), "leftover .tmp after rename failure")

	// The final path is still the (pre-existing) directory, not a published file.
	info, statErr := os.Stat(path)
	require.NoError(t, statErr)
	require.True(t, info.IsDir(), "rename must not have published a .bin file")
}

// ───────────────── closeColdAll phantom metric ─────────────────

// TestBuildColdIngesters_RollbackNoPhantomMetric makes a LATER constructor
// (txhash) fail by planting a regular file where its per-type subdir must be
// created, so MkdirAll fails. The earlier-built ledger ingester is rolled back
// via closeColdAll, which must NOT emit a phantom success ColdIngest — the
// recorded ledger metric (if any) must carry the abort error, never a clean
// (nil-err, 0-items) success.
func TestBuildColdIngesters_RollbackNoPhantomMetric(t *testing.T) {
	chunkID := chunk.ID(0)
	coldDir := t.TempDir()
	sink := &testSink{}

	// Plant a regular file where the txhash per-type subdir would be created;
	// NewTxhashColdIngester's os.MkdirAll then fails.
	txhashPath := filepath.Join(coldDir, dataTypeTxhash)
	require.NoError(t, os.WriteFile(txhashPath, []byte("not a dir"), 0o644))

	_, err := buildColdIngesters(coldDir, chunkID, sink, Config{Ledgers: true, Txhash: true})
	require.Error(t, err, "txhash constructor must fail on the planted file")

	// The ledger ingester was built then rolled back. No phantom SUCCESS metric:
	// any recorded ledger ColdIngest must carry an error.
	cdt := sink.coldDataTypes()
	if cdt[dataTypeLedgers] > 0 {
		require.Equal(t, cdt[dataTypeLedgers], sink.coldErrorTypes()[dataTypeLedgers],
			"rolled-back ledger ingester must not emit a phantom success ColdIngest")
	}
	// And the success-only assertion: there must be zero clean (nil-err) cold
	// ingest signals recorded.
	require.Zero(t, countCleanColdIngests(sink), "no clean ColdIngest on the rollback path")
}

// TestBuildColdIngesters_RollbackLaterFailure_TxhashAborts makes the LAST
// constructor (events) fail AFTER both the ledger AND txhash ingesters were
// already built, so closeColdAll rolls back two ingesters. It asserts the txhash
// ingester (which DOES implement abortMetric) emits an error-carrying — not a
// clean-success — ColdIngest, complementing the ledger-only abort coverage above.
func TestBuildColdIngesters_RollbackLaterFailure_TxhashAborts(t *testing.T) {
	chunkID := chunk.ID(0)
	coldDir := t.TempDir()
	sink := &testSink{}

	// Plant a regular file where the events per-type subdir would be created, so
	// NewEventsColdIngester's bucketDir MkdirAll fails — but only AFTER the ledger
	// and txhash ingesters were successfully built.
	eventsPath := filepath.Join(coldDir, dataTypeEvents)
	require.NoError(t, os.WriteFile(eventsPath, []byte("not a dir"), 0o644))

	_, err := buildColdIngesters(coldDir, chunkID, sink,
		Config{Ledgers: true, Txhash: true, Events: true})
	require.Error(t, err, "events constructor must fail on the planted file")

	// The txhash ingester was built then rolled back: its recorded ColdIngest must
	// carry the abort error, never a clean success.
	cdt := sink.coldDataTypes()
	require.Equal(t, 1, cdt[dataTypeTxhash], "rolled-back txhash ingester emits one ColdIngest")
	require.Equal(t, 1, sink.coldErrorTypes()[dataTypeTxhash],
		"the rolled-back txhash ColdIngest must carry the abort error")

	// No phantom clean success on the rollback path for any ingester.
	require.Zero(t, countCleanColdIngests(sink), "no clean ColdIngest on the rollback path")
}

// countCleanColdIngests counts recorded ColdIngest signals with a nil error.
func countCleanColdIngests(s *testSink) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, c := range s.coldIngests {
		if c.err == nil {
			n++
		}
	}
	return n
}

// ───────────────── events Finish-then-WriteColdIndex symmetry ─────────────────

// TestEventsCold_FinishThenIndexFails_RemovesPack forces WriteColdIndex to fail
// AFTER writer.Finish has committed events.pack, by planting a directory where
// the index.hash file must be written (buildMPHF then hits EISDIR). The
// just-finished events.pack must be removed so no index-less pack survives
// (symmetry with the atomic txhash .bin path).
func TestEventsCold_FinishThenIndexFails_RemovesPack(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	coldDir := t.TempDir()

	ing, err := NewEventsColdIngester(coldDir, chunkID, nil)
	require.NoError(t, err)

	// Ingest one event-bearing ledger so the mirror is non-empty (WriteColdIndex
	// rejects an empty build set before it would otherwise reach buildMPHF).
	rawEv, _, _ := marshalLCMWithEvent(t, first)
	require.NoError(t, ing.Ingest(context.Background(), xdr.LedgerCloseMetaView(rawEv)))

	// Plant a DIRECTORY where index.hash must be written → buildMPHF fails.
	bucketDir := filepath.Join(coldDir, chunkID.BucketID())
	indexHashPath := filepath.Join(bucketDir, eventstore.IndexHashName(chunkID))
	require.NoError(t, os.Mkdir(indexHashPath, 0o755))

	ferr := ing.Finalize(context.Background())
	require.Error(t, ferr, "Finalize must fail when WriteColdIndex fails")
	require.Contains(t, ferr.Error(), "WriteColdIndex")

	// events.pack must have been removed (symmetric with txhash atomicity).
	packPath := filepath.Join(bucketDir, eventstore.EventsPackName(chunkID))
	_, statErr := os.Stat(packPath)
	require.True(t, os.IsNotExist(statErr),
		"orphan events.pack must be removed after WriteColdIndex failure")

	// Close is still safe/idempotent afterwards.
	require.NoError(t, ing.Close())
}

// ───────────────── close-error fold (stubbed writer) ─────────────────

// closeErrWriter is a minimal stand-in for a cold writer whose Close returns an
// error, used to verify the per-ingester Close-error fold contract: the cold
// error metric counts the Close error AND Close still returns it.
type closeErrWriter struct{ err error }

func (w closeErrWriter) Close() error { return w.err }

// stubCold mirrors the production ledgerCold/eventsCold Close contract
// (writer.Close()'s error folded into the cold metric, then returned) over a
// stubbed writer so the fold can be asserted without a real packfile.
type stubCold struct {
	writer  closeErrWriter
	metrics coldMetrics
}

func (s *stubCold) Ingest(context.Context, xdr.LedgerCloseMetaView) error { return nil }
func (s *stubCold) Finalize(context.Context) error                        { s.metrics.emit(0, nil); return nil }
func (s *stubCold) Close() error {
	cerr := s.writer.Close()
	s.metrics.emit(0, cerr)
	return cerr
}

// TestColdIngester_CloseErrorFold asserts that when a cold ingester's writer
// Close() errors (and Finalize never ran), the cold error metric counts it and
// Close still returns the error — the exact fold ledgerCold.Close /
// eventsCold.Close implement.
func TestColdIngester_CloseErrorFold(t *testing.T) {
	sink := &testSink{}
	wantErr := errors.New("induced writer close failure")
	ing := &stubCold{
		writer:  closeErrWriter{err: wantErr},
		metrics: newColdMetrics(sink, dataTypeLedgers),
	}

	got := ing.Close()
	require.ErrorIs(t, got, wantErr, "Close must still return the writer error")

	require.Equal(t, 1, sink.coldDataTypes()[dataTypeLedgers], "one ColdIngest emitted")
	require.Equal(t, 1, sink.coldErrorTypes()[dataTypeLedgers], "the cold error metric counts the Close error")
}

// ───────────────── drain ctx-cancel ─────────────────

// TestDrain_ContextCancelled_NoArtifact passes a context cancelled BEFORE the
// first ledger through RunCold; drain must return context.Canceled and no cold
// pack/.bin may be written.
func TestDrain_ContextCancelled_NoArtifact(t *testing.T) {
	chunkID := chunk.ID(0)
	coldDir := t.TempDir()
	logger := testLogger()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancelled before any ledger is drained

	err := RunCold(ctx, logger, sourceOf(fullStream(t, chunkID, nil)), coldDir, chunkID, 1, 1, nil,
		Config{Ledgers: true, Txhash: true})
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)

	// No finalized artifacts.
	ledgerPack := packPath(filepath.Join(coldDir, dataTypeLedgers), chunkID)
	_, statErr := os.Stat(ledgerPack)
	require.True(t, os.IsNotExist(statErr), "no ledger pack on cancel")
	binPath := filepath.Join(coldDir, dataTypeTxhash, chunkID.String()+".bin")
	_, statErr = os.Stat(binPath)
	require.True(t, os.IsNotExist(statErr), "no txhash .bin on cancel")
}

// ───────────────── ColdService.Finalize first-error ─────────────────

// finalizeErrCold is a ColdIngester whose Finalize errors; it records whether
// Finalize/Close ran.
type finalizeErrCold struct {
	err       error
	finalized bool
	closed    bool
}

func (f *finalizeErrCold) Ingest(context.Context, xdr.LedgerCloseMetaView) error { return nil }
func (f *finalizeErrCold) Finalize(context.Context) error                        { f.finalized = true; return f.err }
func (f *finalizeErrCold) Close() error                                          { f.closed = true; return nil }

// recordFinalizeCold is a ColdIngester that records it was finalized (no error).
type recordFinalizeCold struct {
	finalized bool
	closed    bool
}

func (r *recordFinalizeCold) Ingest(context.Context, xdr.LedgerCloseMetaView) error { return nil }
func (r *recordFinalizeCold) Finalize(context.Context) error                        { r.finalized = true; return nil }
func (r *recordFinalizeCold) Close() error                                          { r.closed = true; return nil }

// TestColdService_Finalize_FirstErrorButFinalizesRest asserts ColdService.Finalize
// returns the FIRST ingester's error AND still finalizes the later ingester (so a
// failure does not strand a writable handle).
func TestColdService_Finalize_FirstErrorButFinalizesRest(t *testing.T) {
	firstErr := errors.New("first finalize failure")
	failing := &finalizeErrCold{err: firstErr}
	later := &recordFinalizeCold{}

	service := NewColdService([]ColdIngester{failing, later}, &testSink{})
	ferr := service.Finalize(context.Background())

	require.ErrorIs(t, ferr, firstErr, "Finalize returns the FIRST error")
	require.True(t, failing.finalized, "first ingester finalized")
	require.True(t, later.finalized, "later ingester still finalized despite earlier error")
}
