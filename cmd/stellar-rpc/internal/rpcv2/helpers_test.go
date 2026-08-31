package rpcv2

import (
	"bytes"
	"context"
	"iter"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/backfill"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/observability"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rpcv2test"
)

// nopServeReads is the no-op ServeReads: it serves nothing and returns when
// its context cancels, like the production serve's clean path.
func nopServeReads(ctx context.Context, _ *query.Registry, _ net.Listener) error {
	<-ctx.Done()
	return ctx.Err()
}

// countingServeReads is a no-op ServeReads that counts its invocations — for
// tests that only care that serving started.
func countingServeReads(served *atomic.Int32) func(context.Context, *query.Registry, net.Listener) error {
	return func(ctx context.Context, _ *query.Registry, _ net.Listener) error {
		served.Add(1)
		<-ctx.Done()
		return ctx.Err()
	}
}

// signalingServeReads is countingServeReads' channel twin, for tests that
// sequence on the moment serving starts.
func signalingServeReads(servedCh chan struct{}) func(context.Context, *query.Registry, net.Listener) error {
	return func(ctx context.Context, _ *query.Registry, _ net.Listener) error {
		servedCh <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}
}

// testCPI is the tx-hash index width tests build layouts with; equals the
// production constant so on-disk geometry reads back identically.
const testCPI = geometry.ChunksPerTxhashIndex

// preGenesisLedger is the "nothing committed, ingest from genesis" base
// (FirstLedgerSeq-1) tests assert against. Production derives it inline in
// lastCommittedLedger; it lives here only as a readable name for test expectations.
const preGenesisLedger = uint32(chunk.FirstLedgerSeq - 1)

func silentLogger() *supportlog.Entry {
	return rpcv2test.SilentLogger()
}

// capturingLogger is silentLogger plus access to what was logged.
func capturingLogger() (*supportlog.Entry, *bytes.Buffer) {
	buf := &bytes.Buffer{}
	log := supportlog.New()
	log.SetLevel(logrus.DebugLevel)
	log.SetOutput(buf)
	return log, buf
}

// testCatalog builds a catalog with the default (wide) tx-hash index, returning it
// and the artifact root.
func testCatalog(t *testing.T) (*catalog.Catalog, string) {
	t.Helper()
	return rpcv2test.OpenTestCatalog(t, testCPI)
}

// freezeKinds flips the given per-chunk kinds to "frozen" via the one-write protocol.
func freezeKinds(t *testing.T, cat *catalog.Catalog, chunkID chunk.ID, kinds ...geometry.Kind) {
	t.Helper()
	require.NoError(t, cat.MarkChunkFreezing(chunkID, kinds...))
	require.NoError(t, cat.FlipChunkFrozen(chunkID, kinds...))
}

// freezeCoverage marks and commits a frozen index coverage [lo, hi] for index w.
func freezeCoverage(t *testing.T, cat *catalog.Catalog, w geometry.TxHashIndexID, lo, hi chunk.ID) {
	t.Helper()
	cov, err := cat.MarkTxHashIndexFreezing(w, lo, hi)
	require.NoError(t, err)
	require.NoError(t, cat.CommitTxHashIndex(cov))
}

// recordingMetrics is a concurrency-safe Metrics sink for backfill tests: counts
// BackfillPass calls and how many times the last-committed gauge was set, no-ops the rest.
type recordingMetrics struct {
	mu                sync.Mutex
	backfillPasses    int
	gaugesSet         map[string]int // how many times each gauge group was set
	lastCommittedVals []uint32       // every value the last-committed gauge was set to, in order
}

func newRecordingMetrics() *recordingMetrics {
	return &recordingMetrics{gaugesSet: map[string]int{}}
}

func (r *recordingMetrics) LastCommitted(v uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gaugesSet["last_committed"]++
	r.lastCommittedVals = append(r.lastCommittedVals, v)
}

func (r *recordingMetrics) RetentionFloor(uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gaugesSet["retention_floor"]++
}

func (r *recordingMetrics) BackfillPass(time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.backfillPasses++
}

func (*recordingMetrics) ChunkBoundary()             {}
func (*recordingMetrics) Freeze(time.Duration)       {}
func (*recordingMetrics) Rebuild(time.Duration)      {}
func (*recordingMetrics) Prune(int, time.Duration)   {}
func (*recordingMetrics) LiveHotChunks(int)          {}
func (*recordingMetrics) Discard(int, time.Duration) {}
func (*recordingMetrics) FailedDestroy()             {}
func (*recordingMetrics) TxIndexInconsistency()      {}
func (*recordingMetrics) TruncatedEventWindow()      {}

// lastCommittedSeq returns the values the last-committed gauge was set to, in order.
func (r *recordingMetrics) lastCommittedSeq() []uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]uint32(nil), r.lastCommittedVals...)
}

var _ observability.Metrics = (*recordingMetrics)(nil)

// ---------------------------------------------------------------------------
// LCM fixtures + a fake backfill.Backend for the daemon E2E test.
// ---------------------------------------------------------------------------

// fullChunkStream is an in-memory ledgerbackend.LedgerStream yielding every
// ledger in [from, to] from a per-seq LCM generator.
type fullChunkStream struct {
	t   *testing.T
	gen func(*testing.T, uint32) []byte
}

var _ ledgerbackend.LedgerStream = (*fullChunkStream)(nil)

func (s *fullChunkStream) RawLedgers(
	_ context.Context, r ledgerbackend.Range, _ ...ledgerbackend.StreamOption,
) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		for seq := r.From(); seq <= r.To(); seq++ {
			if !yield(s.gen(s.t, seq), nil) {
				return
			}
		}
	}
}

// fakeBackend is the rpcv2-package fake backfill.Backend: an in-memory
// LedgerStream (fullChunkStream) plus a programmable frontier Tip.
type fakeBackend struct {
	ledgerbackend.LedgerStream

	tip    uint32
	tipErr error // non-nil ⇒ Tip fails (an unreachable bulk source)
}

var _ backfill.Backend = (*fakeBackend)(nil)

func (b *fakeBackend) Tip(context.Context) (uint32, error) {
	return b.tip, b.tipErr
}
