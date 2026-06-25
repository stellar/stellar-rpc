package fullhistory

import (
	"bytes"
	"context"
	"iter"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/observability"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/metastore"
)

// testCPI is the tx-hash index width tests build layouts with; equals the
// production constant so on-disk geometry reads back identically.
const testCPI = uint32(geometry.ChunksPerTxhashIndex)

func silentLogger() *supportlog.Entry {
	var buf bytes.Buffer
	log := supportlog.New()
	log.SetLevel(logrus.DebugLevel)
	log.SetOutput(&buf)
	return log
}

// newTestCatalog builds a Catalog over a real metastore on temp dirs with
// cpi-wide tx-hash indexes; returns the catalog, open store, and artifact root.
func newTestCatalog(t *testing.T, cpi uint32) (*catalog.Catalog, *metastore.Store, string) {
	t.Helper()
	metaDir := t.TempDir()
	artifactRoot := t.TempDir()

	store, err := metastore.New(filepath.Join(metaDir, "rocksdb"), silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	idxLayout, err := geometry.NewTxHashIndexLayout(cpi)
	require.NoError(t, err)

	return catalog.NewCatalog(store, geometry.NewLayout(artifactRoot), idxLayout), store, artifactRoot
}

// testCatalog builds a catalog with the default (wide) tx-hash index, returning it
// and the artifact root.
func testCatalog(t *testing.T) (*catalog.Catalog, string) {
	t.Helper()
	cat, _, root := newTestCatalog(t, testCPI)
	return cat, root
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

// recordingMetrics is a concurrency-safe Metrics sink for catch-up tests: records
// CatchupPass ranges and counts which gauges were set, no-ops the rest.
type recordingMetrics struct {
	mu          sync.Mutex
	catchupPass []passRec
	gaugesSet   map[string]int // how many times each gauge was set
}

type passRec struct{ lo, hi uint32 }

func newRecordingMetrics() *recordingMetrics {
	return &recordingMetrics{gaugesSet: map[string]int{}}
}

func (r *recordingMetrics) IngestionLag(uint32, uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gaugesSet["lag"]++
}

func (r *recordingMetrics) CatchupProgress(uint32, uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gaugesSet["catchup_progress"]++
}

func (r *recordingMetrics) CatchupPass(lo, hi uint32, _ time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.catchupPass = append(r.catchupPass, passRec{lo, hi})
}

func (*recordingMetrics) LastCommitted(uint32)           {}
func (*recordingMetrics) Watermark(uint32, uint32)       {}
func (*recordingMetrics) ColdTierBytes(int64)            {}
func (*recordingMetrics) ChunkBoundary(uint32)           {}
func (*recordingMetrics) Freeze(int, int, time.Duration) {}
func (*recordingMetrics) Rebuild(int, time.Duration)     {}
func (*recordingMetrics) Prune(int, time.Duration)       {}

var _ observability.Metrics = (*recordingMetrics)(nil)

// ---------------------------------------------------------------------------
// LCM fixtures + fake ChunkSource / BackendWaiter for the daemon E2E test.
// ---------------------------------------------------------------------------

// zeroTxLCMBytes builds wire bytes of a minimal valid zero-tx V2 LedgerCloseMeta;
// zero-tx keeps a full 10k-ledger chunk pass cheap.
func zeroTxLCMBytes(t *testing.T, seq uint32) []byte {
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

// countingChunkSource wraps a stream factory and counts OpenStream calls.
type countingChunkSource struct {
	opens atomic.Int32
	make  func(chunk.ID) (ledgerbackend.LedgerStream, error)
}

func (c *countingChunkSource) OpenStream(id chunk.ID) (ledgerbackend.LedgerStream, error) {
	c.opens.Add(1)
	return c.make(id)
}

// fakeWaiter satisfies backfill.BackendWaiter, returning a fixed result.
type fakeWaiter struct {
	err error
}

func (w *fakeWaiter) WaitForCoverage(context.Context, uint32) error {
	return w.err
}
