package lifecycle

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"iter"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/backfill"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/hotchunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/metastore"
)

// This file provides the shared test scaffolding the lifecycle tests need. The
// catalog/fixture helpers are copied verbatim from the root fullhistory package's
// helpers_test.go (which still serves the root tests). The hot-tier helpers
// (openHotDBForChunk / openLiveHotDB / NewRocksHotProbe) are
// test-local equivalents of the production hot-source primitives that live in the
// root fullhistory package — the lifecycle package cannot import root (root imports
// lifecycle), so the lifecycle tests rebuild them over the same public store APIs.

// testCPI is the tx-hash index width tests build layouts with; equals the
// production constant so on-disk geometry reads back identically.
const testCPI = geometry.ChunksPerTxhashIndex

func silentLogger() *supportlog.Entry {
	var buf bytes.Buffer
	log := supportlog.New()
	log.SetLevel(logrus.DebugLevel)
	log.SetOutput(&buf)
	return log
}

// newTestCatalog builds a Catalog over a real metastore on temp dirs with
// cpi-wide tx-hash indexes; returns the catalog and artifact root (the store is
// closed via t.Cleanup).
func newTestCatalog(t *testing.T, cpi uint32) (*catalog.Catalog, string) {
	t.Helper()
	metaDir := t.TempDir()
	artifactRoot := t.TempDir()

	store, err := metastore.New(filepath.Join(metaDir, "rocksdb"), silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	idxLayout, err := geometry.NewTxHashIndexLayout(cpi)
	require.NoError(t, err)

	return catalog.NewCatalog(store, geometry.NewLayout(artifactRoot), idxLayout), artifactRoot
}

// testCatalog builds a catalog with the default (wide) tx-hash index, returning it
// and the artifact root.
func testCatalog(t *testing.T) (*catalog.Catalog, string) {
	t.Helper()
	cat, root := newTestCatalog(t, testCPI)
	return cat, root
}

// smallTxHashIndexCatalog builds a test catalog whose indexes are cpi chunks
// wide, so a "terminal" (full-index) build needs only a few chunks. Returns the
// catalog and the artifact root.
func smallTxHashIndexCatalog(t *testing.T, cpi uint32) (*catalog.Catalog, string) {
	t.Helper()
	cat, root := newTestCatalog(t, cpi)
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

// ---------------------------------------------------------------------------
// Hot-tier test scaffolding: test-local equivalents of the root package's
// production hot-source primitives (ingest.go's openHotDBForChunk
// and hotsource.go's rocksHotProbe/NewRocksHotProbe). They use only the public
// hotchunk/ledger/catalog/backfill APIs the production code uses, so a lifecycle
// test reads and freezes the SAME on-disk hot DB the real daemon would, without
// importing the root fullhistory package (which would be an import cycle).
// ---------------------------------------------------------------------------

// openHotDBForChunk creates a "ready" shared hot DB for chunkID under the
// hot:chunk bracket (transient -> create -> ready) and returns an open handle the
// caller owns. The test equivalent of the production opener, trimmed to the
// create branch the lifecycle tests need (no crash-recovery / fsync — those edges
// are covered by the root ingest_test.go opener tests).
func openHotDBForChunk(cat *catalog.Catalog, chunkID chunk.ID, logger *supportlog.Entry) (*hotchunk.DB, error) {
	dir := cat.Layout().HotChunkPath(chunkID)
	if err := os.RemoveAll(dir); err != nil {
		return nil, fmt.Errorf("wipe leftover hot dir %s: %w", dir, err)
	}
	if err := cat.PutHotTransient(chunkID); err != nil {
		return nil, fmt.Errorf("mark hot transient chunk %s: %w", chunkID, err)
	}
	db, err := hotchunk.Open(dir, chunkID, logger)
	if err != nil {
		return nil, fmt.Errorf("create hot DB chunk %s: %w", chunkID, err)
	}
	if err := cat.FlipHotReady(chunkID); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("flip hot ready chunk %s: %w", chunkID, err)
	}
	return db, nil
}

// openLiveHotDB opens (and brackets ready) the live hot DB for a chunk via the
// test opener, returning the handle.
func openLiveHotDB(t *testing.T, cat *catalog.Catalog, c chunk.ID) *hotchunk.DB {
	t.Helper()
	db, err := openHotDBForChunk(cat, c, silentLogger())
	require.NoError(t, err)
	return db
}

// NewRocksHotProbe returns a test backfill.HotProbe over real per-chunk hot DBs —
// the test equivalent of the production probe. It opens the chunk's shared hot DB
// read-only and answers MaxCommittedSeq / Source / Close over it.
func NewRocksHotProbe(hotChunkPath func(chunk.ID) string, logger *supportlog.Entry) backfill.HotProbe {
	return &rocksHotProbe{hotRoot: hotChunkPath, logger: logger}
}

type rocksHotProbe struct {
	hotRoot func(chunkID chunk.ID) string
	logger  *supportlog.Entry
}

func (p *rocksHotProbe) OpenHotChunk(chunkID chunk.ID) (backfill.HotChunk, bool, error) {
	dir := p.hotRoot(chunkID)
	if _, err := os.Stat(dir); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("stat hot dir %s: %w", dir, err)
	}
	db, err := hotchunk.OpenReadOnly(dir, chunkID, p.logger)
	if err != nil {
		return nil, false, fmt.Errorf("open hot chunk DB: %w", err)
	}
	return &rocksHotChunk{chunkID: chunkID, db: db}, true, nil
}

type rocksHotChunk struct {
	chunkID chunk.ID
	db      *hotchunk.DB
}

func (h *rocksHotChunk) MaxCommittedSeq() (uint32, bool, error) {
	seq, ok, err := h.db.MaxCommittedSeq()
	if err != nil {
		return 0, false, fmt.Errorf("hot DB max committed seq: %w", err)
	}
	return seq, ok, nil
}

func (h *rocksHotChunk) Source() ledgerbackend.LedgerStream {
	return &hotLedgerStream{store: h.db.Ledgers()}
}

func (h *rocksHotChunk) Close() error {
	if h.db == nil {
		return nil
	}
	return h.db.Close()
}

// hotLedgerStream is a ledgerbackend.LedgerStream backed by a ledger.HotStore so
// the cold pipeline can freeze a just-closed chunk straight from its hot DB.
type hotLedgerStream struct {
	store *ledger.HotStore
}

var _ ledgerbackend.LedgerStream = (*hotLedgerStream)(nil)

func (st *hotLedgerStream) RawLedgers(
	ctx context.Context, r ledgerbackend.Range, _ ...ledgerbackend.StreamOption,
) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		if st.store == nil {
			yield(nil, errors.New("lifecycle test: hotLedgerStream has no store"))
			return
		}
		to := r.To()
		if !r.Bounded() {
			last, ok, err := st.store.LastSeq()
			if err != nil {
				yield(nil, err)
				return
			}
			if !ok {
				return
			}
			to = last
		}
		for e, ierr := range st.store.IterateLedgers(r.From(), to) {
			if cerr := ctx.Err(); cerr != nil {
				yield(nil, cerr)
				return
			}
			if ierr != nil {
				yield(nil, ierr)
				return
			}
			if !yield(e.Bytes, nil) {
				return
			}
		}
	}
}
