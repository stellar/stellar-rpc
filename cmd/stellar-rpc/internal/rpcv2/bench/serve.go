package bench

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/hotchunk"
)

// Serving a prepared dataset over HTTP, for read benchmarks against a fixed
// corpus.
//
// The problem this solves is that a cold artifact tree is not self-describing.
// Routing reads nothing but catalog keys — ReadView.resolveTier serves a chunk
// only when its key says "frozen" (cold) or "ready" with a published handle
// (hot) — and the catalog never lists a directory. So a pack tree with no
// catalog serves nothing, however complete it is on disk. That is the state
// every published dataset is in: `bench-ingest cold` writes its artifacts to
// --cold-out-dir but builds its catalog in a temp dir it deletes on the way out
// (openScratchCatalog), so the artifacts outlive the only record of them.
//
// ADOPTION is the fix: walk the artifacts that exist, then write the keys that
// make them reachable, into a catalog that persists. It is the inverse of the
// one-write protocol — instead of marking then writing a file, it observes a
// file then marks it — and it is sound for the same reason the protocol is: a
// "frozen" key asserts the file is complete and durable, which for an artifact
// some earlier run already fsynced is true.
//
// This never fabricates data. Every key it writes names a file it stat'ed
// first, and a chunk missing its ledger pack fails the command rather than
// being advertised as servable.

// serveOptions configures one bench-serve run.
type serveOptions struct {
	// ColdRoot is the cold artifact root: the tree holding ledgers/, events/
	// and txhash/, laid out by geometry.NewLayout. A published dataset's pack
	// root and `bench-ingest cold`'s --cold-out-dir are both this shape.
	ColdRoot string

	// HotRoot holds the per-chunk hot RocksDB dirs ({chunk:08d}). Empty means
	// no hot tier: cold chunks alone are served.
	HotRoot string

	// CatalogDir is where the adopted catalog is created. Unlike the ingest
	// benchmarks' scratch catalog this one PERSISTS — it is the artifact that
	// makes the dataset servable, so a second run over the same dataset reuses
	// it instead of re-adopting.
	CatalogDir string

	// StartChunk and NumChunks give the cold chunk range to adopt,
	// [StartChunk, StartChunk+NumChunks).
	StartChunk chunk.ID
	NumChunks  int

	// HotChunk is the chunk ID of a pre-built hot DB under HotRoot to serve as
	// the hot tier, or -1 for none. `bench-ingest hot` leaves exactly this: a
	// finished DB whose catalog was thrown away. It is adopted read-write
	// (never through the create bracket, which would wipe it).
	HotChunk int64

	// LatestLedger is the newest ledger reads may serve. Zero derives it from
	// the highest adopted chunk. It must be set for anything to be servable at
	// all: every range is gated against it (ReadView.ClampRange), so a registry
	// left at zero answers nothing.
	LatestLedger uint32

	// Endpoint is the host:port the read server binds.
	Endpoint string

	// NetworkPassphrase is what transaction hashes are computed against. It
	// must match the passphrase the dataset was generated under, or every
	// getTransaction lookup misses.
	NetworkPassphrase string
}

func (o serveOptions) validate() error {
	switch {
	case o.ColdRoot == "":
		return errors.New("--cold-dir is required")
	case o.CatalogDir == "":
		return errors.New("--catalog-dir is required")
	case o.Endpoint == "":
		return errors.New("--endpoint is required")
	case o.NetworkPassphrase == "":
		return errors.New("--network-passphrase is required")
	case o.NumChunks < 1:
		return fmt.Errorf("--num-chunks must be >= 1, got %d", o.NumChunks)
	}
	if end := uint64(o.StartChunk) + uint64(o.NumChunks) - 1; end > uint64(maxChunkID) {
		return fmt.Errorf("--start-chunk=%d with --num-chunks=%d ends at chunk %d, past the last valid chunk ID %d",
			uint32(o.StartChunk), o.NumChunks, end, uint32(maxChunkID))
	}
	if o.HotChunk >= 0 {
		if o.HotRoot == "" {
			return errors.New("--hot-chunk needs --hot-dir")
		}
		if o.HotChunk > int64(maxChunkID) {
			return fmt.Errorf("--hot-chunk=%d is past the last valid chunk ID %d", o.HotChunk, uint32(maxChunkID))
		}
	}
	return nil
}

// lastColdChunk is the highest chunk in the adopted cold range.
func (o serveOptions) lastColdChunk() chunk.ID {
	//nolint:gosec // validate() proved StartChunk+NumChunks-1 <= maxChunkID
	return o.StartChunk + chunk.ID(uint32(o.NumChunks-1))
}

// highestChunk is the highest chunk the run serves from either tier — the
// anchor for the derived latest ledger and the serving frontier.
func (o serveOptions) highestChunk() chunk.ID {
	highest := o.lastColdChunk()
	if o.HotChunk >= 0 && chunk.ID(o.HotChunk) > highest {
		highest = chunk.ID(o.HotChunk)
	}
	return highest
}

// layout binds the cold trees to ColdRoot and the hot tree to HotRoot, so a
// dataset dir and a hot dir on different filesystems can be served together.
// The catalog is its own root: it is derived state, not part of the dataset.
func (o serveOptions) layout() geometry.Layout {
	cold := geometry.NewLayout(o.ColdRoot)
	hotRoot := o.HotRoot
	if hotRoot == "" {
		// No hot tier: point the hot root at a path under the catalog rather
		// than the dataset, so nothing can write into a read-only dataset dir.
		hotRoot = filepath.Join(o.CatalogDir, "hot")
	}
	return geometry.NewLayoutFromRoots(
		filepath.Join(o.CatalogDir, "rocksdb"),
		hotRoot,
		cold.LedgersRoot(),
		cold.EventsRoot(),
		cold.TxHashRawRoot(),
		cold.TxHashIndexRoot(),
	)
}

// runServe adopts the dataset into a persistent catalog, builds a serving
// registry over it, and serves reads until the context is canceled.
func runServe(ctx context.Context, logger *supportlog.Entry, opts serveOptions) error {
	if err := opts.validate(); err != nil {
		return err
	}
	layout := opts.layout()
	if err := os.MkdirAll(opts.CatalogDir, 0o755); err != nil {
		return fmt.Errorf("create --catalog-dir %s: %w", opts.CatalogDir, err)
	}
	txLayout, err := geometry.NewTxHashIndexLayout(geometry.ChunksPerTxhashIndex)
	if err != nil {
		return err
	}
	cat, err := catalog.Open(layout.CatalogPath(), layout, txLayout, logger)
	if err != nil {
		return fmt.Errorf("open catalog at %s: %w", layout.CatalogPath(), err)
	}
	defer func() { _ = cat.Close() }()

	reg, err := buildServingRegistry(cat, logger, opts)
	if err != nil {
		return err
	}
	defer reg.Close()

	return rpcv2.BenchServeReads(ctx, rpcv2.BenchServeConfig{
		Endpoint:          opts.Endpoint,
		NetworkPassphrase: opts.NetworkPassphrase,
		Registry:          reg,
		Logger:            logger,
		RetentionWindow:   0, // full history: nothing is pruned
	})
}

// buildServingRegistry adopts the dataset's artifacts into cat and returns a
// registry serving them. The caller closes the registry.
func buildServingRegistry(
	cat *catalog.Catalog, logger *supportlog.Entry, opts serveOptions,
) (*query.Registry, error) {
	first, last := opts.StartChunk, opts.lastColdChunk()
	if err := adoptColdChunks(cat, logger, first, last); err != nil {
		return nil, err
	}
	if err := adoptTxHashIndexes(cat, logger, first, last); err != nil {
		return nil, err
	}

	// Retention is full history anchored at the first adopted chunk, so the
	// servable window starts exactly where the dataset does.
	reg := query.NewRegistry(cat, geometry.NewRetention(0, first))
	if err := adoptHotChunk(cat, reg, logger, opts); err != nil {
		reg.Close()
		return nil, err
	}
	if err := markServingFrontier(cat, logger, opts.highestChunk()+1); err != nil {
		reg.Close()
		return nil, err
	}

	latest := opts.LatestLedger
	if latest == 0 {
		latest = opts.highestChunk().LastLedger()
	}
	// Close time 0 means "unknown": the adapters fall back to a point read for
	// the tip's timestamp, which is correct here because no ingestion stamped
	// one.
	reg.SetLatestLedger(latest, 0)
	logger.WithField("oldest_ledger", first.FirstLedger()).
		WithField("latest_ledger", latest).
		Info("bench-serve: serving window")
	return reg, nil
}

// adoptColdChunks writes the frozen keys that make each chunk's on-disk cold
// artifacts reachable. Only kinds whose files are all present are marked, so a
// ledgers-only dataset serves ledgers and reports no events rather than
// erroring inside a read.
//
// The tx-hash .bin runs (KindTxHash) are deliberately never adopted: no read
// path opens them — by-hash lookups go through the .idx window indexes — and a
// finished dataset's .bins are demoted at index build time anyway.
func adoptColdChunks(cat *catalog.Catalog, logger *supportlog.Entry, first, last chunk.ID) error {
	for c := first; c <= last; c++ {
		kinds, err := coldKindsPresent(cat.Layout(), c)
		if err != nil {
			return err
		}
		if len(kinds) == 0 {
			return fmt.Errorf("chunk %s has no ledger pack at %s: is --cold-dir the dataset's pack root?",
				c, cat.Layout().LedgerPackPath(c))
		}
		if err := cat.MarkChunkFreezing(c, kinds...); err != nil {
			return fmt.Errorf("mark chunk %s freezing: %w", c, err)
		}
		if err := cat.FlipChunkFrozen(c, kinds...); err != nil {
			return fmt.Errorf("flip chunk %s frozen: %w", c, err)
		}
		logger.WithField("chunk", c.String()).
			WithField("kinds", kindNames(kinds)).
			WithField("ledgers", fmt.Sprintf("%d-%d", c.FirstLedger(), c.LastLedger())).
			Info("bench-serve: adopted cold chunk")
	}
	return nil
}

// coldKindsPresent returns the artifact kinds whose every file exists for chunk
// c. A ledger pack is mandatory — without it the chunk has no ledgers to serve
// and the caller rejects the range — so an empty result means "not a chunk".
func coldKindsPresent(layout geometry.Layout, c chunk.ID) ([]geometry.Kind, error) {
	ok, err := allExist(layout.ArtifactPaths(c, geometry.KindLedgers))
	if err != nil || !ok {
		return nil, err
	}
	kinds := []geometry.Kind{geometry.KindLedgers}
	ok, err = allExist(layout.ArtifactPaths(c, geometry.KindEvents))
	if err != nil {
		return nil, err
	}
	if ok {
		kinds = append(kinds, geometry.KindEvents)
	}
	return kinds, nil
}

// allExist reports whether every path is present. A stat error other than
// not-exist is returned: an unreadable artifact must not be silently treated as
// absent.
func allExist(paths []string) (bool, error) {
	for _, p := range paths {
		switch _, err := os.Stat(p); {
		case err == nil:
		case errors.Is(err, fs.ErrNotExist):
			return false, nil
		default:
			return false, fmt.Errorf("stat artifact %s: %w", p, err)
		}
	}
	return len(paths) > 0, nil
}

func kindNames(kinds []geometry.Kind) string {
	names := make([]string, len(kinds))
	for i, k := range kinds {
		names[i] = string(k)
	}
	return strings.Join(names, ",")
}

// adoptTxHashIndexes registers the on-disk .idx window indexes the adopted
// chunk range needs for by-hash lookups. The coverage a key must carry is the
// one encoded in the FILE NAME ({lo:08d}-{hi:08d}.idx), because that is the
// name the read path composes when it opens the index — so the names on disk
// are the authority, not the chunk range the caller asked for.
//
// This is the one place that lists a directory. Nothing else can: the index's
// coverage is chosen by whichever build produced it, so it is not derivable
// from the chunk range alone.
func adoptTxHashIndexes(cat *catalog.Catalog, logger *supportlog.Entry, first, last chunk.ID) error {
	idxLayout := cat.TxHashIndexLayout()
	firstIdx, lastIdx := idxLayout.TxHashIndexID(first), idxLayout.TxHashIndexID(last)
	adopted := 0
	for id := firstIdx; id <= lastIdx; id++ {
		cov, found, err := widestIndexCoverage(cat.Layout().TxHashIndexDir(id), id)
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		if err := adoptOneTxHashIndex(cat, cov); err != nil {
			return err
		}
		adopted++
		logger.WithField("index", id.String()).
			WithField("coverage", fmt.Sprintf("%s-%s", cov.Lo, cov.Hi)).
			Info("bench-serve: adopted cold tx-hash index")
	}
	if adopted == 0 {
		logger.Warn("bench-serve: no cold tx-hash index found; getTransaction cannot resolve cold hashes")
	}
	return nil
}

func adoptOneTxHashIndex(cat *catalog.Catalog, cov geometry.TxHashIndexCoverage) error {
	marked, err := cat.MarkTxHashIndexFreezing(cov.Index, cov.Lo, cov.Hi)
	if err != nil {
		return fmt.Errorf("mark tx-hash index %s freezing: %w", cov.Index, err)
	}
	if err := cat.CommitTxHashIndex(marked); err != nil {
		return fmt.Errorf("commit tx-hash index %s: %w", cov.Index, err)
	}
	return nil
}

// widestIndexCoverage picks the .idx in dir with the highest upper chunk. An
// index may hold several generations of .idx file, but at most ONE may be
// frozen at a time (the catalog asserts that invariant on every read), and the
// widest is the one a finished build left — so the others are earlier
// generations that a real daemon would have swept.
func widestIndexCoverage(dir string, id geometry.TxHashIndexID) (geometry.TxHashIndexCoverage, bool, error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return geometry.TxHashIndexCoverage{}, false, nil
	}
	if err != nil {
		return geometry.TxHashIndexCoverage{}, false, fmt.Errorf("read tx-hash index dir %s: %w", dir, err)
	}
	var best geometry.TxHashIndexCoverage
	found := false
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		lo, hi, ok := parseIndexFileName(e.Name())
		if !ok {
			continue
		}
		if !found || hi > best.Hi {
			best = geometry.TxHashIndexCoverage{Index: id, Lo: lo, Hi: hi}
			found = true
		}
	}
	return best, found, nil
}

// parseIndexFileName decodes the {lo:08d}-{hi:08d}.idx leaf that
// Layout.TxHashIndexFilePath composes. Anything else in the dir is ignored.
func parseIndexFileName(name string) (lo, hi chunk.ID, ok bool) {
	base, found := strings.CutSuffix(name, ".idx")
	if !found {
		return 0, 0, false
	}
	loStr, hiStr, found := strings.Cut(base, "-")
	if !found {
		return 0, 0, false
	}
	loN, err := geometry.ParsePadded(loStr)
	if err != nil {
		return 0, 0, false
	}
	hiN, err := geometry.ParsePadded(hiStr)
	if err != nil || loN > hiN {
		return 0, 0, false
	}
	return chunk.ID(loN), chunk.ID(hiN), true
}

// adoptHotChunk publishes a pre-built hot DB as the hot tier. It flips the key
// ready and opens must-exist, never through the create bracket: BeginHotCreate
// wipes the dir, which would destroy the very DB being adopted.
func adoptHotChunk(
	cat *catalog.Catalog, reg *query.Registry, logger *supportlog.Entry, opts serveOptions,
) error {
	if opts.HotChunk < 0 {
		return nil
	}
	c := chunk.ID(opts.HotChunk)
	dir := cat.Layout().HotChunkPath(c)
	if _, err := os.Stat(dir); err != nil {
		return fmt.Errorf("stat hot chunk dir %s: %w", dir, err)
	}
	if err := cat.FlipHotReady(c); err != nil {
		return fmt.Errorf("flip hot chunk %s ready: %w", c, err)
	}
	db, err := hotchunk.OpenReadyWrite(geometry.HotReady, dir, c, logger)
	if err != nil {
		return fmt.Errorf("open hot chunk %s at %s: %w", c, dir, err)
	}
	reg.PublishHandle(c, db)
	logger.WithField("chunk", c.String()).
		WithField("ledgers", fmt.Sprintf("%d-%d", c.FirstLedger(), c.LastLedger())).
		Info("bench-serve: adopted hot chunk")
	return nil
}

// markServingFrontier marks one chunk above the served range hot-ready WITHOUT
// a handle, which is what makes a cold-only dataset servable at all.
//
// Read-view acquisition derives its retention anchor from
// Snapshot.LastCompleteChunk, defined as "the highest ready hot chunk minus
// one" because in a running daemon the highest ready chunk is the live,
// still-ingesting one. An empty ready scan is ErrNoReadyHotChunk, and that
// error fails every read view — so a dataset with no hot tier serves nothing
// until some chunk is ready. This marker is that chunk: it stands in for the
// live chunk a daemon would have, putting the frontier exactly one above the
// data and leaving LastCompleteChunk equal to the highest adopted chunk.
//
// It is safe precisely because no handle is published for it: resolveTier needs
// a ready key AND a loaded handle to route hot, so this chunk resolves to no
// serving store. Nothing can read it, and nothing tries — it sits above the
// latest ledger, so the window gate rejects the range first.
func markServingFrontier(cat *catalog.Catalog, logger *supportlog.Entry, frontier chunk.ID) error {
	if err := cat.FlipHotReady(frontier); err != nil {
		return fmt.Errorf("mark serving frontier chunk %s: %w", frontier, err)
	}
	logger.WithField("chunk", frontier.String()).
		Info("bench-serve: marked serving frontier (no handle; stands in for the live chunk)")
	return nil
}
