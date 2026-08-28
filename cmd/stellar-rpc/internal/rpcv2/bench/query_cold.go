package bench

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/spf13/cobra"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
)

func newQueryColdCommand() *cobra.Command {
	var (
		qf         = queryFlags{}
		prof       profileFlags
		startChunk uint32
		numChunks  int
		coldDir    string
		evict      bool
	)
	cmd := newBenchCommand("cold",
		"Benchmark cold reads: queries served from a chunk range's frozen artifacts",
		&prof,
		func(ctx context.Context, logger *supportlog.Entry, env runEnv) error {
			plan, err := qf.plan()
			if err != nil {
				return err
			}
			plan.Evict = evict
			env.Extra["pageCacheEviction"] = evictionState(evict)
			return runQueryCold(ctx, logger, coldQueryOptions{
				ColdRoot:   coldDir,
				StartChunk: chunk.ID(startChunk),
				NumChunks:  numChunks,
				Plan:       plan,
				OutDir:     env.OutDir,
			})
		}, &qf)
	fs := cmd.Flags()
	fs.Uint32Var(&startChunk, "start-chunk", 0, "first chunk to query (required)")
	fs.IntVar(&numChunks, "num-chunks", 1, "how many consecutive chunks to query starting at --start-chunk")
	fs.StringVar(&coldDir, "cold-dir", "",
		"root of the frozen artifact tree to query, as bench-ingest cold's --cold-out-dir laid it out (required)")
	fs.BoolVar(&evict, "evict-page-cache", true,
		"drop the cold artifacts from the OS page cache before each leg, so a cold read is really cold "+
			"(Linux only; elsewhere the run records that it did not happen)")
	markRequired(cmd, "start-chunk", "cold-dir")
	return cmd
}

// evictionState is what invocation.json records about page-cache eviction: what
// was asked for, and — since the syscall exists only on Linux — whether this
// platform could honor it. A cold number measured without eviction is a warm
// number, so that distinction has to reach the results.
func evictionState(requested bool) string {
	switch {
	case !requested:
		return "off"
	case evictSupported:
		return "on"
	default:
		return "unsupported-on-this-platform"
	}
}

// coldQueryOptions configures one cold read benchmark run.
type coldQueryOptions struct {
	// ColdRoot is the layout root of the frozen artifacts to query — the tree
	// a bench-ingest cold run (or a campaign's golden pack download) produced.
	// It is read-only apart from the run's scratch catalog, created and removed
	// under it.
	ColdRoot string

	// StartChunk and NumChunks give the chunk range to query,
	// [StartChunk, StartChunk+NumChunks). Every chunk in it must be materialized
	// under ColdRoot.
	StartChunk chunk.ID
	NumChunks  int

	// Plan is the validated --types × --target-rps ladder.
	Plan queryPlan

	// OutDir receives the CSV report.
	OutDir string
}

// validate checks the flags and chunk range before runQueryCold touches the
// filesystem.
func (o coldQueryOptions) validate() error {
	if o.ColdRoot == "" {
		return errors.New("--cold-dir is required")
	}
	if o.NumChunks < 1 {
		return fmt.Errorf("--num-chunks must be >= 1, got %d", o.NumChunks)
	}
	// The frontier hot key sits one chunk above the range (see openColdFixture),
	// so the range must end below the last valid chunk ID, not at it. uint64 so
	// the sum cannot wrap before the compare.
	if end := uint64(o.StartChunk) + uint64(o.NumChunks) - 1; end >= uint64(maxChunkID) {
		return fmt.Errorf("--start-chunk=%d with --num-chunks=%d ends at chunk %d, at or past the last valid chunk ID %d",
			uint32(o.StartChunk), o.NumChunks, end, uint32(maxChunkID))
	}
	return nil
}

// runQueryCold benchmarks the cold read path: queries against the frozen
// artifacts under --cold-dir, routed through a read view exactly as a served
// request is.
func runQueryCold(ctx context.Context, logger *supportlog.Entry, opts coldQueryOptions) error {
	if err := opts.validate(); err != nil {
		return err
	}
	return runQueryBench(ctx, logger, opts.Plan, opts.OutDir, func() (*queryFixture, func(), error) {
		return openColdFixture(logger, opts)
	})
}

// openColdFixture makes an on-disk frozen artifact tree servable and returns the
// read fixture over it, plus the release that tears the fixture down.
//
// bench-ingest cold writes its artifacts under a SCRATCH catalog it then throws
// away, so the tree arrives with the files but no catalog naming them. This
// rebuilds the catalog state the tree implies, reproducing the deployment those
// artifacts came from:
//
//   - every chunk in the range runs the freeze bracket (MarkChunkFreezing then
//     FlipChunkFrozen) for each kind whose files are actually on disk, so a kind
//     the backfill did not produce — or whose .bin a terminal index build already
//     swept — is absent rather than a frozen key pointing at nothing;
//   - the tx-hash window index on disk is committed under its own bracket, read
//     back from the .idx filename so the catalog names the coverage the by-hash
//     lookup will probe;
//   - the chunk one past the range gets a "ready" hot key. That key is what makes
//     the range complete: LastCompleteChunk is the lowest ready hot chunk minus
//     one, and NewReadView fails outright without a ready hot chunk. It carries
//     no published handle, so routing resolves it to no tier and no query can
//     reach it — it is the frontier a cold range always sits below, nothing more.
//
// Retention is full-history from the range's first chunk, so the whole range
// stays above the view's floor, and the latest ledger is the range's last.
func openColdFixture(logger *supportlog.Entry, opts coldQueryOptions) (*queryFixture, func(), error) {
	layout := geometry.NewLayout(opts.ColdRoot)
	cat, releaseCat, err := openScratchCatalog(opts.ColdRoot, scratchPrefixQuery, layout, logger)
	if err != nil {
		return nil, nil, err
	}
	release := releaseCat

	chunks := chunkRange(opts.StartChunk, opts.NumChunks)
	end := chunks[len(chunks)-1]
	if err := freezeChunks(cat, layout, chunks); err != nil {
		release()
		return nil, nil, err
	}
	if err := commitDiskTxHashIndex(logger, cat, layout, opts.StartChunk, end); err != nil {
		release()
		return nil, nil, err
	}
	// The frontier: a ready hot chunk above the range, so the range counts as
	// complete. No dir is created and no handle published — see the doc comment.
	if err := cat.FlipHotReady(end + 1); err != nil {
		release()
		return nil, nil, fmt.Errorf("mark frontier hot chunk %s ready: %w", end+1, err)
	}

	registry := query.NewRegistry(cat, geometry.NewRetention(0, opts.StartChunk))
	registry.SetLatestLedger(end.LastLedger(), 0)
	f := &queryFixture{
		registry:    registry,
		Passphrase:  opts.Plan.Passphrase,
		Chunks:      chunks,
		FirstLedger: opts.StartChunk.FirstLedger(),
		LastLedger:  end.LastLedger(),
		EvictPaths:  coldArtifactPaths(cat, layout, chunks),
	}
	if err := f.verifyServes(); err != nil {
		release()
		return nil, nil, err
	}
	return f, release, nil
}

// coldArtifactPaths lists every file the benchmarked chunks are served from —
// each chunk's artifacts plus the frozen tx-hash window indexes — so a cold leg
// can drop them from the page cache. The list comes from the catalog, so it
// names exactly what routing will open.
func coldArtifactPaths(cat *catalog.Catalog, layout geometry.Layout, chunks []chunk.ID) []string {
	var paths []string
	for _, c := range chunks {
		for _, kind := range geometry.AllKinds() {
			state, err := cat.State(c, kind)
			if err != nil || state != geometry.StateFrozen {
				continue
			}
			paths = append(paths, layout.ArtifactPaths(c, kind)...)
		}
	}
	covs, err := cat.AllTxHashIndexKeys()
	if err != nil {
		return paths
	}
	for _, cov := range covs {
		if cov.State == geometry.StateFrozen {
			paths = append(paths, layout.TxHashIndexFilePath(cov))
		}
	}
	return paths
}

// freezeChunks runs the freeze bracket over each chunk for the artifact kinds
// materialized under the layout, and reports which kinds each chunk got. A
// chunk whose ledger pack is missing fails the open: without it no query type
// can run, and a later per-query ErrUnavailable would be far harder to read.
func freezeChunks(cat *catalog.Catalog, layout geometry.Layout, chunks []chunk.ID) error {
	for _, c := range chunks {
		var present []geometry.Kind
		for _, kind := range geometry.AllKinds() {
			if artifactOnDisk(layout, c, kind) {
				present = append(present, kind)
			}
		}
		if !slices.Contains(present, geometry.KindLedgers) {
			return fmt.Errorf("chunk %s has no ledger pack under the layout (%s): the dataset does not cover it",
				c, layout.LedgerPackPath(c))
		}
		if err := cat.MarkChunkFreezing(c, present...); err != nil {
			return fmt.Errorf("mark chunk %s freezing: %w", c, err)
		}
		if err := cat.FlipChunkFrozen(c, present...); err != nil {
			return fmt.Errorf("flip chunk %s frozen: %w", c, err)
		}
		cat.Logger().Infof("chunk %s frozen for kinds %s", c, kindList(present))
	}
	return nil
}

// artifactOnDisk reports whether every file a (chunk, kind) artifact owns
// exists, which is what a "frozen" key promises.
func artifactOnDisk(layout geometry.Layout, c chunk.ID, kind geometry.Kind) bool {
	paths := layout.ArtifactPaths(c, kind)
	if len(paths) == 0 {
		return false
	}
	for _, p := range paths {
		if _, err := os.Stat(p); err != nil {
			return false
		}
	}
	return true
}

// kindList renders an artifact-kind set for a log line.
func kindList(kinds []geometry.Kind) string {
	names := make([]string, len(kinds))
	for i, k := range kinds {
		names[i] = string(k)
	}
	return strings.Join(names, ",")
}

// commitDiskTxHashIndex commits the tx-hash window index covering [lo, hi] under
// its freeze bracket, so ReadView.ColdTxHashIndexCoverages returns the coverage
// the by-hash lookup opens. The commit runs AFTER the chunks are frozen, as the
// production order does: a terminal coverage demotes the per-chunk .bin keys it
// supersedes.
//
// An absent index is a real state — a shallow dataset's backfill builds none —
// so the open succeeds, the txhash leg reports an empty probe set, and the
// three types that do not need the index run as usual.
func commitDiskTxHashIndex(
	logger *supportlog.Entry, cat *catalog.Catalog, layout geometry.Layout, lo, hi chunk.ID,
) error {
	txLayout := cat.TxHashIndexLayout()
	cov, ok, err := diskTxHashCoverage(layout, txLayout, lo, hi)
	if err != nil {
		return err
	}
	if !ok {
		logger.Warnf("no tx-hash window index on disk covers chunks [%s, %s]: cold by-hash lookups have nothing to probe",
			lo, hi)
		return nil
	}
	marked, err := cat.MarkTxHashIndexFreezing(cov.Index, cov.Lo, cov.Hi)
	if err != nil {
		return fmt.Errorf("mark tx-hash index %s freezing: %w", cov.Key, err)
	}
	if err := cat.CommitTxHashIndex(marked); err != nil {
		return fmt.Errorf("commit tx-hash index %s: %w", marked.Key, err)
	}
	logger.Infof("tx-hash index %s covers chunks [%s, %s]", cov.Index, cov.Lo, cov.Hi)
	return nil
}

// diskTxHashCoverage reads back the widest window-index coverage on disk that
// spans [lo, hi]. The .idx files are named {lo:08d}-{hi:08d}.idx under their
// index's dir, so the coverage comes from the filename rather than being
// assumed: a backfill's index covers the whole range it ingested, which is
// usually wider than the chunks one query leg reads.
//
// Only the index containing lo is considered. A range straddling two window
// indexes would need both, and no such range is benchmarked — the runner queries
// one chunk at a time.
func diskTxHashCoverage(
	layout geometry.Layout, txLayout geometry.TxHashIndexLayout, lo, hi chunk.ID,
) (geometry.TxHashIndexCoverage, bool, error) {
	idx := txLayout.TxHashIndexID(lo)
	if txLayout.TxHashIndexID(hi) != idx {
		return geometry.TxHashIndexCoverage{}, false,
			fmt.Errorf("chunks [%s, %s] straddle two tx-hash window indexes; query one index's chunks at a time", lo, hi)
	}
	dir := layout.TxHashIndexDir(idx)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return geometry.TxHashIndexCoverage{}, false, nil
		}
		return geometry.TxHashIndexCoverage{}, false, fmt.Errorf("read tx-hash index dir %s: %w", dir, err)
	}
	var best geometry.TxHashIndexCoverage
	found := false
	for _, e := range entries {
		covLo, covHi, ok := parseIndexFileName(e.Name())
		if !ok || covLo > lo || covHi < hi {
			continue
		}
		if !found || covHi > best.Hi {
			best = geometry.TxHashIndexCoverage{
				Index: idx, Lo: covLo, Hi: covHi,
				Key:   geometry.TxHashIndexKey(idx, covLo, covHi),
				State: geometry.StateFrozen,
			}
			found = true
		}
	}
	return best, found, nil
}

// parseIndexFileName decodes a window index's {lo:08d}-{hi:08d}.idx basename —
// the reverse of geometry.Layout.TxHashIndexFilePath. ok is false for anything
// else in the dir (a partial write, a stray file).
func parseIndexFileName(name string) (chunk.ID, chunk.ID, bool) {
	stem, isIdx := strings.CutSuffix(filepath.Base(name), ".idx")
	if !isIdx {
		return 0, 0, false
	}
	loStr, hiStr, split := strings.Cut(stem, "-")
	if !split {
		return 0, 0, false
	}
	lo, err := geometry.ParsePadded(loStr)
	if err != nil {
		return 0, 0, false
	}
	hi, err := geometry.ParsePadded(hiStr)
	if err != nil || hi < lo {
		return 0, 0, false
	}
	return chunk.ID(lo), chunk.ID(hi), true
}
