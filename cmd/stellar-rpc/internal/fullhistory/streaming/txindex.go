package streaming

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/stellar/streamhash"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/streaming/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/streaming/geometry"
)

// IndexBuild names one tx-hash index rebuild: the window and the coverage
// [Lo, Hi] to materialize. Terminal-ness (Hi == window's last chunk) is
// DERIVED at build time (TxHashIndexLayout.IsTerminalCoverage), never carried as a field
// — the spec's "marked nowhere". It mirrors the resolver's plan value
// (design-docs/full-history-streaming-workflow.md "Postcondition-driven
// scheduling").
type IndexBuild struct {
	Index  geometry.TxHashIndexID
	Lo, Hi chunk.ID
}

// BuildConfig is the dependency bundle buildTxhashIndex/buildThenSweep read: the
// catalog (key state, path layout, window arithmetic, CommitTxHashIndex + the sweeps)
// and a logger.
type BuildConfig struct {
	Catalog *catalog.Catalog
	Logger  *supportlog.Entry

	// BuildOpts are extra streamhash.BuildOptions (e.g. WithWorkers) threaded
	// into txhash.BuildColdIndex. Optional; the cold payload/fingerprint/metadata
	// format options are pinned by BuildColdIndex and cannot be overridden here
	// (cold_index.go's "format options go last").
	BuildOpts []streamhash.BuildOption
}

func (cfg BuildConfig) validate() error {
	if cfg.Catalog == nil {
		return errors.New("streaming: BuildConfig.Catalog is nil")
	}
	if cfg.Logger == nil {
		return errors.New("streaming: BuildConfig.Logger is nil")
	}
	return nil
}

// buildTxhashIndex is the tx-hash rolling rebuild (design-docs rule 3 /
// gettransaction-full-history-design.md §7.2): it rebuilds window w's index at
// coverage [lo, hi] from scratch via the one-write protocol with CommitTxHashIndex's
// batch-commit extension, in four steps (each marked inline below):
//
//  1. Skip check — w's unique "frozen" coverage already equals [lo, hi].
//  2. Precondition + mark — require every chunk's .bin frozen, then mark.
//  3. Write — k-way merge the .bin files into the .idx, then fsync.
//  4. Commit — CommitTxHashIndex's one atomic synced batch.
//
// buildTxhashIndex never deletes a file: removal is exclusively the sweeps' job
// (buildThenSweep / the tick's prune scan). The ordering covers the crash matrix
// (§7.6): a crash before step 4 leaves the predecessor frozen and the new
// coverage as "freezing" debris; after, the new coverage is frozen and the
// demoted keys are "pruning" sweep work.
func buildTxhashIndex(ctx context.Context, w geometry.TxHashIndexID, lo, hi chunk.ID, cfg BuildConfig) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	if lo > hi {
		return fmt.Errorf("streaming: buildTxhashIndex window %s lo %s > hi %s", w, lo, hi)
	}
	cat := cfg.Catalog
	layout := cat.Layout()

	// Step 1 — skip check. Checked FIRST so a re-scheduled build of a finalized
	// window (whose .bin inputs the terminal sweep has since deleted) never
	// reaches the precondition below; leftover transient keys are the sweeps' job.
	frozen, hasFrozen, err := cat.FrozenTxHashIndex(w)
	if err != nil {
		return fmt.Errorf("streaming: buildTxhashIndex read frozen coverage window %s: %w", w, err)
	}
	if hasFrozen && frozen.Lo == lo && frozen.Hi == hi {
		cfg.Logger.Debugf("buildTxhashIndex: window %s coverage [%s,%s] already frozen; skipping", w, lo, hi)
		return nil
	}

	// Step 2a — loud precondition, BEFORE any key is touched: the executor's
	// done-channels broadcast completion, not success, so this is the backstop
	// that every chunk's .bin is actually frozen.
	inputs, err := txhashBinInputs(cat, w, lo, hi)
	if err != nil {
		return err
	}

	// Step 2b — mark the coverage "freezing" (idempotent overwrite of debris).
	cov, err := cat.MarkTxHashIndexFreezing(w, lo, hi)
	if err != nil {
		return fmt.Errorf("streaming: buildTxhashIndex mark freezing %s: %w", geometry.TxHashIndexKey(w, lo, hi), err)
	}

	// Step 3 — write the .idx from scratch. txhash.BuildColdIndex create-or-
	// truncates outputPath, so a crashed attempt's partial is overwritten
	// wholesale. The window dir is created on demand; detect whether THIS build
	// created it so BarrierNewFile fsyncs the grandparent (txhash/index/) too.
	idxPath := layout.TxHashIndexFilePath(cov)
	windowDir := layout.TxHashIndexDir(w)
	_, statErr := os.Stat(windowDir)
	newWindowDir := errors.Is(statErr, os.ErrNotExist)
	if statErr != nil && !newWindowDir {
		return fmt.Errorf("streaming: buildTxhashIndex stat window dir %s: %w", windowDir, statErr)
	}
	if newWindowDir {
		if mkErr := os.MkdirAll(windowDir, 0o755); mkErr != nil {
			return fmt.Errorf("streaming: buildTxhashIndex mkdir %s: %w", windowDir, mkErr)
		}
	}

	minLedger := lo.FirstLedger()
	maxLedger := hi.LastLedger()
	if berr := txhash.BuildColdIndex(ctx, inputs, idxPath, minLedger, maxLedger, cfg.BuildOpts...); berr != nil {
		return fmt.Errorf("streaming: buildTxhashIndex build window %s coverage [%s,%s]: %w", w, lo, hi, berr)
	}

	// Durability barrier: fsync the .idx + its dir (+ the grandparent on a new
	// window dir) BEFORE the coverage flips to "frozen" in CommitTxHashIndex.
	if barErr := geometry.BarrierNewFile(idxPath, newWindowDir); barErr != nil {
		return fmt.Errorf("streaming: buildTxhashIndex fsync barrier %s: %w", idxPath, barErr)
	}

	// Step 4 — commit. CommitTxHashIndex re-derives the predecessor and terminal-ness
	// from durable state, so it is safe to re-run after a crash.
	if cerr := cat.CommitTxHashIndex(cov); cerr != nil {
		return fmt.Errorf("streaming: buildTxhashIndex commit window %s coverage [%s,%s]: %w", w, lo, hi, cerr)
	}
	return nil
}

// buildThenSweep is how the executor runs an IndexBuild (design-docs rule 4's
// eager call site / §7.4): buildTxhashIndex, then the sweeps for THIS window's
// "pruning" coverages and (terminal) demoted .bin inputs. The commit batch only
// demotes keys; the eager sweep frees their disk without waiting for a tick.
//
// The sweep is WINDOW-LOCAL (only b.Index's index keys and chunk:{c}:txhash
// keys), so concurrent windows' sweeps touch disjoint keys and files (the
// executor holds at most one IndexBuild per window) and any "pruning" leftover
// from a previous crashed pass is also finished. A crash mid-sweep leaves
// "pruning" keys the next build (or the prune scan) re-runs — same convergence
// regardless of caller.
func buildThenSweep(ctx context.Context, b IndexBuild, cfg BuildConfig) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	cat := cfg.Catalog

	if err := buildTxhashIndex(ctx, b.Index, b.Lo, b.Hi, cfg); err != nil {
		return err
	}

	// Sweep this window's superseded coverages: the just-frozen coverage is
	// skipped, a "pruning" predecessor (from this commit or a crashed pass) removed.
	covs, err := cat.TxHashIndexKeys(b.Index)
	if err != nil {
		return fmt.Errorf("streaming: buildThenSweep read index keys window %s: %w", b.Index, err)
	}
	for _, cov := range covs {
		if cov.State != geometry.StatePruning {
			continue
		}
		if serr := cat.SweepTxHashIndexKey(cov); serr != nil {
			return fmt.Errorf("streaming: buildThenSweep sweep coverage %s: %w", cov.Key, serr)
		}
	}

	// Sweep this window's demoted .bin inputs (terminal build) in one batched
	// pass. A non-terminal build demotes none, so this is a no-op.
	demoted, err := windowDemotedTxhashRefs(cat, b.Index)
	if err != nil {
		return err
	}
	if serr := cat.SweepChunkArtifacts(demoted); serr != nil {
		return fmt.Errorf("streaming: buildThenSweep sweep demoted inputs window %s: %w", b.Index, serr)
	}
	return nil
}

// txhashBinInputs returns the .bin paths for chunks [lo, hi], enforcing rule
// 3's loud precondition: every chunk's chunk:{c}:txhash key MUST be "frozen"
// (its .bin durable, trusted blindly). It errors on the first offending chunk
// and returns no partial inputs — checked before any write in buildTxhashIndex.
func txhashBinInputs(cat *catalog.Catalog, w geometry.TxHashIndexID, lo, hi chunk.ID) ([]string, error) {
	layout := cat.Layout()
	inputs := make([]string, 0, uint32(hi)-uint32(lo)+1)
	for cid := lo; ; cid++ {
		state, err := cat.State(cid, geometry.KindTxHash)
		if err != nil {
			return nil, fmt.Errorf("streaming: buildTxhashIndex read txhash state chunk %s: %w", cid, err)
		}
		if state != geometry.StateFrozen {
			return nil, fmt.Errorf(
				"streaming: buildTxhashIndex precondition violated: window %s chunk %s txhash is %q, want %q",
				w, cid, state, geometry.StateFrozen)
		}
		inputs = append(inputs, layout.TxHashBinPath(cid))
		if cid == hi { // guard against chunk.ID wraparound at the top of the range
			break
		}
	}
	return inputs, nil
}

// windowDemotedTxhashRefs returns the chunk:{c}:txhash refs in window w whose
// key is "pruning" — the terminal commit's demoted .bin inputs (and any a
// previous crashed pass left). The window-local scan walks [firstChunk,
// lastChunk]; a non-terminal build leaves none.
func windowDemotedTxhashRefs(cat *catalog.Catalog, w geometry.TxHashIndexID) ([]catalog.ArtifactRef, error) {
	txLayout := cat.TxHashIndexLayout()
	first := txLayout.FirstChunk(w)
	last := txLayout.LastChunk(w)
	var refs []catalog.ArtifactRef
	for cid := first; ; cid++ {
		state, err := cat.State(cid, geometry.KindTxHash)
		if err != nil {
			return nil, fmt.Errorf("streaming: read txhash state chunk %s: %w", cid, err)
		}
		if state == geometry.StatePruning {
			refs = append(refs, catalog.ArtifactRef{Chunk: cid, Kind: geometry.KindTxHash, State: geometry.StatePruning})
		}
		if cid == last { // guard against chunk.ID wraparound at the top
			break
		}
	}
	return refs, nil
}
