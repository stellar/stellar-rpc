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
)

// IndexBuild names one tx-hash index rebuild: the window and the coverage
// [Lo, Hi] to materialize. Terminal-ness (Hi == window's last chunk) is
// DERIVED at build time (Windows.IsTerminalCoverage), never carried as a field
// — the spec's "marked nowhere". It mirrors the resolver's plan value
// (design-docs/full-history-streaming-workflow.md "Postcondition-driven
// scheduling").
type IndexBuild struct {
	Window WindowID
	Lo, Hi chunk.ID
}

// BuildConfig is the dependency bundle buildTxhashIndex/buildThenSweep read: the
// catalog (key state, path layout, window arithmetic, the one-write protocol's
// CommitIndex + the sweeps) and a logger. BuildOpts are optional streamhash
// build options threaded into the merged txhash.BuildColdIndex — the cold
// payload/fingerprint/metadata options are pinned by BuildColdIndex itself and
// cannot be overridden here (see cold_index.go's "format options go last").
type BuildConfig struct {
	Catalog *Catalog
	Logger  *supportlog.Entry

	// BuildOpts are extra streamhash.BuildOptions (e.g. WithWorkers) passed
	// through to BuildColdIndex. Optional; the cold format options always win.
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
// gettransaction-full-history-design.md §7.2). It rebuilds window w's index at
// coverage [lo, hi] from scratch, running the one-write protocol with
// CommitIndex's batch-commit extension. The four steps map exactly onto the
// spec:
//
//  1. Skip check — if w's unique "frozen" coverage already equals [lo, hi],
//     return. This also short-circuits re-scheduled builds of finalized windows
//     (a full-window frozen coverage is terminal by definition), which must NOT
//     demand .bin inputs the terminal commit's sweep has since deleted. The skip
//     precedes the precondition for exactly that reason.
//  2. Precondition + mark — every chunk in [lo, hi] must have its
//     chunk:{c}:txhash key "frozen" (its .bin exists); fail loudly BEFORE any
//     key is touched (the executor's done-channels broadcast completion, not
//     success — this is the backstop). Then MarkIndexFreezing puts the coverage
//     key "freezing" (an idempotent overwrite of a crashed attempt's debris).
//  3. Write — k-way merge the .bin files for [lo, hi] into the .idx via the
//     merged txhash.BuildColdIndex (create-or-truncate at the coverage's
//     canonical path; minLedger anchored at lo.FirstLedger()), then fsync the
//     file + its dir (+ the grandparent dirent when this build created the
//     window dir).
//  4. Commit — Catalog.CommitIndex: one atomic synced batch promoting this
//     coverage to "frozen", demoting the predecessor to "pruning", and — iff
//     terminal — demoting every chunk:{c}:txhash key in the window to "pruning".
//
// buildTxhashIndex never deletes a file: file removal is exclusively the sweeps'
// job (buildThenSweep / the tick's prune scan). The crash matrix (§7.6) is
// covered by the four-step ordering: a crash before step 4 leaves the
// predecessor frozen and the new coverage as "freezing" debris; a crash after
// leaves the new coverage frozen and the demoted keys as "pruning" sweep work.
func buildTxhashIndex(ctx context.Context, w WindowID, lo, hi chunk.ID, cfg BuildConfig) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	if lo > hi {
		return fmt.Errorf("streaming: buildTxhashIndex window %s lo %s > hi %s", w, lo, hi)
	}
	cat := cfg.Catalog

	// Step 1 — skip check. If the window's unique frozen coverage already covers
	// exactly [lo, hi], there is nothing to write; leftover transient keys are
	// the sweeps' job, not the builder's. Checked FIRST so a re-scheduled build
	// of a finalized window (whose .bin inputs the terminal sweep deleted) never
	// reaches the precondition below.
	frozen, hasFrozen, err := cat.FrozenCoverage(w)
	if err != nil {
		return fmt.Errorf("streaming: buildTxhashIndex read frozen coverage window %s: %w", w, err)
	}
	if hasFrozen && frozen.Lo == lo && frozen.Hi == hi {
		cfg.Logger.Debugf("buildTxhashIndex: window %s coverage [%s,%s] already frozen; skipping", w, lo, hi)
		return nil
	}

	// Step 2a — loud precondition, checked BEFORE any key is touched. Every chunk
	// in [lo, hi] must have its .bin frozen.
	inputs, err := cat.txhashBinInputs(w, lo, hi)
	if err != nil {
		return err
	}

	// Step 2b — mark the coverage "freezing" (idempotent overwrite of any crashed
	// attempt's debris at this name).
	cov, err := cat.MarkIndexFreezing(w, lo, hi)
	if err != nil {
		return fmt.Errorf("streaming: buildTxhashIndex mark freezing %s: %w", indexKey(w, lo, hi), err)
	}

	// Test-only observation point at the post-mark / pre-write instant (§7.6
	// "after step 2, mid step 3"): new coverage "freezing", predecessor still the
	// unique frozen coverage, no resolvable in-flight name. No-op in production.
	cat.hooks.fireAfterIndexMark()

	// Step 3 — write the coverage's .idx from scratch. txhash.BuildColdIndex
	// create-or-truncates outputPath (streamhash's SortedBuilder), so a crashed
	// attempt's partial is overwritten wholesale, never appended. The window dir
	// is created on demand; detect whether THIS build created it so barrierNewFile
	// can fsync the grandparent dirent (txhash/index/) on a window's first build.
	idxPath := cat.layout.IndexFilePath(cov)
	windowDir := cat.layout.IndexWindowDir(w)
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
	// window dir) BEFORE the coverage flips to "frozen" in CommitIndex.
	if barErr := barrierNewFile(idxPath, newWindowDir); barErr != nil {
		return fmt.Errorf("streaming: buildTxhashIndex fsync barrier %s: %w", idxPath, barErr)
	}

	// Step 4 — commit: one atomic synced batch (promote new -> "frozen", demote
	// predecessor -> "pruning", and iff terminal demote every in-window
	// chunk:{c}:txhash -> "pruning"). CommitIndex re-derives the predecessor and
	// terminal-ness from durable state, so it is safe to re-run after a crash.
	if cerr := cat.CommitIndex(cov); cerr != nil {
		return fmt.Errorf("streaming: buildTxhashIndex commit window %s coverage [%s,%s]: %w", w, lo, hi, cerr)
	}
	return nil
}

// buildThenSweep is how the executor runs an IndexBuild (design-docs rule 4's
// eager call site / §7.4): buildTxhashIndex, then the standard sweeps for THIS
// window's "pruning" coverages and (terminal) demoted .bin inputs. The commit
// batch only demotes keys; this brings the demoted files back without waiting
// for a lifecycle tick.
//
// The sweep is WINDOW-LOCAL — it walks only b.Window's index keys and only the
// chunk:{c}:txhash keys in b.Window — so concurrent windows' sweeps touch
// disjoint keys and files (the executor holds at most one IndexBuild per
// window). As a bonus it finishes any "pruning" leftovers a previous crashed
// pass left in the same window. A crash anywhere mid-sweep leaves "pruning"
// keys the next build (or the tick's prune scan) re-runs — the same convergence
// story regardless of caller.
func buildThenSweep(ctx context.Context, b IndexBuild, cfg BuildConfig) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	cat := cfg.Catalog

	if err := buildTxhashIndex(ctx, b.Window, b.Lo, b.Hi, cfg); err != nil {
		return err
	}

	// Test-only observation point at the post-commit / pre-sweep instant (§7.6
	// "after step 4, before the eager sweep"). No-op in production.
	cat.hooks.fireAfterCommitBeforeSweep()

	// Sweep this window's superseded coverages ("pruning" index keys). The
	// just-frozen coverage is "frozen" and skipped; a predecessor demoted by the
	// commit (or by a previous crashed pass) is "pruning" and removed.
	covs, err := cat.IndexKeys(b.Window)
	if err != nil {
		return fmt.Errorf("streaming: buildThenSweep read index keys window %s: %w", b.Window, err)
	}
	for _, cov := range covs {
		if cov.State != StatePruning {
			continue
		}
		if serr := cat.SweepIndexKey(cov); serr != nil {
			return fmt.Errorf("streaming: buildThenSweep sweep coverage %s: %w", cov.Key, serr)
		}
	}

	// Sweep this window's demoted .bin inputs (terminal build) in one batched
	// pass. Non-terminal builds demote no inputs, so demoted is empty and
	// SweepChunkArtifacts is a no-op.
	demoted, err := cat.windowDemotedTxhashRefs(b.Window)
	if err != nil {
		return err
	}
	if serr := cat.SweepChunkArtifacts(demoted); serr != nil {
		return fmt.Errorf("streaming: buildThenSweep sweep demoted inputs window %s: %w", b.Window, serr)
	}
	return nil
}

// txhashBinInputs returns the .bin paths for chunks [lo, hi], enforcing rule
// 3's loud precondition: every chunk in the range MUST have its chunk:{c}:txhash
// key "frozen" (its .bin exists and is durable, trusted blindly). It returns an
// error naming the first offending chunk and produces NO partial inputs on
// failure — the precondition is checked before any write in buildTxhashIndex.
func (c *Catalog) txhashBinInputs(w WindowID, lo, hi chunk.ID) ([]string, error) {
	inputs := make([]string, 0, uint32(hi)-uint32(lo)+1)
	for cid := lo; ; cid++ {
		state, err := c.State(cid, KindTxHash)
		if err != nil {
			return nil, fmt.Errorf("streaming: buildTxhashIndex read txhash state chunk %s: %w", cid, err)
		}
		if state != StateFrozen {
			return nil, fmt.Errorf(
				"streaming: buildTxhashIndex precondition violated: window %s chunk %s txhash is %q, want %q",
				w, cid, state, StateFrozen)
		}
		inputs = append(inputs, c.layout.TxHashBinPath(cid))
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
func (c *Catalog) windowDemotedTxhashRefs(w WindowID) ([]ArtifactRef, error) {
	first := c.windows.FirstChunk(w)
	last := c.windows.LastChunk(w)
	var refs []ArtifactRef
	for cid := first; ; cid++ {
		state, err := c.State(cid, KindTxHash)
		if err != nil {
			return nil, fmt.Errorf("streaming: read txhash state chunk %s: %w", cid, err)
		}
		if state == StatePruning {
			refs = append(refs, ArtifactRef{Chunk: cid, Kind: KindTxHash, State: StatePruning})
		}
		if cid == last { // guard against chunk.ID wraparound at the top
			break
		}
	}
	return refs, nil
}
