package backfill

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"os"
	"time"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/durable"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/observability"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/txhash"
)

// IndexBuild names one tx-hash index rebuild: the window and coverage [Lo, Hi].
// Terminal-ness (Hi == the window's last chunk) is derived at build time, not stored.
type IndexBuild struct {
	Index  geometry.TxHashIndexID
	Lo, Hi chunk.ID
}

// BuildConfig is what buildTxhashIndex/buildThenSweep need.
type BuildConfig struct {
	Catalog *catalog.Catalog
	Logger  *supportlog.Entry

	// Metrics meters the eager post-build sweep (Prune); nil ⇒ Nop via MetricsOrNop.
	Metrics observability.Metrics
}

func (cfg BuildConfig) validate() error {
	if cfg.Catalog == nil {
		return errors.New("BuildConfig.Catalog is nil")
	}
	if cfg.Logger == nil {
		return errors.New("BuildConfig.Logger is nil")
	}
	return nil
}

// buildTxhashIndex rebuilds window w's index at [lo, hi] from scratch (rule 3 /
// gettransaction §7.2) via the one-write protocol: skip if already frozen; require
// every .bin frozen; merge into the .idx and fsync; commit. It never deletes a
// file — that's the sweeps' job. A crash before the commit leaves "freezing"
// debris; after, the demoted keys become "pruning" sweep work.
func buildTxhashIndex(ctx context.Context, w geometry.TxHashIndexID, lo, hi chunk.ID, cfg BuildConfig) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	if lo > hi {
		return fmt.Errorf("buildTxhashIndex window %s lo %s > hi %s", w, lo, hi)
	}
	cat := cfg.Catalog
	layout := cat.Layout()

	// Skip first, before the precondition: a finalized window's .bin inputs are
	// already swept, so demanding them frozen would wrongly fail a re-schedule.
	frozen, hasFrozen, err := cat.FrozenTxHashIndex(w)
	if err != nil {
		return fmt.Errorf("buildTxhashIndex read frozen coverage window %s: %w", w, err)
	}
	if hasFrozen && frozen.Lo == lo && frozen.Hi == hi {
		cfg.Logger.Debugf("buildTxhashIndex: window %s coverage [%s,%s] already frozen; skipping", w, lo, hi)
		return nil
	}

	// Precondition, before any key is marked (no debris if it fails).
	inputs, err := txhashBinInputs(cat, w, lo, hi)
	if err != nil {
		return err
	}

	// The one-write protocol, straight-line (see catalog_protocol.go header). Each
	// beat carries per-site error context, so the // one-write: labels keep the
	// four steps greppable without a wrapper.

	// one-write:mark — MarkTxHashIndexFreezing returns the coverage the flip needs.
	cov, err := cat.MarkTxHashIndexFreezing(w, lo, hi)
	if err != nil {
		return fmt.Errorf("buildTxhashIndex mark freezing %s: %w",
			geometry.TxHashIndexKey(w, lo, hi), err)
	}
	idxPath := layout.TxHashIndexFilePath(cov)

	// one-write:create — write from scratch (BuildColdIndex truncates any crashed
	// partial). MkdirAll is idempotent, so the index dir is created on demand.
	indexDir := layout.TxHashIndexDir(w)
	if mkErr := os.MkdirAll(indexDir, 0o755); mkErr != nil {
		return fmt.Errorf("buildTxhashIndex mkdir %s: %w", indexDir, mkErr)
	}
	if berr := txhash.BuildColdIndex(
		ctx, inputs, idxPath, lo.FirstLedger(), hi.LastLedger(),
	); berr != nil {
		return fmt.Errorf("buildTxhashIndex build window %s coverage [%s,%s]: %w", w, lo, hi, berr)
	}

	// one-write:barrier — fsync the .idx file + its dirents before the keys flip.
	if barErr := durable.BarrierNewFile(idxPath); barErr != nil {
		return fmt.Errorf("buildTxhashIndex fsync barrier %s: %w", idxPath, barErr)
	}

	// one-write:flip — CommitTxHashIndex promotes cov, demotes the predecessor, and
	// (terminal builds) demotes the window .bin inputs. It re-derives predecessor +
	// terminal-ness from durable state, so it's safe to re-run after a crash.
	if cerr := cat.CommitTxHashIndex(cov); cerr != nil {
		return fmt.Errorf("buildTxhashIndex commit window %s coverage [%s,%s]: %w", w, lo, hi, cerr)
	}
	return nil
}

// buildThenSweep runs an IndexBuild (rule 4), then eagerly sweeps this window's
// superseded ("pruning") coverages plus (terminal builds) its demoted .bin inputs —
// freeing disk without waiting for a prune tick. Window-local, so concurrent windows'
// sweeps don't collide; a crash mid-sweep is finished by the next run. Abandoned
// "freezing" debris from a crashed earlier build is the lifecycle prune stage's job.
func buildThenSweep(ctx context.Context, b IndexBuild, cfg BuildConfig) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	cat := cfg.Catalog

	if err := buildTxhashIndex(ctx, b.Index, b.Lo, b.Hi, cfg); err != nil {
		return err
	}

	// Eager sweep: reclaim the now-redundant inputs the fresh .idx supersedes.
	sweepStart := time.Now()
	swept := 0

	covs, err := cat.TxHashIndexKeys(b.Index)
	if err != nil {
		return fmt.Errorf("buildThenSweep read index keys window %s: %w", b.Index, err)
	}
	for _, cov := range covs {
		// Sweep the superseded ("pruning") coverages this build just demoted. The
		// coverage just built is "frozen" now, so it's skipped.
		if cov.State != geometry.StatePruning {
			continue
		}
		if serr := cat.SweepTxHashIndexKey(cov); serr != nil {
			return fmt.Errorf("buildThenSweep sweep coverage %s: %w", cov.Key, serr)
		}
		swept++
	}

	demoted, err := demotedTxhashRefs(cat, b.Index)
	if err != nil {
		return err
	}
	if serr := cat.SweepChunkArtifacts(demoted); serr != nil {
		return fmt.Errorf("buildThenSweep sweep demoted inputs window %s: %w", b.Index, serr)
	}
	swept += len(demoted)

	// Meter the sweep on the success path only — a mid-sweep failure aborts the plan.
	observability.MetricsOrNop(cfg.Metrics).Prune(swept, time.Since(sweepStart))
	return nil
}

// txhashBinInputs returns the .bin paths for chunks [lo, hi], requiring every
// chunk's txhash key be "frozen". Errors on the first that isn't, with no partial result.
func txhashBinInputs(cat *catalog.Catalog, w geometry.TxHashIndexID, lo, hi chunk.ID) ([]string, error) {
	layout := cat.Layout()
	inputs := make([]string, 0, uint32(hi)-uint32(lo)+1)
	for cs, err := range txHashStates(cat, lo, hi) {
		if err != nil {
			return nil, err
		}
		if cs.State != geometry.StateFrozen {
			return nil, fmt.Errorf(
				"buildTxhashIndex precondition violated: window %s chunk %s txhash is %q, want %q",
				w, cs.Chunk, cs.State, geometry.StateFrozen)
		}
		inputs = append(inputs, layout.TxHashBinPath(cs.Chunk))
	}
	return inputs, nil
}

// demotedTxhashRefs returns window w's "pruning" txhash refs — the demoted
// .bin inputs a terminal commit (or crashed pass) left. None on a non-terminal build.
func demotedTxhashRefs(cat *catalog.Catalog, w geometry.TxHashIndexID) ([]catalog.ArtifactRef, error) {
	txLayout := cat.TxHashIndexLayout()
	var refs []catalog.ArtifactRef
	for cs, err := range txHashStates(cat, txLayout.FirstChunk(w), txLayout.LastChunk(w)) {
		if err != nil {
			return nil, err
		}
		if cs.State == geometry.StatePruning {
			refs = append(refs, catalog.ArtifactRef{Chunk: cs.Chunk, Kind: geometry.KindTxHash, State: geometry.StatePruning})
		}
	}
	return refs, nil
}

// chunkTxHashState pairs a chunk with its txhash-artifact lifecycle state — the
// value half of the txHashStates iterator.
type chunkTxHashState struct {
	Chunk chunk.ID
	State geometry.State
}

// txHashStates yields each chunk's txhash state over the inclusive [lo, hi] range,
// paired with any read error (the iter.Seq2[T, error] shape the catalog scans use,
// since catalog.State can fail).
func txHashStates(cat *catalog.Catalog, lo, hi chunk.ID) iter.Seq2[chunkTxHashState, error] {
	return func(yield func(chunkTxHashState, error) bool) {
		for cid := lo; cid <= hi; cid++ {
			state, err := cat.State(cid, geometry.KindTxHash)
			if err != nil {
				yield(chunkTxHashState{Chunk: cid}, fmt.Errorf("read txhash state chunk %s: %w", cid, err))
				return
			}
			if !yield(chunkTxHashState{Chunk: cid, State: state}, nil) {
				return
			}
		}
	}
}
