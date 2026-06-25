package backfill

import (
	"context"
	"errors"
	"fmt"
	"os"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/streamhash"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash"
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

	// BuildOpts are extra streamhash options; the cold format options are pinned in BuildColdIndex.
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

// buildTxhashIndex rebuilds window w's index at [lo, hi] from scratch (rule 3 /
// gettransaction §7.2) via the one-write protocol: skip if already frozen; require
// every .bin frozen; merge into the .idx and fsync; commit. It never deletes a
// file — that's the sweeps' job. A crash before the commit leaves "freezing"
// debris; after, the demoted keys become "pruning" sweep work.
//
//nolint:cyclop // sequential one-write protocol; splitting the ordered steps fragments the invariant
func buildTxhashIndex(ctx context.Context, w geometry.TxHashIndexID, lo, hi chunk.ID, cfg BuildConfig) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	if lo > hi {
		return fmt.Errorf("streaming: buildTxhashIndex window %s lo %s > hi %s", w, lo, hi)
	}
	cat := cfg.Catalog
	layout := cat.Layout()

	// Skip first, before the precondition: a finalized window's .bin inputs are
	// already swept, so demanding them frozen would wrongly fail a re-schedule.
	frozen, hasFrozen, err := cat.FrozenTxHashIndex(w)
	if err != nil {
		return fmt.Errorf("streaming: buildTxhashIndex read frozen coverage window %s: %w", w, err)
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

	cov, err := cat.MarkTxHashIndexFreezing(w, lo, hi)
	if err != nil {
		return fmt.Errorf("streaming: buildTxhashIndex mark freezing %s: %w", geometry.TxHashIndexKey(w, lo, hi), err)
	}

	// Write from scratch (BuildColdIndex truncates any crashed partial). Note if
	// THIS build created the index dir, so the barrier fsyncs its grandparent.
	idxPath := layout.TxHashIndexFilePath(cov)
	indexDir := layout.TxHashIndexDir(w)
	_, statErr := os.Stat(indexDir)
	newIndexDir := errors.Is(statErr, os.ErrNotExist)
	if statErr != nil && !newIndexDir {
		return fmt.Errorf("streaming: buildTxhashIndex stat index dir %s: %w", indexDir, statErr)
	}
	if newIndexDir {
		if mkErr := os.MkdirAll(indexDir, 0o755); mkErr != nil {
			return fmt.Errorf("streaming: buildTxhashIndex mkdir %s: %w", indexDir, mkErr)
		}
	}

	minLedger := lo.FirstLedger()
	maxLedger := hi.LastLedger()
	if berr := txhash.BuildColdIndex(ctx, inputs, idxPath, minLedger, maxLedger, cfg.BuildOpts...); berr != nil {
		return fmt.Errorf("streaming: buildTxhashIndex build window %s coverage [%s,%s]: %w", w, lo, hi, berr)
	}

	if barErr := geometry.BarrierNewFile(idxPath, newIndexDir); barErr != nil {
		return fmt.Errorf("streaming: buildTxhashIndex fsync barrier %s: %w", idxPath, barErr)
	}

	// Commit: re-derives predecessor + terminal-ness from durable state, so it's
	// safe to re-run after a crash.
	if cerr := cat.CommitTxHashIndex(cov); cerr != nil {
		return fmt.Errorf("streaming: buildTxhashIndex commit window %s coverage [%s,%s]: %w", w, lo, hi, cerr)
	}
	return nil
}

// buildThenSweep runs an IndexBuild (rule 4), then eagerly sweeps this window's
// "pruning" coverages and (terminal builds) its demoted .bin inputs — freeing
// disk without waiting for a prune tick. Window-local, so concurrent windows'
// sweeps don't collide; a crash mid-sweep is finished by the next run.
func buildThenSweep(ctx context.Context, b IndexBuild, cfg BuildConfig) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	cat := cfg.Catalog

	if err := buildTxhashIndex(ctx, b.Index, b.Lo, b.Hi, cfg); err != nil {
		return err
	}

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

	demoted, err := demotedTxhashRefs(cat, b.Index)
	if err != nil {
		return err
	}
	if serr := cat.SweepChunkArtifacts(demoted); serr != nil {
		return fmt.Errorf("streaming: buildThenSweep sweep demoted inputs window %s: %w", b.Index, serr)
	}
	return nil
}

// txhashBinInputs returns the .bin paths for chunks [lo, hi], requiring every
// chunk's txhash key be "frozen". Errors on the first that isn't, with no partial result.
func txhashBinInputs(cat *catalog.Catalog, w geometry.TxHashIndexID, lo, hi chunk.ID) ([]string, error) {
	layout := cat.Layout()
	inputs := make([]string, 0, uint32(hi)-uint32(lo)+1)
	err := forEachChunkTxHashState(cat, lo, hi, func(cid chunk.ID, state geometry.State) error {
		if state != geometry.StateFrozen {
			return fmt.Errorf(
				"streaming: buildTxhashIndex precondition violated: window %s chunk %s txhash is %q, want %q",
				w, cid, state, geometry.StateFrozen)
		}
		inputs = append(inputs, layout.TxHashBinPath(cid))
		return nil
	})
	if err != nil {
		return nil, err
	}
	return inputs, nil
}

// demotedTxhashRefs returns window w's "pruning" txhash refs — the demoted
// .bin inputs a terminal commit (or crashed pass) left. None on a non-terminal build.
func demotedTxhashRefs(cat *catalog.Catalog, w geometry.TxHashIndexID) ([]catalog.ArtifactRef, error) {
	txLayout := cat.TxHashIndexLayout()
	var refs []catalog.ArtifactRef
	err := forEachChunkTxHashState(cat, txLayout.FirstChunk(w), txLayout.LastChunk(w),
		func(cid chunk.ID, state geometry.State) error {
			if state == geometry.StatePruning {
				refs = append(refs, catalog.ArtifactRef{Chunk: cid, Kind: geometry.KindTxHash, State: geometry.StatePruning})
			}
			return nil
		})
	if err != nil {
		return nil, err
	}
	return refs, nil
}

// forEachChunkTxHashState calls fn with each chunk's txhash state over [lo, hi].
// Centralises the chunk.ID wraparound guard (the top can be the max id).
func forEachChunkTxHashState(cat *catalog.Catalog, lo, hi chunk.ID, fn func(chunk.ID, geometry.State) error) error {
	for cid := lo; ; cid++ {
		state, err := cat.State(cid, geometry.KindTxHash)
		if err != nil {
			return fmt.Errorf("streaming: read txhash state chunk %s: %w", cid, err)
		}
		if ferr := fn(cid, state); ferr != nil {
			return ferr
		}
		if cid == hi {
			break
		}
	}
	return nil
}
