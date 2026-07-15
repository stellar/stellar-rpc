package registry

import (
	"cmp"
	"fmt"
	"slices"
	"time"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/hotchunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/txhash"
)

// Options configures BuildFromCatalog. The zero value works: defaults fill in
// the grace period and cache capacities, and the logger falls back to the
// catalog's.
type Options struct {
	// Grace is the reaper's grace period T. Assembly derives it from the
	// serving config (max request duration + margin); zero means
	// DefaultGrace.
	Grace time.Duration
	// LedgerCacheCap / EventCacheCap bound the cold-reader LRU caches; zero
	// means DefaultLedgerCacheCap / DefaultEventCacheCap.
	LedgerCacheCap int
	EventCacheCap  int
	// PreOpened hands in hot handles the caller already holds (the live
	// chunk's write handle, owned by ingestion at startup) so the build
	// doesn't try a second write open — RocksDB allows only one. Ownership
	// of every handle the build ACCEPTS transfers to the registry; a handle
	// the build rejects (its catalog key is not "ready", or it sits below
	// the floor) stays the caller's to close.
	PreOpened map[chunk.ID]*hotchunk.DB
	// Logger for the registry and its reaper; nil falls back to cat.Logger().
	Logger *supportlog.Entry
}

// BuildFromCatalog constructs the registry's initial View from a catalog scan
// — the startup row of the spec's View-update table. It runs before serving
// opens and before the lifecycle goroutine starts, so no transition can land
// between the scan and the hooks becoming live.
//
//   - "ready" hot keys open write handles (hotchunk.OpenReadyWrite — the
//     events facade is only warmed on read-write opens), except chunks the
//     caller pre-opened, whose handles are adopted as-is.
//   - "frozen" chunk artifact keys become cold flags.
//   - Frozen index coverages open one txhash.ColdReader per window.
//   - The floor comes from Retention.FloorAt over the last complete chunk at
//     latest, and below-floor entries never enter the View (they are prune
//     debris the first lifecycle run sweeps).
//
// Only "ready"/"frozen" keys are visible (R1); transient states are skipped.
// latest seeds the watermark — the caller passes the derived last-committed
// ledger. On error every resource the build itself opened is closed;
// PreOpened handles stay with the caller.
func BuildFromCatalog(
	cat *catalog.Catalog, ret geometry.Retention, latest uint32, opts Options,
) (*Registry, error) {
	logger := opts.Logger
	if logger == nil {
		logger = cat.Logger()
	}
	layout := cat.Layout()

	view := &View{
		floor: ret.FloorAt(geometry.LastCompleteChunkAt(latest)),
		hot:   map[chunk.ID]*hotchunk.DB{},
		cold:  map[chunk.ID]ColdChunk{},
	}

	// Everything opened HERE (not pre-opened) closes on a failed build.
	var opened []interface{ Close() error }
	fail := func(err error) (*Registry, error) {
		for _, res := range opened {
			if cerr := res.Close(); cerr != nil {
				logger.WithError(cerr).Warn("registry: closing partially built view")
			}
		}
		return nil, err
	}

	readyIDs, err := cat.ReadyHotChunkKeys()
	if err != nil {
		return fail(fmt.Errorf("registry: scan ready hot chunks: %w", err))
	}
	for _, c := range readyIDs {
		if c < view.floor {
			continue
		}
		if db, ok := opts.PreOpened[c]; ok {
			view.hot[c] = db
			continue
		}
		db, oerr := hotchunk.OpenReadyWrite(geometry.HotReady, layout.HotChunkPath(c), c, logger)
		if oerr != nil {
			return fail(fmt.Errorf("registry: open hot chunk %s: %w", c, oerr))
		}
		opened = append(opened, db)
		view.hot[c] = db
	}
	for c := range opts.PreOpened {
		if _, accepted := view.hot[c]; !accepted {
			logger.WithField("chunk", c.String()).
				Warn("registry: pre-opened hot handle is not a ready in-retention chunk; not serving it")
		}
	}

	refs, err := cat.ChunkArtifactKeys()
	if err != nil {
		return fail(fmt.Errorf("registry: scan chunk artifacts: %w", err))
	}
	for _, ref := range refs {
		if ref.State != geometry.StateFrozen || ref.Chunk < view.floor {
			continue
		}
		cc := view.cold[ref.Chunk]
		switch ref.Kind {
		case geometry.KindLedgers:
			cc.Ledgers = true
		case geometry.KindEvents:
			cc.Events = true
		case geometry.KindTxHash:
			continue // index-build input, never served from the chunk tier
		default:
			continue
		}
		view.cold[ref.Chunk] = cc
	}

	covs, err := cat.AllTxHashIndexKeys()
	if err != nil {
		return fail(fmt.Errorf("registry: scan tx index coverages: %w", err))
	}
	seenWindows := map[geometry.TxHashIndexID]bool{}
	for _, cov := range covs {
		if cov.State != geometry.StateFrozen || cov.Hi < view.floor {
			continue
		}
		if seenWindows[cov.Index] {
			// INV-2: at most one frozen coverage per window; two is a
			// detectable bug, not a tie to break (mirrors
			// catalog.FrozenTxHashIndex).
			return fail(fmt.Errorf("registry: index %s has two frozen coverages — uniqueness invariant violated",
				cov.Index))
		}
		seenWindows[cov.Index] = true
		reader, oerr := txhash.OpenColdReader(layout.TxHashIndexFilePath(cov))
		if oerr != nil {
			return fail(fmt.Errorf("registry: open tx index for window %s: %w", cov.Index, oerr))
		}
		opened = append(opened, reader)
		view.indexes = append(view.indexes, IndexCoverage{
			Window: cov.Index, Lo: cov.Lo, Hi: cov.Hi, Idx: reader,
		})
	}
	slices.SortFunc(view.indexes, func(a, b IndexCoverage) int {
		return cmp.Compare(a.Window, b.Window)
	})

	reg := newRegistry(layout, logger, opts)
	reg.latest.Store(latest)
	reg.current.Store(view)
	logger.WithField("floor", view.floor.String()).
		WithField("hot", len(view.hot)).
		WithField("cold", len(view.cold)).
		WithField("indexes", len(view.indexes)).
		WithField("latest", latest).
		Info("registry: initial view built from catalog")
	return reg, nil
}
