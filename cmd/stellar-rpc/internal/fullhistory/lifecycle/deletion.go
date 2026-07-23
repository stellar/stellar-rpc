package lifecycle

import (
	"context"
	"errors"
	"time"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
)

// Deferred deletion. A lifecycle run demotes each retired resource during a stage
// (a discarded hot chunk to "transient" with its handle unpublished; pruned cold
// artifacts to "pruning") and appends its destroy step here. After all stages
// finish the run waits the grace period once and runs every destroy. The catalog
// demotion is the durable work list: the demoted key persists, so a run that
// crashes or a destroy that is skipped is re-discovered by the next run's scans.

// errReaderInFlight marks a hot handle that could not be closed because an
// operation is still in flight. The destroy is skipped and retried next run.
var errReaderInFlight = errors.New("hot handle busy: reader in flight")

// pendingDeletions collects the destroy steps a run demoted, to run at end of run.
type pendingDeletions struct {
	items []deferredDestroy
}

type deferredDestroy struct {
	label   string
	destroy func() error
}

func (p *pendingDeletions) add(label string, destroy func() error) {
	p.items = append(p.items, deferredDestroy{label: label, destroy: destroy})
}

// demoteHotChunk unpublishes chunk c's handle (so new admissions stop routing to
// it) and marks it transient, then queues the destroy: close the handle, then
// remove the dir and key. router may be nil (bounded backfill / tests).
func (p *pendingDeletions) demoteHotChunk(router HandleRetirer, cat *catalog.Catalog, c chunk.ID) error {
	if router != nil {
		router.DiscardHandle(c)
	}
	if err := cat.PutHotTransient(c); err != nil {
		return err
	}
	p.add("hot chunk "+c.String(), func() error {
		// Close the handle before removing files. A reader still in flight leaves
		// the transient key for a later run: CloseDiscarded reports false, the next
		// discard scan re-collects the key, and the router keeps the handle in its
		// closing set until it drains — so the close itself retries, not just the
		// catalog cleanup.
		if router != nil && !router.CloseDiscarded(c) {
			return errReaderInFlight
		}
		return cat.DestroyHotChunk(c)
	})
	return nil
}

// demoteChunkArtifacts demotes a batch of per-chunk artifact refs to "pruning" and
// queues their file/key destroy.
func (p *pendingDeletions) demoteChunkArtifacts(cat *catalog.Catalog, refs []catalog.ArtifactRef) error {
	if err := cat.DemoteChunkArtifacts(refs); err != nil {
		return err
	}
	p.add("chunk artifacts", func() error { return cat.DestroyChunkArtifacts(refs) })
	return nil
}

// demoteTxHashIndex demotes one index coverage to "pruning" and queues its
// file/key destroy.
func (p *pendingDeletions) demoteTxHashIndex(cat *catalog.Catalog, cov geometry.TxHashIndexCoverage) error {
	if err := cat.DemoteTxHashIndexKey(cov); err != nil {
		return err
	}
	p.add("index "+cov.Key, func() error { return cat.DestroyTxHashIndexKey(cov) })
	return nil
}

// run waits the grace period once, then runs every queued destroy. A destroy that
// fails is logged and left for the next run's scan to re-discover via its still-
// demoted key.
func (p *pendingDeletions) run(ctx context.Context, cfg Config) {
	if len(p.items) == 0 {
		return
	}
	if cfg.Grace > 0 {
		select {
		case <-time.After(cfg.Grace):
		case <-ctx.Done():
			return // shutdown: the demoted keys persist for the next run
		}
	}
	for _, it := range p.items {
		if err := it.destroy(); err != nil {
			cfg.Logger.WithError(err).WithField("target", it.label).
				Warn("lifecycle: deferred destroy skipped; retry next run")
		}
	}
}

// HandleRetirer is the slice of the router the discard path uses: unpublish a hot
// handle (DiscardHandle), then close it once idle (CloseDiscarded), retried across
// runs until it drains. An interface so the lifecycle does not depend on the whole
// serving package (a nil retirer is the bounded backfill / test case).
// *serving.Router satisfies it.
type HandleRetirer interface {
	DiscardHandle(c chunk.ID)
	CloseDiscarded(c chunk.ID) bool
}
