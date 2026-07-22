package lifecycle

import (
	"context"
	"time"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/hotchunk"
)

// Deferred deletion. A lifecycle run demotes a discarded hot chunk during the
// discard stage (unpublish its handle, mark it transient) and appends it here;
// after all stages finish the run waits the grace period once and destroys each
// (close the handle, remove the dir and key). Demotion is the durable work list:
// the transient key persists, so a run that crashes or a destroy that is skipped
// is re-discovered by the next run's discard scan.

// hotDeletion is one demoted hot chunk awaiting destruction. handle is the shared
// handle removed from the router, or nil when the chunk was re-discovered from a
// leftover transient key (its handle is no longer published).
type hotDeletion struct {
	chunk  chunk.ID
	handle *hotchunk.DB
}

// hotDeletions collects the hot chunks a run demoted, for destruction at end of run.
type hotDeletions struct {
	items []hotDeletion
}

// demote unpublishes chunk c's handle (so new admissions stop routing to it) and
// marks it transient, then records it for destruction. Idempotent: a re-run finds
// the handle already gone (DiscardHandle returns nil) and re-marks the key.
func (d *hotDeletions) demote(router HandleDiscarder, cat *catalog.Catalog, c chunk.ID) error {
	var handle *hotchunk.DB
	if router != nil {
		handle = router.DiscardHandle(c)
	}
	if err := cat.PutHotTransient(c); err != nil {
		return err
	}
	d.items = append(d.items, hotDeletion{chunk: c, handle: handle})
	return nil
}

// destroy waits the grace period once (skipped when zero, as it is until the read
// server sets a request deadline), then closes each handle and removes each chunk.
// A handle still serving an in-flight reader is left for a later run: CloseIfIdle
// reports busy, the transient key persists, and the next discard scan retries.
func (d *hotDeletions) destroy(ctx context.Context, cfg Config, cat *catalog.Catalog) {
	if len(d.items) == 0 {
		return
	}
	if cfg.grace > 0 {
		select {
		case <-time.After(cfg.grace):
		case <-ctx.Done():
			return // shutdown: the transient keys persist for the next run
		}
	}
	for _, it := range d.items {
		if it.handle != nil {
			if ok, _ := it.handle.CloseIfIdle(); !ok {
				cfg.Logger.WithField("chunk", it.chunk.String()).
					Warn("lifecycle: hot chunk close deferred (reader in flight); retry next run")
				continue // leave the transient key; the next scan re-discovers it
			}
		}
		if err := cat.DestroyHotChunk(it.chunk); err != nil {
			cfg.Logger.WithError(err).WithField("chunk", it.chunk.String()).
				Warn("lifecycle: destroy hot chunk failed; retry next run")
		}
	}
}

// HandleDiscarder is the slice of the router the discard path uses: unpublish a
// hot handle and return it for closing. Narrowed to an interface so the lifecycle
// does not depend on the whole serving package (a nil discarder is the bounded
// backfill / test case). *serving.Router satisfies it.
type HandleDiscarder interface {
	DiscardHandle(c chunk.ID) *hotchunk.DB
}
