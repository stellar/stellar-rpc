package ingest

import (
	"context"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// HotIngester ingests one data type for one ledger into a long-lived hot store.
//
// Ownership: the hot store is INJECTED into the ingester's constructor and owned
// by the caller (the daemon). The ingester does NOT open the store and does NOT
// close it — Close is intentionally absent from this interface.
//
// Input: seq is the DRIVER-VALIDATED ledger sequence of lcm — the drain loop
// has already read it off the view and checked it against the chunk's expected
// position (duplicate / out-of-order / overrun), so ingesters consume it
// directly instead of each re-deriving and re-error-handling it. lcm is a
// zero-copy xdr.LedgerCloseMetaView (a []byte alias over the source stream's
// BORROWED buffer), valid only for the current iteration step; an ingester
// must copy any bytes it retains. The hot fan-out (HotService) waits for all
// ingesters to finish a ledger before the source pulls the next one, so
// synchronous consumption inside Ingest is safe.
//
// Concurrency: distinct HotIngester instances are run concurrently for the same
// ledger (HotService fans out via errgroup); each instance touches only its own
// store plus the read-only view.
type HotIngester interface {
	Ingest(ctx context.Context, seq uint32, lcm xdr.LedgerCloseMetaView) error
}

// ColdIngester ingests one data type for one chunk into a per-chunk cold writer.
//
// Ownership: the ingester OPENS its own per-chunk writer in its constructor and
// owns its lifecycle. Finalize commits the chunk's artifact (explicit,
// error-checked, never deferred). Close is always deferred and idempotent; on
// the failure path (Finalize never ran) it drops any partial file.
//
// Contract: Finalize must NOT be called after a failed Ingest — once any
// Ingest errors, the chunk is abandoned via Close and retried from scratch.
// Implementations may have committed partial per-ledger state before the
// error (e.g. the events ingester's mirror/pack run ahead of its offsets
// commit point), so a post-failure Finalize could publish an inconsistent
// artifact; implementations are encouraged to latch the failure and refuse
// (eventsCold does).
//
// Input: same driver-validated-seq and borrowed-view contract as HotIngester.
// ColdService drives the per-ledger Ingest calls sequentially, so each view is
// fully consumed before the next.
type ColdIngester interface {
	Ingest(ctx context.Context, seq uint32, lcm xdr.LedgerCloseMetaView) error
	Finalize(ctx context.Context) error
	Close() error
}
