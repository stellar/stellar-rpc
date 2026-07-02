package ingest

import (
	"context"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// HotIngester ingests one ledger by sequence into a long-lived, caller-owned
// store. The hot tier's HotService implements it — one atomic synced WriteBatch
// across all column families per ledger (decision (a); NO per-type fan-out, no
// errgroup) — and the cold drain loop (drain) consumes the same shape, so
// ColdService satisfies it too. (The "Hot" name is historical: this is just the
// per-ledger ingest contract now.)
//
// Ownership: the store is INJECTED into the implementation's constructor and
// owned by the caller (the daemon). The implementation does NOT open the store
// and does NOT close it — Close is intentionally absent from this interface.
//
// Input: seq is the DRIVER-VALIDATED ledger sequence of lcm — the drain loop
// has already read it off the view and checked it against the chunk's expected
// position (duplicate / out-of-order / overrun), so implementations consume it
// directly instead of re-deriving and re-error-handling it. lcm is a zero-copy
// xdr.LedgerCloseMetaView (a []byte alias over the source stream's BORROWED
// buffer), valid only for the current iteration step; an implementation must
// copy any bytes it retains. Ledgers are ingested sequentially — the source
// pulls the next only after Ingest returns — so synchronous consumption inside
// Ingest is safe.
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
