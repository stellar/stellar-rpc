package ingest

import (
	"context"

	"github.com/stellar/go-stellar-sdk/xdr"
)

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
// Input: seq is the cursor-validated ledger sequence of lcm (the shared
// SeqValidatedCursor has already checked contiguity), and lcm is a zero-copy
// xdr.LedgerCloseMetaView over the source stream's BORROWED buffer, valid only for
// the current iteration step — an implementation must copy any bytes it retains.
// ColdService drives the per-ledger Ingest calls sequentially, so each view is
// fully consumed before the next.
type ColdIngester interface {
	Ingest(ctx context.Context, seq uint32, lcm xdr.LedgerCloseMetaView) error
	Finalize(ctx context.Context) error
	Close() error
}
