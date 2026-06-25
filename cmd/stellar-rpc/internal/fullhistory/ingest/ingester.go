package ingest

import (
	"context"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// HotIngester ingests one data type for one ledger into a long-lived hot store.
//
// Ownership: the store is INJECTED and caller-owned (the daemon); the ingester
// does NOT open or close it — Close is intentionally absent.
//
// Input: seq is the DRIVER-VALIDATED sequence of lcm (drain already checked it
// against the chunk's expected position), so ingesters consume it directly. lcm
// is a zero-copy view over the source's BORROWED buffer, valid only this step —
// an ingester must copy what it retains. The view is consumed synchronously
// within Ingest, so it is never read past its lifetime.
type HotIngester interface {
	Ingest(ctx context.Context, seq uint32, lcm xdr.LedgerCloseMetaView) error
}

// ColdIngester ingests one data type for one chunk into a per-chunk cold writer.
//
// Ownership: the ingester OPENS its own writer and owns its lifecycle. Finalize
// commits the artifact (explicit, error-checked); Close is deferred + idempotent
// and drops any partial on the failure path.
//
// Contract: Finalize must NOT be called after a failed Ingest — the chunk is
// abandoned via Close and retried from scratch. Partial per-ledger state may
// already be committed, so a post-failure Finalize could publish an inconsistent
// artifact; implementations should latch the failure and refuse (eventsCold does).
//
// Input: same driver-validated-seq + borrowed-view contract as HotIngester;
// ColdService drives Ingest sequentially.
type ColdIngester interface {
	Ingest(ctx context.Context, seq uint32, lcm xdr.LedgerCloseMetaView) error
	Finalize(ctx context.Context) error
	Close() error
}
