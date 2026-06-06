package ingest

import (
	"context"
	"io"
)

// ingester ingests one data type into one storage tier. Hot ingesters
// implement only this interface; cold ingesters implement coldIngester (below).
// The hot driver holds a []ingester, the cold driver a []coldIngester — there
// is no mixed-type slice and no type assertion in the per-ledger loop.
//
// Concurrency:
//   - Ingest on a single ingester instance is invoked serially across ledgers
//     (each ingester's own state is never accessed by two goroutines).
//   - Distinct ingester instances are invoked concurrently within the same
//     ledger (the driver fans out per-ledger via errgroup). Each instance
//     reads its own state plus the read-only Ledger value.
//
// Lifecycle:
//   - Constructors open the underlying store/writer. For cold tiers that opens
//     the per-chunk writer.
//   - Ingest is called once per ledger in the chunk range.
//   - Close is ALWAYS deferred. It is idempotent and drops the store handle.
//     For cold ingesters, when Finalize never ran (the failure path), the
//     underlying packfile writer's Close removes any partial file.
type ingester interface {
	Ingest(ctx context.Context, l Ledger) error
	io.Closer
}

// coldIngester adds the cold-tier commit step: Commit (ledger) / Finish +
// WriteColdIndex (events) / sort + write .bin (txhash).
//
// Finalize is called explicitly by the driver on the success path with an error
// check; it is never deferred. An error from Finalize means the chunk's cold
// artifact did not durably land and must be re-ingested.
//
// Hot ingesters intentionally do NOT implement coldIngester — the distinction
// is enforced at compile time by the per-driver slice type.
type coldIngester interface {
	ingester
	Finalize(ctx context.Context) error
}
