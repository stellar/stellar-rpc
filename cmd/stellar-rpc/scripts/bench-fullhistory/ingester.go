package main

import (
	"context"
	"io"
)

// Ingester ingests one data type into one storage tier. Hot ingesters
// implement only this interface; cold ingesters implement ColdIngester
// (below). The driver holds one typed slice per tier — hot driver uses
// []Ingester, cold driver uses []ColdIngester. There is no mixed-type
// slice and no type assertion in the per-ledger loop.
//
// Concurrency:
//   - Ingest on a single Ingester instance is invoked serially across
//     ledgers (each ingester's own state is never accessed by two
//     goroutines).
//   - Distinct Ingester instances may be invoked concurrently within
//     the same ledger when --parallel is set. Each instance reads its
//     own state + the (read-only) Ledger value; collectors held by
//     different ingesters are disjoint.
//
// Lifecycle:
//   - Constructors open the underlying store/writer. For cold tiers
//     that means opening the per-chunk writer.
//   - Ingest is called once per ledger in the chunk range.
//   - Close is ALWAYS deferred. It is idempotent and drops the store
//     handle. For cold ingesters, when Finalize never ran (the
//     failure path), the underlying packfile writer's Close removes
//     any partial file.
type Ingester interface {
	Ingest(ctx context.Context, l Ledger) error
	io.Closer
}

// ColdIngester adds the cold-tier commit step: Commit (ledger) /
// Finish + WriteColdIndex (events) / sort + write .bin (txhash).
//
// Finalize is called explicitly by the driver on the success path with
// an error check; never deferred. Errors from Finalize indicate the
// chunk's cold artifact did not durably land and must be re-ingested.
//
// Hot ingesters intentionally do NOT implement ColdIngester — the
// distinction is enforced at compile time by the per-driver slice type.
type ColdIngester interface {
	Ingester
	Finalize(ctx context.Context) error
}
