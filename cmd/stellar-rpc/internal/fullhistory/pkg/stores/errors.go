// Package stores hosts shared per-domain store packages (ledger,
// txhash, metastore) and the cross-cutting error sentinels they
// emit. Lifecycle errors (closed store, invalid config) come from
// the Layer-1 wrapper at pkg/rocksdb and are propagated directly by
// the per-domain stores — no separate stores-level sentinel for
// those.
package stores

import "errors"

// ErrNotFound — read-side miss sentinel. Returned by Get methods
// when a key is absent. Per-domain stores wrap this verbatim;
// callers detect via errors.Is(err, stores.ErrNotFound).
var ErrNotFound = errors.New("stores: key not found")
