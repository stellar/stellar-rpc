// Package stores hosts shared per-domain store packages (ledger,
// txhash, metastore) and the cross-cutting error sentinels they
// emit.
package stores

import "errors"

// ErrNotFound — read-side miss sentinel. Returned by Get methods
// when a key is absent. Per-domain stores wrap this verbatim;
// callers detect via errors.Is(err, stores.ErrNotFound).
var ErrNotFound = errors.New("stores: key not found")

// ErrStoreClosed — closed-store lifecycle sentinel for stores
// whose backing primitive is not pkg/rocksdb (currently the
// packfile-backed cold ledger store). Returned at the L2 boundary
// by public methods on a closed store so callers depend only on
// pkg/stores sentinels and never see packfile.* directly.
//
// Rocksdb-backed stores (HotStore, TxHashStore, MetaStore)
// propagate rocksdb.ErrStoreClosed verbatim — their lifecycle is
// the rocksdb wrapper's lifecycle.
var ErrStoreClosed = errors.New("stores: store is closed")
