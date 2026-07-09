// Package stores hosts shared per-domain store packages (ledger,
// txhash, metastore) and the cross-cutting error sentinels they
// emit. Per-domain stores translate their backing primitive's
// errors (storage/rocksdb, internal/packfile, os) into these sentinels at
// their public-method boundaries, so callers depend only on
// storage/stores sentinels regardless of which backend served the
// call.
package stores

import "errors"

// ErrNotFound — read-side miss sentinel. Returned by Get methods
// when a key is absent. Per-domain stores wrap this verbatim;
// callers detect via errors.Is(err, stores.ErrNotFound).
var ErrNotFound = errors.New("stores: key not found")

// ErrStoreClosed — closed-store lifecycle sentinel. Returned by
// every public method on a store after Close. Per-domain stores
// translate their backing primitive's closed error
// (rocksdb.ErrStoreClosed, packfile.ErrWriterClosed, os.ErrClosed)
// into this at the L2 boundary.
var ErrStoreClosed = errors.New("stores: store is closed")

// ErrInvalidConfig — constructor input-validation sentinel.
// Returned by Newxxx constructors when a required input is missing
// or invalid (e.g., empty path, nil dependency).
var ErrInvalidConfig = errors.New("stores: invalid config")

// ErrCorrupt — data-integrity sentinel. Returned when the backing
// primitive reports corruption (packfile trailer/index, content
// hash, or decompression failure on read). Per-domain stores
// translate the underlying corruption signal into this at the L2
// boundary.
var ErrCorrupt = errors.New("stores: data corrupt")

// ErrOutOfRange — range/bounds sentinel. Returned when a requested
// sequence or [start, end] range falls outside the store's known
// coverage, or when the range is otherwise invalid (e.g., start
// > end). Distinct from ErrNotFound — ErrNotFound is a sparse-key
// miss within the store's coverage; ErrOutOfRange signals "you
// asked for something outside what this store holds."
var ErrOutOfRange = errors.New("stores: out of range")
