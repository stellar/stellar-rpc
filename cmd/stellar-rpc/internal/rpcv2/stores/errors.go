// Package stores hosts shared per-domain store packages (ledger,
// txhash, event, hotchunk), the RAM-only primitives more than one of
// them routes through (bloom), and the cross-cutting error sentinels
// they emit. Per-domain stores translate their backing primitive's
// errors (rpcv2/rocksdb, rpcv2/packfile, os) into these sentinels at
// their public-method boundaries, so callers depend only on
// stores sentinels regardless of which backend served the
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

// ErrNoTable — the accelerator-absent sentinel. Returned by a ledger store's
// table read when the ledger HAS no transaction span table: a store that
// predates the table, a ledger whose build refused one, a cold record too
// small to carry one. It is never a failure — the caller reads the ledger
// whole and walks it, which is what every reader did before tables existed.
//
// A table that is there and cannot be used is NOT this: it is an error naming
// the ledger and the reason, because a read that quietly walked around it
// would hide a bad artifact from the operator.
var ErrNoTable = errors.New("stores: no transaction span table")

// ErrOutOfRange — range/bounds sentinel. Returned when a requested
// sequence or [start, end] range falls outside the store's known
// coverage, or when the range is otherwise invalid (e.g., start
// > end). Distinct from ErrNotFound — ErrNotFound is a sparse-key
// miss within the store's coverage; ErrOutOfRange signals "you
// asked for something outside what this store holds."
var ErrOutOfRange = errors.New("stores: out of range")
