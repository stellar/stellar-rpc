package stores

import "errors"

// ErrNotFound is the canonical "key not present" sentinel every store
// in this package returns from its read-side methods (TxHashStore.Get,
// MetaStore.HasIndexEntry's "no, not present" path collapsed onto
// (zero, error), and similar).
// Callers detect a miss with errors.Is(err, stores.ErrNotFound).
var ErrNotFound = errors.New("stores: key not found")

// ErrStoreClosed is returned by any store method called after the
// caller's Close() has run.
// Concrete impls translate their backend-specific closed sentinel
// (e.g., rocksdb.ErrStoreClosed) into this one at the interface
// boundary so consumers depending only on this package can detect
// the condition without importing the backend.
var ErrStoreClosed = errors.New("stores: store is closed")
