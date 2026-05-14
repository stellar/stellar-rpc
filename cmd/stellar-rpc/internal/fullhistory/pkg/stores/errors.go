// Package stores declares the typed Layer-2 store contracts.
// Concrete impls live in pkg/rocksdb. Error sentinels here are
// detected via errors.Is by consumers.
package stores

import "errors"

// ErrNotFound — read-side miss sentinel.
var ErrNotFound = errors.New("stores: key not found")

// ErrStoreClosed — any method called after Close.
var ErrStoreClosed = errors.New("stores: store is closed")
