package config

// This file declares the two interfaces that ValidateAgainstConfigStore and
// ValidateAgainstBSB depend on, plus in-memory stub implementations used by
// the subcommand until slice #689 (Layer-2 metastore facade) and slice #688
// (BSB source wrapper) land their real implementations.
//
// The stubs are NOT production-grade — they exist solely so the subcommand
// can wire the full validation pipeline end-to-end today without pulling in
// pkg/rocksdb or the Stellar SDK's BufferedStorageBackend. MEMORY.md pins
// the decision: "CHUNKS_PER_TXHASH_INDEX immutability check goes through a
// ConfigStore interface with a no-op stub impl in this slice. Do NOT import
// pkg/rocksdb."

// ConfigStore is the narrow interface that ValidateAgainstConfigStore
// consumes. Exactly two methods: Get (returning value + found + err so the
// "key absent" case is distinguishable from a real read error) and Put
// (used on first-run to persist the CHUNKS_PER_TXHASH_INDEX seed value).
//
// Slice #689's metastore facade will implement this over the real RocksDB
// key `config:chunks_per_txhash_index`.
type ConfigStore interface {
	// Get retrieves the value for a key. found=false means the key is
	// absent (first-run semantics); err != nil means the store itself
	// failed to read (propagate as a hard abort).
	Get(key string) (value string, found bool, err error)

	// Put writes a key/value pair. Errors propagate as a hard abort —
	// a subsequent run will retry the same seed value.
	Put(key, value string) error
}

// NewStubConfigStore returns an in-memory ConfigStore with no persisted
// state — suitable for the subcommand wiring in slice #684 where the real
// RocksDB-backed store lands in #689. Every subcommand invocation sees an
// empty store, so the CHUNKS_PER_TXHASH_INDEX first-run branch always
// fires. Callers that want genuine persistence must wait for #689.
func NewStubConfigStore() ConfigStore {
	return &stubConfigStore{data: map[string]string{}}
}

// stubConfigStore is the trivial map-backed impl. Not thread-safe and not
// persisted across process restarts — intentional, since the real impl
// lands in #689 with proper locking and fsync semantics.
type stubConfigStore struct {
	data map[string]string
}

// Get reports whether the key exists and returns its value. Never fails —
// the in-memory map cannot produce an I/O error.
func (s *stubConfigStore) Get(key string) (string, bool, error) {
	v, ok := s.data[key]
	return v, ok, nil
}

// Put stores the key/value pair. Never fails.
func (s *stubConfigStore) Put(key, value string) error {
	s.data[key] = value
	return nil
}

// BSBAvailabilityProbe is the narrow interface that ValidateAgainstBSB
// consumes. Exactly one method: MaxAvailableLedger returns the highest
// ledger present in the remote object store BSB points at. A real
// implementation (slice #688) probes GCS/S3; the stub here skips the check
// entirely by returning math.MaxUint32.
type BSBAvailabilityProbe interface {
	// MaxAvailableLedger returns the highest ledger currently retrievable
	// from the BSB backend.
	MaxAvailableLedger() (uint32, error)
}

// NewStubBSBAvailabilityProbe returns a probe that reports "everything is
// available" — effectively disabling the availability check until slice
// #688 lands the real BSB-backed probe.
func NewStubBSBAvailabilityProbe() BSBAvailabilityProbe {
	return &stubBSBAvailabilityProbe{}
}

// stubBSBAvailabilityProbe always returns math.MaxUint32, satisfying the
// "expanded end <= max available" check for any conceivable Stellar ledger.
type stubBSBAvailabilityProbe struct{}

// MaxAvailableLedger returns the max uint32 so the availability check is
// effectively a no-op in this slice.
func (stubBSBAvailabilityProbe) MaxAvailableLedger() (uint32, error) {
	// math.MaxUint32 — Stellar ledger sequences are uint32 so this is an
	// unreachable cap in practice.
	return 0xFFFFFFFF, nil
}
