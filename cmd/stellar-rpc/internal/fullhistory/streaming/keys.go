package streaming

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// State is an artifact key's lifecycle value. The empty State (key absent)
// means "neither file nor in-progress write exists".
type State string

const (
	// StateFreezing — file being written; set before any I/O. See the one-write
	// protocol in catalog_protocol.go.
	StateFreezing State = "freezing"
	// StateFrozen — file and dirent fsynced and durable; readers and the
	// resolver trust it blindly.
	StateFrozen State = "frozen"
	// StatePruning — file queued for removal (may still be on disk); a sweep
	// finishes the unlink, then deletes the key. See catalog_sweep.go.
	StatePruning State = "pruning"
)

// HotState is a hot-DB key's value. One key per chunk brackets the chunk's
// hot RocksDB directory; the column families inside carry no individual key.
type HotState string

const (
	// HotTransient — a directory operation is in flight (creation or deletion),
	// or recovery demoted the key. Recovery is identical either way: the open
	// path wipes and recreates, the discard scan re-runs.
	HotTransient HotState = "transient"
	// HotReady — the dir exists and is usable for reads and writes.
	HotReady HotState = "ready"
)

// Kind is a per-chunk artifact kind. Each maps to one meta-store key suffix
// and one set of on-disk files.
type Kind string

const (
	// KindLedgers is the ledger pack file (.pack).
	KindLedgers Kind = "ledgers"
)

// allKinds is the canonical iteration order for per-chunk artifact kinds.
//
//nolint:gochecknoglobals // immutable kind registry, single source of truth
var allKinds = []Kind{KindLedgers}

// AllKinds returns the per-chunk artifact kinds in canonical order.
func AllKinds() []Kind { return append([]Kind(nil), allKinds...) }

// Key prefixes and constructors. Every key is built here so the key↔path
// bijection has one source of truth (paths.go holds the inverse).

const (
	chunkPrefix = "chunk:"
	hotPrefix   = "hot:chunk:"

	// Config pins.
	configEarliestLedger = "config:earliest_ledger"
)

// chunkKey returns the per-chunk artifact key chunk:{chunk:08d}:{kind}.
func chunkKey(c chunk.ID, kind Kind) string {
	return chunkPrefix + c.String() + ":" + string(kind)
}

// hotChunkKey returns the hot-DB key hot:chunk:{chunk:08d}.
func hotChunkKey(c chunk.ID) string {
	return hotPrefix + c.String()
}

// Key parsing — the inverse of the constructors above; one parser per
// constructor.

// parseChunkKey decodes chunk:{chunk:08d}:{kind}. ok is false for any key that
// is not a well-formed per-chunk artifact key.
func parseChunkKey(key string) (chunk.ID, Kind, bool) {
	rest, found := strings.CutPrefix(key, chunkPrefix)
	if !found {
		return 0, "", false
	}
	id, suffix, found := strings.Cut(rest, ":")
	if !found {
		return 0, "", false
	}
	n, err := parsePadded(id)
	if err != nil {
		return 0, "", false
	}
	kind := Kind(suffix)
	if !isKnownKind(kind) {
		return 0, "", false
	}
	return chunk.ID(n), kind, true
}

// parseHotChunkKey decodes hot:chunk:{chunk:08d}.
func parseHotChunkKey(key string) (chunk.ID, bool) {
	rest, found := strings.CutPrefix(key, hotPrefix)
	if !found {
		return 0, false
	}
	n, err := parsePadded(rest)
	if err != nil {
		return 0, false
	}
	return chunk.ID(n), true
}

// parsePadded parses an 8-digit zero-padded decimal segment as produced by
// chunk.ID.String(). It enforces the fixed 8-char width so the bijection is
// exact — a non-padded or wrong-width segment is rejected, not silently
// accepted.
func parsePadded(s string) (uint32, error) {
	if len(s) != 8 {
		return 0, fmt.Errorf("streaming: %q is not an 8-digit padded id", s)
	}
	n, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("streaming: %q is not numeric: %w", s, err)
	}
	return uint32(n), nil
}

func isKnownKind(k Kind) bool {
	return slices.Contains(allKinds, k)
}
