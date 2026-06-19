package streaming

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// State is an artifact key's lifecycle value. Per-chunk artifacts and index
// coverages share the same three states with the same meanings; the empty
// State (key absent) means "neither file nor in-progress write exists".
type State string

const (
	// StateFreezing — the immutable file is being written. Set BEFORE any I/O
	// (the mark-then-write rule), so a crash mid-write is detectable from the
	// key alone and every file on disk is reachable from a key.
	StateFreezing State = "freezing"
	// StateFrozen — the file and its dirent are fsynced and durable. Truth:
	// readers, the resolver, and buildTxhashIndex's precondition trust it
	// blindly.
	StateFrozen State = "frozen"
	// StatePruning — the file is queued for removal; it may or may not still be
	// on disk. A sweep finishes the unlink and then deletes the key.
	StatePruning State = "pruning"
)

// HotState is a hot-DB key's value. One key per chunk brackets the chunk's
// hot RocksDB directory; the column families inside carry no individual key.
type HotState string

const (
	// HotTransient — a directory operation is in flight (creation or
	// deletion), or a recovery demoted the key. The recovery is identical
	// either way: the open path wipes and recreates, the discard scan re-runs.
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
	// KindEvents is the events cold segment (three files per chunk).
	KindEvents Kind = "events"
	// KindTxHash is the per-chunk sorted txhash run (.bin). Transient —
	// removed at window finalization.
	KindTxHash Kind = "txhash"
)

// allKinds is the canonical iteration order for per-chunk artifact kinds.
//
//nolint:gochecknoglobals // immutable kind registry, single source of truth
var allKinds = []Kind{KindLedgers, KindEvents, KindTxHash}

// AllKinds returns the per-chunk artifact kinds in canonical order.
func AllKinds() []Kind { return append([]Kind(nil), allKinds...) }

// WindowID identifies a txhash index window: a contiguous run of
// chunks_per_txhash_index chunks. Distinct type from chunk.ID so window ids
// and chunk ids never silently interchange — both are uint32.
type WindowID uint32

// String formats a window id as zero-padded 8-digit decimal — the same width
// chunk ids use, matching the {window:08d} segment in keys and paths.
func (w WindowID) String() string { return fmt.Sprintf("%08d", uint32(w)) }

// ---------------------------------------------------------------------------
// Key prefixes and constructors. Every key is built here so the key<->path
// bijection has exactly one source of truth (see paths.go for the inverse).
// ---------------------------------------------------------------------------

const (
	chunkPrefix = "chunk:"
	hotPrefix   = "hot:chunk:"
	indexPrefix = "index:"

	// Config pins.
	configEarliestLedger     = "config:earliest_ledger"
	configChunksPerTxhashIdx = "config:chunks_per_txhash_index"
)

// chunkKey returns the per-chunk artifact key chunk:{chunk:08d}:{kind}.
func chunkKey(c chunk.ID, kind Kind) string {
	return chunkPrefix + c.String() + ":" + string(kind)
}

// hotChunkKey returns the hot-DB key hot:chunk:{chunk:08d}.
func hotChunkKey(c chunk.ID) string {
	return hotPrefix + c.String()
}

// indexKey returns the index coverage key index:{window:08d}:{lo:08d}:{hi:08d}.
// The COVERAGE [lo, hi] lives in the key NAME; the value is pure lifecycle
// state. lo > hi is a programmer error worth surfacing loudly.
func indexKey(w WindowID, lo, hi chunk.ID) string {
	if lo > hi {
		panic(fmt.Sprintf("streaming: indexKey lo %s > hi %s", lo, hi))
	}
	return indexPrefix + w.String() + ":" + lo.String() + ":" + hi.String()
}

// indexWindowPrefix returns the scan prefix for all coverage keys of one
// window: index:{window:08d}:. Used to enumerate a window's coverages.
func indexWindowPrefix(w WindowID) string {
	return indexPrefix + w.String() + ":"
}

// ---------------------------------------------------------------------------
// Key parsing. The inverse of the constructors above; every parser is the
// reverse bijection of exactly one constructor.
// ---------------------------------------------------------------------------

// IndexCoverage is one parsed index coverage key: the window, the covered
// chunk range [Lo, Hi], the full key string, and its lifecycle State.
type IndexCoverage struct {
	Window WindowID
	Lo, Hi chunk.ID
	Key    string
	State  State
}

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

// parseIndexKey decodes index:{window:08d}:{lo:08d}:{hi:08d}. The value is not
// part of the key; callers fill IndexCoverage.State from the scanned value.
func parseIndexKey(key string) (IndexCoverage, bool) {
	rest, found := strings.CutPrefix(key, indexPrefix)
	if !found {
		return IndexCoverage{}, false
	}
	parts := strings.Split(rest, ":")
	if len(parts) != 3 {
		return IndexCoverage{}, false
	}
	w, err := parsePadded(parts[0])
	if err != nil {
		return IndexCoverage{}, false
	}
	lo, err := parsePadded(parts[1])
	if err != nil {
		return IndexCoverage{}, false
	}
	hi, err := parsePadded(parts[2])
	if err != nil {
		return IndexCoverage{}, false
	}
	if lo > hi {
		return IndexCoverage{}, false
	}
	return IndexCoverage{
		Window: WindowID(w),
		Lo:     chunk.ID(lo),
		Hi:     chunk.ID(hi),
		Key:    key,
	}, true
}

// parsePadded parses an 8-digit zero-padded decimal segment as produced by
// chunk.ID.String()/WindowID.String(). It enforces the fixed 8-char width so
// the bijection is exact — a non-padded or wrong-width segment is rejected,
// not silently accepted.
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
