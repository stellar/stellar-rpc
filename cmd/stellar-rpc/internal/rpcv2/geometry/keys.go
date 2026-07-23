package geometry

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
)

// State is an artifact key's lifecycle value, shared with identical meaning by
// per-chunk artifacts and index coverages. The empty State (key absent) means
// "neither file nor in-progress write exists".
type State string

const (
	// StateFreezing — the immutable file is being written. Set BEFORE any I/O
	// (mark-then-write), so a crash mid-write is detectable from the key alone
	// and every on-disk file is reachable from a key.
	StateFreezing State = "freezing"
	// StateFrozen — file and dirent are fsynced and durable. Trusted blindly by
	// readers, the resolver, and buildTxhashIndex's precondition.
	StateFrozen State = "frozen"
	// StatePruning — file queued for removal, may or may not still be on disk.
	// A sweep finishes the unlink, then deletes the key.
	StatePruning State = "pruning"
)

// HotState is a hot-DB key's value. One key per chunk brackets the chunk's hot
// RocksDB directory; the column families inside carry no individual key.
type HotState string

const (
	// HotTransient — a dir operation is in flight (create/delete) or recovery
	// demoted the key. Recovery is identical either way: open wipes+recreates,
	// discard re-runs the scan.
	HotTransient HotState = "transient"
	// HotReady — the dir exists and is usable.
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
	// KindTxHash is the per-chunk sorted txhash run (.bin). Transient — removed
	// at index finalization.
	KindTxHash Kind = "txhash"
)

// allKinds is the canonical iteration order for per-chunk artifact kinds.
//
//nolint:gochecknoglobals // immutable kind registry, single source of truth
var allKinds = []Kind{KindLedgers, KindEvents, KindTxHash}

// AllKinds returns the per-chunk artifact kinds in canonical order.
func AllKinds() []Kind { return append([]Kind(nil), allKinds...) }

// TxHashIndexID identifies a tx-hash index: a contiguous run of
// chunks_per_txhash_index chunks. Distinct type from chunk.ID (both uint32) so
// index ids and chunk ids never silently interchange.
type TxHashIndexID uint32

// String formats an index id as zero-padded 8-digit decimal — same width as
// chunk ids, matching the {idx:08d} segment in keys and paths.
func (i TxHashIndexID) String() string { return fmt.Sprintf("%08d", uint32(i)) }

// ---------------------------------------------------------------------------
// Key prefixes and constructors — the single source of truth for the
// key<->path bijection (paths.go holds the inverse).
// ---------------------------------------------------------------------------

const (
	ChunkPrefix       = "chunk:"
	HotChunkPrefix    = "hot:chunk:"
	TxHashIndexPrefix = "index:"

	// ConfigEarliestLedger is the sole config pin key. (chunks_per_txhash_index is
	// the fixed ChunksPerTxhashIndex constant, not a pin.)
	ConfigEarliestLedger = "config:earliest_ledger"
)

// ChunkKey returns the per-chunk artifact key chunk:{chunk:08d}:{kind}.
func ChunkKey(c chunk.ID, kind Kind) string {
	return ChunkPrefix + c.String() + ":" + string(kind)
}

// HotChunkKey returns the hot-DB key hot:chunk:{chunk:08d}. One key per chunk
// brackets the hot RocksDB dir; the value is a HotState.
func HotChunkKey(c chunk.ID) string {
	return HotChunkPrefix + c.String()
}

// TxHashIndexKey returns the index coverage key index:{idx:08d}:{lo:08d}:{hi:08d}.
// The coverage [lo, hi] lives in the key NAME; the value is pure lifecycle
// state. lo > hi is a programmer error, surfaced loudly via panic.
func TxHashIndexKey(idx TxHashIndexID, lo, hi chunk.ID) string {
	if lo > hi {
		panic(fmt.Sprintf("TxHashIndexKey lo %s > hi %s", lo, hi))
	}
	return TxHashIndexPrefix + idx.String() + ":" + lo.String() + ":" + hi.String()
}

// TxHashIndexPrefixFor returns the scan prefix index:{idx:08d}: that enumerates
// all coverage keys of one index.
func TxHashIndexPrefixFor(idx TxHashIndexID) string {
	return TxHashIndexPrefix + idx.String() + ":"
}

// ---------------------------------------------------------------------------
// Key parsing — each parser is the reverse bijection of exactly one
// constructor above.
// ---------------------------------------------------------------------------

// TxHashIndexCoverage is one parsed index coverage key: the index, the covered
// chunk range [Lo, Hi], the full key string, and its lifecycle State.
type TxHashIndexCoverage struct {
	Index  TxHashIndexID
	Lo, Hi chunk.ID
	Key    string
	State  State
}

// ParseChunkKey decodes chunk:{chunk:08d}:{kind}. ok is false for any key that
// is not a well-formed per-chunk artifact key.
func ParseChunkKey(key string) (chunk.ID, Kind, bool) {
	rest, found := strings.CutPrefix(key, ChunkPrefix)
	if !found {
		return 0, "", false
	}
	id, suffix, found := strings.Cut(rest, ":")
	if !found {
		return 0, "", false
	}
	n, err := ParsePadded(id)
	if err != nil {
		return 0, "", false
	}
	kind := Kind(suffix)
	if !isKnownKind(kind) {
		return 0, "", false
	}
	return chunk.ID(n), kind, true
}

// ParseHotChunkKey decodes hot:chunk:{chunk:08d}. ok is false for any key that
// is not a well-formed hot-chunk key.
func ParseHotChunkKey(key string) (chunk.ID, bool) {
	rest, found := strings.CutPrefix(key, HotChunkPrefix)
	if !found {
		return 0, false
	}
	n, err := ParsePadded(rest)
	if err != nil {
		return 0, false
	}
	return chunk.ID(n), true
}

// ParseTxHashIndexKey decodes index:{idx:08d}:{lo:08d}:{hi:08d}. State is not part
// of the key; callers fill TxHashIndexCoverage.State from the scanned value.
func ParseTxHashIndexKey(key string) (TxHashIndexCoverage, bool) {
	rest, found := strings.CutPrefix(key, TxHashIndexPrefix)
	if !found {
		return TxHashIndexCoverage{}, false
	}
	parts := strings.Split(rest, ":")
	if len(parts) != 3 {
		return TxHashIndexCoverage{}, false
	}
	w, err := ParsePadded(parts[0])
	if err != nil {
		return TxHashIndexCoverage{}, false
	}
	lo, err := ParsePadded(parts[1])
	if err != nil {
		return TxHashIndexCoverage{}, false
	}
	hi, err := ParsePadded(parts[2])
	if err != nil {
		return TxHashIndexCoverage{}, false
	}
	if lo > hi {
		return TxHashIndexCoverage{}, false
	}
	return TxHashIndexCoverage{
		Index: TxHashIndexID(w),
		Lo:    chunk.ID(lo),
		Hi:    chunk.ID(hi),
		Key:   key,
	}, true
}

// ParsePadded parses an 8-digit zero-padded decimal segment as produced by
// chunk.ID.String()/TxHashIndexID.String(). The fixed 8-char width is enforced (not
// silently accepted) so the bijection stays exact.
func ParsePadded(s string) (uint32, error) {
	if len(s) != 8 {
		return 0, fmt.Errorf("%q is not an 8-digit padded id", s)
	}
	n, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("%q is not numeric: %w", s, err)
	}
	return uint32(n), nil
}

func isKnownKind(k Kind) bool {
	return slices.Contains(allKinds, k)
}
