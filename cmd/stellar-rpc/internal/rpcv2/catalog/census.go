package catalog

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
)

// ErrForeignCatalog marks a catalog holding entries outside this binary's
// vocabulary. The daemon refuses to start on it: the entries were either
// written by a newer stellar-rpc (whose formats this binary cannot read, and
// whose artifacts the resolver would otherwise overwrite) or corrupted.
var ErrForeignCatalog = errors.New("catalog: entries unknown to this version of stellar-rpc")

// censusMaxDetailed caps how many offending entries the refusal error spells
// out; the total count is always reported.
const censusMaxDetailed = 10

// census validates every key and value in the store against the exact
// vocabulary this binary writes. It runs once inside Open, before the secret
// mint, so a foreign catalog is refused before anything writes into it. The
// scan is read-only and touches only the catalog (never a hot DB or a file).
//
// Everything this binary durably writes must parse here; the whitelist spans
// the geometry key families AND the meta/catalog-secret key, and the value
// checks are the census's own exact-token comparisons (State/HotState reads
// elsewhere are raw casts, not validators).
func (c *Catalog) census() error {
	var (
		offenders []string
		total     int
	)
	flag := func(key, detail string) {
		total++
		if len(offenders) < censusMaxDetailed {
			offenders = append(offenders, fmt.Sprintf("%q: %s", key, detail))
		}
	}

	for e, err := range c.prefixScan("") {
		if err != nil {
			return fmt.Errorf("catalog: census scan: %w", err)
		}
		switch e.Key {
		case geometry.ConfigEarliestLedger:
			if !isCanonicalUint32(e.Value) {
				flag(e.Key, fmt.Sprintf("value %q is not a canonical decimal uint32", e.Value))
			}
		case catalogSecretStoreKey:
			// Never print the secret: it is the live index-blinding key.
			if len(e.Value) != catalogSecretLen {
				flag(e.Key, fmt.Sprintf("value is %d bytes, want %d (value redacted)", len(e.Value), catalogSecretLen))
			}
		default:
			if detail, ok := censusArtifactEntry(e.Key, e.Value); !ok {
				flag(e.Key, detail)
			}
		}
	}

	if total == 0 {
		return nil
	}
	return fmt.Errorf("%w — either written by a newer stellar-rpc (deploy that version or newer) "+
		"or corrupted; %d offending entr%s: %s",
		ErrForeignCatalog, total, plural(total), strings.Join(offenders, "; "))
}

// censusArtifactEntry validates one non-config entry against the three state
// key families. ok=false returns the reason.
func censusArtifactEntry(key, value string) (string, bool) {
	switch {
	case strings.HasPrefix(key, geometry.HotChunkPrefix):
		if _, ok := geometry.ParseHotChunkKey(key); !ok {
			return "malformed hot-chunk key", false
		}
		if !isKnownHotState(value) {
			return fmt.Sprintf("unknown hot state %q", value), false
		}
	case strings.HasPrefix(key, geometry.ChunkPrefix):
		if _, _, ok := geometry.ParseChunkKey(key); !ok {
			return "malformed per-chunk artifact key", false
		}
		if !isKnownState(value) {
			return fmt.Sprintf("unknown artifact state %q", value), false
		}
	case strings.HasPrefix(key, geometry.TxHashIndexPrefix):
		if _, ok := geometry.ParseTxHashIndexKey(key); !ok {
			return "malformed index coverage key", false
		}
		if !isKnownState(value) {
			return fmt.Sprintf("unknown artifact state %q", value), false
		}
	default:
		return fmt.Sprintf("unknown key (value %q)", value), false
	}
	return "", true
}

func isKnownState(v string) bool {
	s := geometry.State(v)
	return s == geometry.StateFreezing || s == geometry.StateFrozen || s == geometry.StatePruning
}

func isKnownHotState(v string) bool {
	s := geometry.HotState(v)
	return s == geometry.HotTransient || s == geometry.HotReady
}

// isCanonicalUint32 reports whether v round-trips through ParseUint and
// FormatUint byte-identically — the exact form PinEarliestLedger writes.
func isCanonicalUint32(v string) bool {
	n, err := strconv.ParseUint(v, 10, 32)
	if err != nil {
		return false
	}
	return strconv.FormatUint(n, 10) == v
}

func plural(n int) string {
	if n == 1 {
		return "y"
	}
	return "ies"
}
