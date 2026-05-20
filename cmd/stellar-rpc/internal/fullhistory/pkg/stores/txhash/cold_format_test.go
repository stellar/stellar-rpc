package txhash

import (
	"context"
	"math/rand/v2"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
)

// randHash returns a 32-byte hash drawn from r. Test data uses true
// random hashes (matching streamhash's own test patterns) — structured
// hashes can produce correlated 16-byte prefixes that defeat the
// fingerprint check and confuse this test.
func randHash(r *rand.Rand) [32]byte {
	var h [32]byte
	for i := range h {
		h[i] = byte(r.UintN(256))
	}
	return h
}

// testRNG returns a deterministic rand source. Tests using this RNG
// are reproducible even though the hashes look random.
func testRNG(seed uint64) *rand.Rand {
	return rand.New(rand.NewPCG(seed, seed*7919+1))
}

// fixtureEntry pairs a generated hash with its assigned ledgerSeq.
type fixtureEntry struct {
	hash [32]byte
	seq  uint32
}

// buildColdFixture builds a populated cold index at a tempdir path
// and returns (path, entries). Used by both writer and reader tests
// so the path/format invariants stay in one place.
func buildColdFixture(t *testing.T, n int) (path string, entries []fixtureEntry) {
	t.Helper()
	path = filepath.Join(t.TempDir(), ColdIndexName)
	w, err := NewColdIndexWriter(context.Background(), path, uint64(n))
	require.NoError(t, err)

	r := testRNG(uint64(n) | 0xfeed)
	entries = make([]fixtureEntry, 0, n)
	seen := make(map[[32]byte]struct{}, n)
	for len(entries) < n {
		h := randHash(r)
		if _, dup := seen[h]; dup {
			continue // streamhash rejects duplicates; resample
		}
		seen[h] = struct{}{}
		// Pick a ledgerSeq that's distinct from the index so a bug
		// swapping the two would surface as a value mismatch.
		seq := uint32(2 + len(entries)*7)
		require.NoError(t, w.AddEntry(h, seq))
		entries = append(entries, fixtureEntry{hash: h, seq: seq})
	}
	require.NoError(t, w.Commit())
	require.NoError(t, w.Close())
	return path, entries
}

func TestColdBuilder_EmptyTotalErrors(t *testing.T) {
	_, err := newColdBuilder(context.Background(), filepath.Join(t.TempDir(), "x.idx"), 0)
	assert.ErrorIs(t, err, ErrEmptyBuildSet)
}

func TestColdBuilder_OptionsRoundTrip(t *testing.T) {
	// Build a tiny index and reopen it; verify Lookup returns the
	// recorded ledgerSeq verbatim. This is the format contract: the
	// uint32 payload survives the streamhash uint64 packing.
	path, entries := buildColdFixture(t, 16)

	m, err := openColdMPHF(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	for _, e := range entries {
		got, err := m.Lookup(e.hash)
		require.NoError(t, err)
		assert.Equal(t, e.seq, got, "ledgerSeq for hash %x must round-trip", e.hash[:8])
	}
}

func TestColdLookup_UnseenKeyReturnsNotFound(t *testing.T) {
	// streamhash's WithFingerprint(4) is what makes this work — without
	// it, an unseen key would map to a slot in [0, N) and return a
	// bogus payload. With it, the fingerprint check inside QueryPayload
	// rejects unseen keys directly. ~1 in 2^32 unseen keys may slip
	// past the fingerprint; we don't try to test that residual rate
	// here. Note: we tolerate at most one false positive across 200
	// probes — anything more indicates the fingerprint isn't actually
	// being checked.
	const n = 64
	path, entries := buildColdFixture(t, n)

	m, err := openColdMPHF(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	seen := make(map[[32]byte]struct{}, n)
	for _, e := range entries {
		seen[e.hash] = struct{}{}
	}

	r := testRNG(0xdeadbeef)
	const probes = 200
	notFound := 0
	for i := 0; i < probes; i++ {
		unseen := randHash(r)
		if _, dup := seen[unseen]; dup {
			continue
		}
		_, err := m.Lookup(unseen)
		if err != nil {
			assert.ErrorIs(t, err, stores.ErrNotFound,
				"unseen key %d: expected ErrNotFound", i)
			notFound++
		}
	}
	// With a 4-byte fingerprint, residual FPR ≈ 1/2^32 — across 200
	// probes the expected pass-through is far below 1. Asserting at
	// least 95% rejection catches a fingerprint-not-actually-applied
	// regression without flaking on the rare residual collision.
	assert.GreaterOrEqual(t, notFound, probes*95/100,
		"WithFingerprint(4) should reject virtually all unseen keys; got %d/%d", notFound, probes)
}

func TestColdMPHF_OpenNonExistentErrors(t *testing.T) {
	_, err := openColdMPHF(filepath.Join(t.TempDir(), "does-not-exist.idx"))
	assert.Error(t, err)
}

func TestColdMPHF_CloseIsIdempotent(t *testing.T) {
	path, _ := buildColdFixture(t, 4)
	m, err := openColdMPHF(path)
	require.NoError(t, err)

	require.NoError(t, m.Close())
	require.NoError(t, m.Close(), "second Close must be a no-op")
}

func TestColdMPHF_LookupAfterCloseReturnsClosed(t *testing.T) {
	path, entries := buildColdFixture(t, 4)
	m, err := openColdMPHF(path)
	require.NoError(t, err)
	require.NoError(t, m.Close())

	_, err = m.Lookup(entries[0].hash)
	assert.ErrorIs(t, err, stores.ErrStoreClosed)
}
