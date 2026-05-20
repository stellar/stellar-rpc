package txhash

import (
	"bytes"
	"context"
	"math/rand/v2"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tamirms/streamhash"

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

// fixtureMinLedger is the MinLedger anchor used by buildColdFixture.
// Picked >0 so the offset math is exercised (a bug returning the raw
// payload as the absolute seq would surface as a 100-off mismatch).
const fixtureMinLedger uint32 = 100

// buildColdFixture builds a populated cold index at a tempdir path by
// driving streamhash.NewSortedBuilder directly with ColdBuildOptions,
// then returns (path, entries). Keys are pre-sorted (streamhash's
// sorted-mode requirement); seqs are assigned distinct from the slot
// index so a bug swapping the two surfaces as a value mismatch.
func buildColdFixture(t *testing.T, n int) (string, []fixtureEntry) {
	t.Helper()
	path := filepath.Join(t.TempDir(), ColdIndexName)

	// Generate n unique hashes.
	r := testRNG(uint64(n) | 0xfeed)
	entries := make([]fixtureEntry, 0, n)
	seen := make(map[[32]byte]struct{}, n)
	for len(entries) < n {
		h := randHash(r)
		if _, dup := seen[h]; dup {
			continue
		}
		seen[h] = struct{}{}
		entries = append(entries, fixtureEntry{
			hash: h,
			seq:  fixtureMinLedger + uint32(len(entries)*7),
		})
	}

	// streamhash sorted mode requires keys ascending by the
	// big-endian 16-byte prefix; sorting full hashes lex-byte is
	// equivalent for our purposes.
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].hash[:16], entries[j].hash[:16]) < 0
	})

	opts := ColdBuildOptions(fixtureMinLedger)
	sb, err := streamhash.NewSortedBuilder(context.Background(), path, uint64(n), opts...)
	require.NoError(t, err)
	for _, e := range entries {
		payload := uint64(e.seq - fixtureMinLedger)
		require.NoError(t, sb.AddKey(e.hash[:], payload))
	}
	require.NoError(t, sb.Finish())
	return path, entries
}

func TestEncodeParseMinLedger_RoundTrip(t *testing.T) {
	for _, ml := range []uint32{0, 1, 100, 12345, 0xFFFFFFFF} {
		got, err := ParseMinLedger(EncodeMinLedger(ml))
		require.NoError(t, err)
		assert.Equal(t, ml, got)
	}
}

func TestParseMinLedger_WrongSizeErrors(t *testing.T) {
	for _, sz := range []int{0, 1, 3, 5, 8} {
		_, err := ParseMinLedger(make([]byte, sz))
		assert.ErrorIs(t, err, ErrInvalidMetadata, "size %d should error", sz)
	}
}

func TestColdBuildOptions_RoundTrip(t *testing.T) {
	// Build a tiny index with ColdBuildOptions and verify the reader
	// recovers (a) the MinLedger anchor and (b) the absolute seq for
	// every entry. This is the format contract end-to-end.
	path, entries := buildColdFixture(t, 16)

	m, err := openColdMPHF(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.close() })

	assert.Equal(t, fixtureMinLedger, m.minLedger, "MinLedger must survive UserMetadata round-trip")

	for _, e := range entries {
		got, err := m.lookup(e.hash)
		require.NoError(t, err)
		assert.Equal(t, e.seq, got, "absolute ledgerSeq for hash %x must round-trip", e.hash[:8])
	}
}

func TestColdLookup_UnseenKeyReturnsNotFound(t *testing.T) {
	// With a 1-byte fingerprint, the residual false-positive rate for
	// unseen keys is ~1/256. Across 1000 probes the expected FP count
	// is ~4 (std dev ≈ 2). Requiring at least 95% rejection gives
	// generous headroom and still catches a regression where the
	// fingerprint isn't checked at all.
	const n = 64
	path, entries := buildColdFixture(t, n)

	m, err := openColdMPHF(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.close() })

	seen := make(map[[32]byte]struct{}, n)
	for _, e := range entries {
		seen[e.hash] = struct{}{}
	}

	r := testRNG(0xdeadbeef)
	const probes = 1000
	notFound := 0
	tried := 0
	for tried < probes {
		unseen := randHash(r)
		if _, dup := seen[unseen]; dup {
			continue
		}
		tried++
		_, err := m.lookup(unseen)
		if err != nil {
			require.ErrorIs(t, err, stores.ErrNotFound)
			notFound++
		}
	}
	assert.GreaterOrEqual(t, notFound, tried*95/100,
		"WithFingerprint(1) should reject >=95%% of unseen keys; got %d/%d", notFound, tried)
}

func TestColdMPHF_OpenNonExistentErrors(t *testing.T) {
	_, err := openColdMPHF(filepath.Join(t.TempDir(), "does-not-exist.idx"))
	assert.Error(t, err)
}

func TestColdMPHF_OpenBadMetadataErrors(t *testing.T) {
	// Build an index with no UserMetadata; openColdMPHF should
	// reject it because ParseMinLedger requires exactly 4 bytes.
	path := filepath.Join(t.TempDir(), ColdIndexName)
	sb, err := streamhash.NewSortedBuilder(context.Background(), path, 1,
		streamhash.WithPayload(ColdPayloadSize),
		streamhash.WithFingerprint(ColdFingerprintSize),
		// no WithMetadata
	)
	require.NoError(t, err)
	var k [16]byte
	require.NoError(t, sb.AddKey(k[:], 0))
	require.NoError(t, sb.Finish())

	_, err = openColdMPHF(path)
	assert.ErrorIs(t, err, ErrInvalidMetadata)
}
