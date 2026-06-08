package txhash

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/streamhash"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
)

func TestEncodeParseMinLedger_RoundTrip(t *testing.T) {
	for _, ml := range []uint32{0, 1, 2, 100, 12345, 0xFFFFFFFF} {
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

func TestIndexBaseChunk(t *testing.T) {
	const cpi = 1000
	cases := []struct {
		in   chunk.ID
		want chunk.ID
	}{
		{0, 0},
		{1, 0},
		{999, 0},
		{1000, 1000},
		{1500, 1000},
		{2000, 2000},
	}
	for _, tc := range cases {
		assert.Equalf(t, tc.want, IndexBaseChunk(tc.in, cpi),
			"IndexBaseChunk(%d, %d)", tc.in, cpi)
	}
}

func TestIndexBaseChunk_ZeroPanics(t *testing.T) {
	assert.Panics(t, func() { IndexBaseChunk(5, 0) },
		"a zero group size must panic loudly, not divide-by-zero")
}

func TestIndexFileName(t *testing.T) {
	assert.Equal(t, "00000000-txhash.idx", IndexFileName(0))
	assert.Equal(t, "00001000-txhash.idx", IndexFileName(1000))
}

func TestColdMPHF_RoundTrip(t *testing.T) {
	// The format contract end-to-end: build via BuildColdIndex, then
	// confirm the reader recovers (a) the MinLedger anchor and (b) the
	// absolute seq for every entry.
	idxPath, entries := buildColdFixture(t, 64)

	m, err := openColdMPHF(idxPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.close() })

	assert.Equal(t, fixtureMinLedger(), m.minLedger, "MinLedger must survive the user-metadata round-trip")
	for _, e := range entries {
		got, err := m.lookup(e.hash)
		require.NoError(t, err)
		assert.Equal(t, e.seq, got, "absolute ledgerSeq for hash %x must round-trip", e.hash[:8])
	}
}

func TestColdLookup_UnseenKeyReturnsNotFound(t *testing.T) {
	// With a 1-byte fingerprint the residual false-positive rate for
	// unseen keys is ~1/256. Across 1000 probes the expected FP count
	// is ~4; requiring >=95% rejection gives generous headroom while
	// still catching a regression where the fingerprint isn't checked.
	const n = 64
	idxPath, entries := buildColdFixture(t, n)

	m, err := openColdMPHF(idxPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.close() })

	seen := make(map[[16]byte]struct{}, n)
	for _, e := range entries {
		var k [16]byte
		copy(k[:], e.hash[:16])
		seen[k] = struct{}{}
	}

	r := testRNG(0xdeadbeef)
	const probes = 1000
	notFound, tried := 0, 0
	for tried < probes {
		unseen := randHash(r)
		var k [16]byte
		copy(k[:], unseen[:16])
		if _, dup := seen[k]; dup {
			continue
		}
		tried++
		if _, err := m.lookup(unseen); err != nil {
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
	// An index built without ColdBuildOptions' user metadata must be
	// rejected at open: ParseMinLedger requires exactly 4 bytes.
	path := filepath.Join(t.TempDir(), "no-metadata.idx")
	sb, err := streamhash.NewSortedBuilder(context.Background(), path, 1,
		streamhash.WithPayload(ColdPayloadSize),
		streamhash.WithFingerprint(ColdFingerprintSize),
		// deliberately no WithMetadata
	)
	require.NoError(t, err)
	var k [binKeySize]byte
	require.NoError(t, sb.AddKey(k[:], 0))
	require.NoError(t, sb.Finish())
	require.NoError(t, sb.Close())

	_, err = openColdMPHF(path)
	assert.ErrorIs(t, err, ErrInvalidMetadata)
}

func TestColdMPHF_OpenWrongPayloadSizeErrors(t *testing.T) {
	// An index built with a payload width other than ColdPayloadSize
	// must be rejected at open, so lookup never silently truncates a
	// wider payload down to uint32.
	path := filepath.Join(t.TempDir(), "wrong-payload.idx")
	sb, err := streamhash.NewSortedBuilder(context.Background(), path, 1,
		streamhash.WithPayload(ColdPayloadSize+1), // wrong width
		streamhash.WithFingerprint(ColdFingerprintSize),
		streamhash.WithMetadata(EncodeMinLedger(2)),
	)
	require.NoError(t, err)
	var k [binKeySize]byte
	require.NoError(t, sb.AddKey(k[:], 0))
	require.NoError(t, sb.Finish())
	require.NoError(t, sb.Close())

	_, err = openColdMPHF(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "payload size")
}
