package txhash

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/streamhash"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/storage/stores"
)

func TestEncodeParseLedgerRange_RoundTrip(t *testing.T) {
	cases := [][2]uint32{{0, 0}, {2, 2}, {100, 12345}, {50002, 60001}, {0, 0xFFFFFFFF}}
	for _, c := range cases {
		gotMin, gotMax, err := ParseLedgerRange(EncodeLedgerRange(c[0], c[1]))
		require.NoError(t, err)
		assert.Equal(t, c[0], gotMin)
		assert.Equal(t, c[1], gotMax)
	}
}

func TestParseLedgerRange_WrongSizeErrors(t *testing.T) {
	for _, sz := range []int{0, 1, 4, 7, 9, 16} {
		_, _, err := ParseLedgerRange(make([]byte, sz))
		assert.ErrorIs(t, err, ErrInvalidMetadata, "size %d should error", sz)
	}
}

func TestParseLedgerRange_MaxBelowMinErrors(t *testing.T) {
	_, _, err := ParseLedgerRange(EncodeLedgerRange(100, 99))
	assert.ErrorIs(t, err, ErrInvalidMetadata)
}

func TestColdReader_UnseenKeyReturnsNotFound(t *testing.T) {
	// With a 1-byte fingerprint the residual false-positive rate for
	// unseen keys is ~1/256. Across 1000 probes the expected FP count
	// is ~4; requiring >=95% rejection gives generous headroom while
	// still catching a regression where the fingerprint isn't checked.
	const n = 64
	idxPath, entries := buildColdFixture(t, n)

	r, err := OpenColdReader(idxPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })

	seen := make(map[[16]byte]struct{}, n)
	for _, e := range entries {
		var k [16]byte
		copy(k[:], e.hash[:16])
		seen[k] = struct{}{}
	}

	rng := testRNG(0xdeadbeef)
	const probes = 1000
	notFound, tried := 0, 0
	for tried < probes {
		unseen := randHash(rng)
		var k [16]byte
		copy(k[:], unseen[:16])
		if _, dup := seen[k]; dup {
			continue
		}
		tried++
		if _, err := r.Get(unseen); err != nil {
			require.ErrorIs(t, err, stores.ErrNotFound)
			notFound++
		}
	}
	assert.GreaterOrEqual(t, notFound, tried*95/100,
		"WithFingerprint(1) should reject >=95%% of unseen keys; got %d/%d", notFound, tried)
}

func TestOpenColdReader_BadMetadataErrors(t *testing.T) {
	// An index built without ColdBuildOptions' metadata must be rejected
	// at open: ParseLedgerRange requires exactly 8 bytes.
	path := filepath.Join(t.TempDir(), "no-metadata.idx")
	sb, err := streamhash.NewSortedBuilder(context.Background(), path, 1,
		streamhash.WithPayload(ColdPayloadSize),
		streamhash.WithFingerprint(ColdFingerprintSize),
		// deliberately no WithMetadata
	)
	require.NoError(t, err)
	var k [ColdKeySize]byte
	require.NoError(t, sb.AddKey(k[:], 0))
	require.NoError(t, sb.Finish())
	require.NoError(t, sb.Close())

	_, err = OpenColdReader(path)
	assert.ErrorIs(t, err, ErrInvalidMetadata)
}

func TestOpenColdReader_WrongPayloadSizeErrors(t *testing.T) {
	// A payload width other than ColdPayloadSize must be rejected at open,
	// so Get never silently truncates a wider payload to uint32.
	path := filepath.Join(t.TempDir(), "wrong-payload.idx")
	sb, err := streamhash.NewSortedBuilder(context.Background(), path, 1,
		streamhash.WithPayload(ColdPayloadSize+1), // wrong width
		streamhash.WithFingerprint(ColdFingerprintSize),
		streamhash.WithMetadata(EncodeLedgerRange(2, 2)),
	)
	require.NoError(t, err)
	var k [ColdKeySize]byte
	require.NoError(t, sb.AddKey(k[:], 0))
	require.NoError(t, sb.Finish())
	require.NoError(t, sb.Close())

	_, err = OpenColdReader(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "payload size")
}
