package txhash

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/streamhash"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

func TestEncodeParseColdMetadata_RoundTrip(t *testing.T) {
	for _, m := range []ColdMetadata{
		{MinLedger: 0, MaxLedger: 0},
		{MinLedger: 2, MaxLedger: 2},
		{MinLedger: 100, MaxLedger: 12345},
		{MinLedger: 50002, MaxLedger: 60001},
		{MinLedger: 0, MaxLedger: 0xFFFFFFFF},
	} {
		m.Secret = testSecret()
		got, err := ParseColdMetadata(EncodeColdMetadata(m))
		require.NoError(t, err)
		assert.Equal(t, m, got)
	}
}

func TestParseColdMetadata_WrongSizeErrors(t *testing.T) {
	// 25 is the only valid width. Blobs carry a valid version byte so the size
	// check is what fires (the version check runs first by design).
	for _, sz := range []int{1, 4, 13, 24, 26, 29} {
		blob := make([]byte, sz)
		blob[0] = coldMetadataVersion
		_, err := ParseColdMetadata(blob)
		assert.ErrorIs(t, err, ErrInvalidMetadata, "size %d should error", sz)
	}
	_, err := ParseColdMetadata(nil)
	require.ErrorIs(t, err, ErrInvalidMetadata, "empty blob should error")
	require.ErrorContains(t, err, "empty")
}

func TestParseColdMetadata_UnknownVersionErrors(t *testing.T) {
	blob := EncodeColdMetadata(ColdMetadata{MinLedger: 5, MaxLedger: 9, Secret: testSecret()})
	blob[0] = coldMetadataVersion + 1
	_, err := ParseColdMetadata(blob)
	require.ErrorContains(t, err, "written by a newer stellar-rpc")
}

func TestParseColdMetadata_OutOfOrderErrors(t *testing.T) {
	for _, m := range []ColdMetadata{
		{MinLedger: 100, MaxLedger: 99},
	} {
		_, err := ParseColdMetadata(EncodeColdMetadata(m))
		assert.ErrorIs(t, err, ErrInvalidMetadata, "%+v should error", m)
	}
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
	// An index built without the cold metadata must be rejected at open:
	// ParseColdMetadata requires exactly coldMetadataSize bytes.
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
	require.ErrorIs(t, err, ErrInvalidMetadata, "a metadata-less index must fail open")
	require.ErrorContains(t, err, "empty")
}

func TestOpenColdReader_WrongPayloadSizeErrors(t *testing.T) {
	// A payload width other than ColdPayloadSize must be rejected at open,
	// so Get never silently truncates a wider payload to uint32.
	path := filepath.Join(t.TempDir(), "wrong-payload.idx")
	sb, err := streamhash.NewSortedBuilder(context.Background(), path, 1,
		streamhash.WithPayload(ColdPayloadSize+1), // wrong width
		streamhash.WithFingerprint(ColdFingerprintSize),
		streamhash.WithMetadata(EncodeColdMetadata(ColdMetadata{MinLedger: 2, MaxLedger: 2, Secret: testSecret()})),
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
