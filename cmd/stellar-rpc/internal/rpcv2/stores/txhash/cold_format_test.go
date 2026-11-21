package txhash

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/streamhash"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

func TestEncodeColdMetadata_Layout(t *testing.T) {
	m := ColdMetadata{MinLedger: 100, FirstCovered: 200, MaxLedger: 12345, Secret: testSecret()}
	blob := EncodeColdMetadata(m)
	require.Len(t, blob, coldMetadataSize)
	require.Equal(t, coldMetadataVersion, blob[0])
	require.Equal(t, m.MinLedger, binary.LittleEndian.Uint32(blob[1:5]))
	require.Equal(t, m.FirstCovered, binary.LittleEndian.Uint32(blob[5:9]))
	require.Equal(t, m.MaxLedger, binary.LittleEndian.Uint32(blob[9:13]))
	require.Equal(t, m.Secret[:], blob[13:])
	require.Equal(t, "0164000000c800000039300000dba5038d85bce2c689d18353f1aeb26f", hex.EncodeToString(blob))
}

func TestColdReader_UnseenKeyReturnsNotFound(t *testing.T) {
	// With a 1-byte fingerprint the residual false-positive rate for
	// unseen keys is ~1/256. Across 1000 probes the expected FP count
	// is ~4; requiring >=95% rejection gives generous headroom while
	// still catching a regression where the fingerprint isn't checked.
	const n = 64
	idxPath, entries := buildColdFixture(t, n)

	r, err := OpenColdReader(idxPath, fixtureMeta())
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })

	seen := make(map[[32]byte]struct{}, n)
	for _, e := range entries {
		seen[e.hash] = struct{}{}
	}

	rng := testRNG(0xdeadbeef)
	const probes = 1000
	notFound, tried := 0, 0
	for tried < probes {
		unseen := randHash(rng)
		if _, dup := seen[unseen]; dup {
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
	// An index whose metadata is missing or of a newer version is refused at
	// open, before any comparison with the expected metadata.
	for _, tc := range []struct {
		name string
		opts []streamhash.BuildOption
		want string
	}{
		{"no metadata", nil, "empty"},
		{
			"a newer version",
			[]streamhash.BuildOption{streamhash.WithMetadata([]byte{coldMetadataVersion + 1})},
			"written by a newer stellar-rpc",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "bad-metadata.idx")
			opts := append([]streamhash.BuildOption{
				streamhash.WithPayload(ColdPayloadSize),
				streamhash.WithFingerprint(ColdFingerprintSize),
			}, tc.opts...)
			sb, err := streamhash.NewSortedBuilder(context.Background(), path, 1, opts...)
			require.NoError(t, err)
			var k [ColdKeySize]byte
			require.NoError(t, sb.AddKey(k[:], 0))
			require.NoError(t, sb.Finish())
			require.NoError(t, sb.Close())

			_, err = OpenColdReader(path, fixtureMeta())
			require.ErrorIs(t, err, ErrInvalidMetadata)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestOpenColdReader_RefusesAnotherIndex(t *testing.T) {
	idxPath, _ := buildColdFixture(t, 8)
	otherCoverage, otherFirst := fixtureMeta(), fixtureMeta()
	otherCoverage.MaxLedger--
	otherFirst.FirstCovered--
	for name, want := range map[string]ColdMetadata{
		"another coverage": otherCoverage, "another first covered ledger": otherFirst,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := OpenColdReader(idxPath, want)
			require.ErrorIs(t, err, stores.ErrCorrupt)
			require.ErrorContains(t, err, idxPath)
		})
	}
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

	_, err = OpenColdReader(path, ColdMetadata{MinLedger: 2, MaxLedger: 2, Secret: testSecret()})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "payload size")
}
