package event

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/streamhash"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

// ──────────────────────────────────────────────────────────────────
// LedgerOffsets app-data wire-format tests.
// ──────────────────────────────────────────────────────────────────

func TestLedgerOffsets_EncodeDecodeRoundTrip(t *testing.T) {
	o := NewLedgerOffsets(50_002)
	require.NoError(t, o.Append(50_002, 3))
	require.NoError(t, o.Append(50_003, 0)) // empty ledger
	require.NoError(t, o.Append(50_004, 7))

	bytes, err := encodeLedgerOffsets(o)
	require.NoError(t, err)
	// Header (9 bytes) + 3 ledgers × 4 bytes.
	assert.Len(t, bytes, ledgerOffsetsHeaderLen+3*4)
	assert.Equal(t, LedgerOffsetsFormatVersion, bytes[0])

	decoded, err := DecodeLedgerOffsets(bytes)
	require.NoError(t, err)
	assert.Equal(t, o.StartLedger(), decoded.StartLedger())
	assert.Equal(t, o.LedgerCount(), decoded.LedgerCount())
	assert.Equal(t, o.TotalEvents(), decoded.TotalEvents())

	for _, ledger := range []uint32{50_002, 50_003, 50_004} {
		wantStart, wantEnd, err := o.EventIDs(ledger)
		require.NoError(t, err)
		gotStart, gotEnd, err := decoded.EventIDs(ledger)
		require.NoError(t, err)
		assert.Equal(t, wantStart, gotStart, "ledger %d start", ledger)
		assert.Equal(t, wantEnd, gotEnd, "ledger %d end", ledger)
	}
}

func TestLedgerOffsets_EncodeEmpty(t *testing.T) {
	o := NewLedgerOffsets(50_002)
	bytes, err := encodeLedgerOffsets(o)
	require.NoError(t, err)
	assert.Len(t, bytes, ledgerOffsetsHeaderLen)

	decoded, err := DecodeLedgerOffsets(bytes)
	require.NoError(t, err)
	assert.Equal(t, uint32(50_002), decoded.StartLedger())
	assert.Zero(t, decoded.LedgerCount())
	assert.Zero(t, decoded.TotalEvents())
}

func TestLedgerOffsets_DecodeRejectsShortBuffer(t *testing.T) {
	_, err := DecodeLedgerOffsets(nil)
	require.ErrorContains(t, err, "empty")

	short := make([]byte, ledgerOffsetsHeaderLen-1)
	short[0] = LedgerOffsetsFormatVersion // valid version, so the length check fires
	_, err = DecodeLedgerOffsets(short)
	assert.ErrorIs(t, err, ErrShortLedgerOffsets)
}

func TestLedgerOffsets_DecodeRejectsUnknownVersion(t *testing.T) {
	buf := make([]byte, ledgerOffsetsHeaderLen)
	buf[0] = 0xff // not LedgerOffsetsFormatVersion
	_, err := DecodeLedgerOffsets(buf)
	assert.ErrorContains(t, err, "written by a newer stellar-rpc")
}

func TestLedgerOffsets_DecodeRejectsTruncatedArray(t *testing.T) {
	// Declare 3 ledgers but only supply 2 entries of payload bytes.
	o := NewLedgerOffsets(50_002)
	require.NoError(t, o.Append(50_002, 1))
	require.NoError(t, o.Append(50_003, 1))
	require.NoError(t, o.Append(50_004, 1))

	full, err := encodeLedgerOffsets(o)
	require.NoError(t, err)

	truncated := full[:len(full)-4]
	_, err = DecodeLedgerOffsets(truncated)
	assert.ErrorIs(t, err, ErrShortLedgerOffsets)
}

func TestLedgerOffsets_EncodeNil(t *testing.T) {
	_, err := encodeLedgerOffsets(nil)
	assert.Error(t, err)
}

// ──────────────────────────────────────────────────────────────────
// MPHF wrapper tests.
// ──────────────────────────────────────────────────────────────────

// testIndexSecret is the fixed routing secret every index build in this test
// package uses (production derives one per chunk via ColdIndexSecret).
var testIndexSecret = [stores.SecretLen]byte{
	0xd0, 0xd1, 0xd2, 0xd3, 0xd4, 0xd5, 0xd6, 0xd7,
	0xd8, 0xd9, 0xda, 0xdb, 0xdc, 0xdd, 0xde, 0xdf,
}

// keyFor returns the TermKey ComputeTermKey produces for the i'th
// test value — useful for verifying Lookup against the same value
// the test loaded into the Bitmaps.
func keyFor(i int) TermKey {
	return ComputeTermKey(
		fmt.Appendf(nil, "key-%d", i),
		FieldContractID,
	)
}

// buildIndex returns a populated Bitmaps of n distinct terms,
// each with a single event ID. Mirrors how the freeze writer will
// hand the chunk's term set to buildMPHF at runtime — the writer
// always has an Bitmaps in hand (the chunk's in-memory mirror
// or one rebuilt from a RocksDB scan). The returned index is already
// Close()'d so buildMPHF can iterate via idx.All().
func buildIndex(t *testing.T, n int) Bitmaps {
	t.Helper()
	idx := NewBitmaps()
	for i := range n {
		idx.AddTo(
			ComputeTermKey(fmt.Appendf(nil, "key-%d", i), FieldContractID),
			uint32(i),
		)
	}
	return idx
}

func TestBuild_KnownKeysGetUniqueSlotsInRange(t *testing.T) {
	const n = 128
	m, err := buildMPHF(context.Background(), buildIndex(t, n), filepath.Join(t.TempDir(), "index.hash"), testIndexSecret)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	seen := make(map[uint32]int, n)
	for i := range n {
		slot, err := m.Lookup(keyFor(i))
		require.NoError(t, err)
		assert.Less(t, slot, uint32(n), "slot %d out of range for key %d", slot, i)
		if prev, dup := seen[slot]; dup {
			t.Fatalf("slot %d returned for both key %d and key %d — MPHF must be injective on the build set",
				slot, prev, i)
		}
		seen[slot] = i
	}
	assert.Len(t, seen, n, "every build-set key must map to a distinct slot")
}

func TestBuild_LookupIsDeterministic(t *testing.T) {
	const n = 16
	m, err := buildMPHF(context.Background(), buildIndex(t, n), filepath.Join(t.TempDir(), "index.hash"), testIndexSecret)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	for i := range n {
		k := keyFor(i)
		first, err := m.Lookup(k)
		require.NoError(t, err)
		for range 5 {
			repeat, err := m.Lookup(k)
			require.NoError(t, err)
			assert.Equal(t, first, repeat, "Lookup must be deterministic across calls")
		}
	}
}

func TestLookup_UnseenKeyBehavior(t *testing.T) {
	// streamhash gives us a free partial fingerprint: routing-stage
	// detection catches *some* unseen keys outright (returning
	// ErrKeyNotFound). Others map to a build-set slot and need the
	// 4-byte fingerprint in index.pack to catch downstream. Pin both
	// possibilities so cold-reader code (PR-3a) knows what to handle.
	const n = 64
	m, err := buildMPHF(context.Background(), buildIndex(t, n), filepath.Join(t.TempDir(), "index.hash"), testIndexSecret)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	// Try a batch of unseen keys; record outcomes. Both outcomes are
	// valid per the MPHF contract. 2000 probes keep P(zero collisions)
	// negligible.
	var (
		fastNoMatch  int
		collidedSlot int
	)
	for i := range 2000 {
		unseen := ComputeTermKey(
			fmt.Appendf(nil, "never-added-%d", i),
			FieldTopic0,
		)
		slot, err := m.Lookup(unseen)
		switch {
		case errors.Is(err, ErrKeyNotFound):
			fastNoMatch++
		case err == nil:
			assert.Less(t, slot, uint32(n),
				"colliding unseen key must still produce a slot in [0, N)")
			collidedSlot++
		default:
			t.Fatalf("unexpected error for unseen key: %v", err)
		}
	}
	// Both outcomes should occur for a reasonable batch — exact
	// ratios depend on streamhash's internals, so we just assert
	// "at least one of each."
	assert.Positive(t, fastNoMatch, "streamhash should fast-no-match SOME unseen keys")
	assert.Positive(t, collidedSlot, "some unseen keys collide into the slot space — that's why fingerprints exist")
}

func TestBuild_EmptyIndexSucceeds(t *testing.T) {
	// Zero terms builds a valid empty index rather than erroring.
	empty := NewBitmaps()
	m, err := buildMPHF(context.Background(), empty, filepath.Join(t.TempDir(), "index.hash"), testIndexSecret)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })
	assert.True(t, m.isEmpty())
	_, lerr := m.Lookup(ComputeTermKey([]byte("anything"), FieldContractID))
	assert.ErrorIs(t, lerr, ErrKeyNotFound)
}

func TestOpen_RoundTripsBuiltFile(t *testing.T) {
	const n = 32
	path := filepath.Join(t.TempDir(), "index.hash")

	built, err := buildMPHF(context.Background(), buildIndex(t, n), path, testIndexSecret)
	require.NoError(t, err)

	// Record every (key, slot) the Build handle reports, then close
	// it and reopen via Open. Slots must match — the file is the
	// authoritative serialization.
	expected := make(map[TermKey]uint32, n)
	for i := range n {
		k := keyFor(i)
		slot, err := built.Lookup(k)
		require.NoError(t, err)
		expected[k] = slot
	}
	require.NoError(t, built.Close())

	reopened, err := openMPHF(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })

	for k, want := range expected {
		got, err := reopened.Lookup(k)
		require.NoError(t, err)
		assert.Equal(t, want, got, "slot for key %x must round-trip via Open", k)
	}
}

func TestBuild_AcceptsManyKeys(t *testing.T) {
	// A more realistic workload — exercise streamhash beyond toy
	// sizes so basic build-time issues (chunked partition handling,
	// etc.) surface in unit tests rather than at PR-2c integration
	// time. Also exercises the streaming path: with an Bitmaps
	// holding 10K terms, Build never materializes the keys as a
	// slice.
	const n = 10_000
	m, err := buildMPHF(context.Background(), buildIndex(t, n), filepath.Join(t.TempDir(), "index.hash"), testIndexSecret)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	seen := make(map[uint32]struct{}, n)
	for i := range n {
		slot, err := m.Lookup(keyFor(i))
		require.NoError(t, err)
		assert.Less(t, slot, uint32(n))
		seen[slot] = struct{}{}
	}
	assert.Len(t, seen, n, "every key in the build set must occupy a unique slot")
}

func TestOpen_NonExistentFileErrors(t *testing.T) {
	_, err := openMPHF(filepath.Join(t.TempDir(), "does-not-exist.hash"))
	assert.Error(t, err)
}

func TestClose_IsIdempotent(t *testing.T) {
	m, err := buildMPHF(context.Background(), buildIndex(t, 4), filepath.Join(t.TempDir(), "index.hash"), testIndexSecret)
	require.NoError(t, err)

	require.NoError(t, m.Close())
	assert.NoError(t, m.Close(), "second Close must be a no-op")
}

// Keyed-routing metadata tests.

func TestEventsMeta_EncodeDecodeRoundTrip(t *testing.T) {
	secret := testIndexSecret

	blob := encodeEventsMeta(secret)
	require.Len(t, blob, eventsMetaLen)
	assert.Equal(t, eventsMetaVersion, blob[0])

	decoded, err := decodeEventsMeta(blob)
	require.NoError(t, err)
	assert.Equal(t, secret, decoded)
}

func TestEventsMeta_DecodeRejectsBadInput(t *testing.T) {
	good := encodeEventsMeta(testIndexSecret)

	_, err := decodeEventsMeta(good[:len(good)-1])
	assert.ErrorIs(t, err, errBadIndexMetadata, "short blob")

	_, err = decodeEventsMeta(append(good, 0x00))
	assert.ErrorIs(t, err, errBadIndexMetadata, "long blob")

	bad := append([]byte(nil), good...)
	bad[0] = 0x7f
	_, err = decodeEventsMeta(bad)
	assert.ErrorIs(t, err, errBadIndexMetadata, "unknown version")
}

func TestBuild_WritesKeyedRoutingMetadata(t *testing.T) {
	// A fresh build must carry v1 metadata and route every term key
	// through stores.BlindKey under the stored secret.
	const n = 64
	path := filepath.Join(t.TempDir(), "index.hash")
	m, err := buildMPHF(context.Background(), buildIndex(t, n), path, testIndexSecret)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	raw, err := streamhash.OpenBytes(data)
	require.NoError(t, err)
	t.Cleanup(func() { _ = raw.Close() })

	meta := raw.UserMetadata()
	require.Len(t, meta, eventsMetaLen, "index.hash must carry [version][secret] metadata")
	assert.Equal(t, eventsMetaVersion, meta[0])
	secret, err := decodeEventsMeta(meta)
	require.NoError(t, err)
	assert.Equal(t, testIndexSecret, secret, "stored secret must be the one the build was given")

	for i := range n {
		k := keyFor(i)
		rk := stores.BlindKey(secret, k[:])
		want, err := raw.QueryRank(rk[:])
		require.NoError(t, err, "routed key %d must be in the build set", i)
		got, err := m.Lookup(k)
		require.NoError(t, err)
		assert.Equal(t, uint32(want), got, "Lookup must equal QueryRank over the routed key")
	}
}

func TestBuild_DeterministicForFixedSecret(t *testing.T) {
	// Same inputs + same secret ⇒ byte-identical index.hash, so rebuilds
	// are reproducible (streamhash's build is insertion-order-independent,
	// covering the Bitmaps map-iteration randomness).
	const n = 512
	idx := buildIndex(t, n)

	build := func() []byte {
		path := filepath.Join(t.TempDir(), "index.hash")
		m, err := buildMPHF(context.Background(), idx, path, testIndexSecret)
		require.NoError(t, err)
		require.NoError(t, m.Close())
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		return data
	}

	first := build()
	second := build()
	assert.Equal(t, first, second, "rebuild with the same secret must be byte-identical")
}

func TestOpenMPHF_RejectsMissingOrMalformedMetadata(t *testing.T) {
	// Every index is keyed: openMPHF must refuse an index.hash whose
	// metadata is absent or unparseable.
	build := func(t *testing.T, opts ...streamhash.BuildOption) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "index.hash")
		builder, err := streamhash.NewUnsortedBuilder(context.Background(), path, 4, t.TempDir(), opts...)
		require.NoError(t, err)
		for i := range 4 {
			k := keyFor(i)
			require.NoError(t, builder.AddKey(k[:], 0))
		}
		require.NoError(t, builder.Finish())
		return path
	}

	_, err := openMPHF(build(t))
	assert.ErrorIs(t, err, errBadIndexMetadata, "no metadata")

	_, err = openMPHF(build(t, streamhash.WithMetadata([]byte{0x7f, 0x00})))
	assert.ErrorIs(t, err, errBadIndexMetadata, "malformed metadata")
}
