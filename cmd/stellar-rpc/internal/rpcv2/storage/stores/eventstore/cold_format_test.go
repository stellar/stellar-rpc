package eventstore

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/events"
)

// ──────────────────────────────────────────────────────────────────
// events.LedgerOffsets app-data wire-format tests.
// ──────────────────────────────────────────────────────────────────

func TestLedgerOffsets_EncodeDecodeRoundTrip(t *testing.T) {
	o := events.NewLedgerOffsets(50_002)
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
	o := events.NewLedgerOffsets(50_002)
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
	require.ErrorIs(t, err, ErrShortLedgerOffsets)

	_, err = DecodeLedgerOffsets(make([]byte, ledgerOffsetsHeaderLen-1))
	assert.ErrorIs(t, err, ErrShortLedgerOffsets)
}

func TestLedgerOffsets_DecodeRejectsUnknownVersion(t *testing.T) {
	buf := make([]byte, ledgerOffsetsHeaderLen)
	buf[0] = 0xff // not LedgerOffsetsFormatVersion
	_, err := DecodeLedgerOffsets(buf)
	assert.ErrorIs(t, err, ErrUnknownLedgerOffsetsVersion)
}

func TestLedgerOffsets_DecodeRejectsTruncatedArray(t *testing.T) {
	// Declare 3 ledgers but only supply 2 entries of payload bytes.
	o := events.NewLedgerOffsets(50_002)
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

// keyFor returns the events.TermKey events.ComputeTermKey produces for the i'th
// test value — useful for verifying Lookup against the same value
// the test loaded into the events.Bitmaps.
func keyFor(i int) events.TermKey {
	return events.ComputeTermKey(
		fmt.Appendf(nil, "key-%d", i),
		events.FieldContractID,
	)
}

// buildIndex returns a populated events.Bitmaps of n distinct terms,
// each with a single event ID. Mirrors how the freeze writer will
// hand the chunk's term set to buildMPHF at runtime — the writer
// always has an events.Bitmaps in hand (the chunk's in-memory mirror
// or one rebuilt from a RocksDB scan). The returned index is already
// Close()'d so buildMPHF can iterate via idx.All().
func buildIndex(t *testing.T, n int) events.Bitmaps {
	t.Helper()
	idx := events.NewBitmaps()
	for i := range n {
		idx.AddTo(
			events.ComputeTermKey(fmt.Appendf(nil, "key-%d", i), events.FieldContractID),
			uint32(i),
		)
	}
	return idx
}

func TestBuild_KnownKeysGetUniqueSlotsInRange(t *testing.T) {
	const n = 128
	m, err := buildMPHF(context.Background(), buildIndex(t, n), filepath.Join(t.TempDir(), "index.hash"))
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
	m, err := buildMPHF(context.Background(), buildIndex(t, n), filepath.Join(t.TempDir(), "index.hash"))
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
	m, err := buildMPHF(context.Background(), buildIndex(t, n), filepath.Join(t.TempDir(), "index.hash"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	// Try a batch of unseen keys; record outcomes. Both outcomes are
	// valid per the MPHF contract.
	var (
		fastNoMatch  int
		collidedSlot int
	)
	for i := range 200 {
		unseen := events.ComputeTermKey(
			fmt.Appendf(nil, "never-added-%d", i),
			events.FieldTopic0,
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
	empty := events.NewBitmaps()
	m, err := buildMPHF(context.Background(), empty, filepath.Join(t.TempDir(), "index.hash"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })
	assert.True(t, m.isEmpty())
	_, lerr := m.Lookup(events.ComputeTermKey([]byte("anything"), events.FieldContractID))
	assert.ErrorIs(t, lerr, ErrKeyNotFound)
}

func TestOpen_RoundTripsBuiltFile(t *testing.T) {
	const n = 32
	path := filepath.Join(t.TempDir(), "index.hash")

	built, err := buildMPHF(context.Background(), buildIndex(t, n), path)
	require.NoError(t, err)

	// Record every (key, slot) the Build handle reports, then close
	// it and reopen via Open. Slots must match — the file is the
	// authoritative serialization.
	expected := make(map[events.TermKey]uint32, n)
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
	// time. Also exercises the streaming path: with an events.Bitmaps
	// holding 10K terms, Build never materializes the keys as a
	// slice.
	const n = 10_000
	m, err := buildMPHF(context.Background(), buildIndex(t, n), filepath.Join(t.TempDir(), "index.hash"))
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
	m, err := buildMPHF(context.Background(), buildIndex(t, 4), filepath.Join(t.TempDir(), "index.hash"))
	require.NoError(t, err)

	require.NoError(t, m.Close())
	assert.NoError(t, m.Close(), "second Close must be a no-op")
}
