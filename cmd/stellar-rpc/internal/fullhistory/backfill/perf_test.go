package backfill

// perf_test.go pins the tx-hash cold-index format the streaming rebuild
// produces to the merged #728/#780 cold path, and records the design's
// Part-4 sizing expectation (see PERF.md). It is the load-bearing assertion
// behind PERF.md's "the formats are identical, so the bench figures transfer"
// claim: the perf numbers are honest only if the bytes the streaming rebuild
// writes are the same bytes the bench harness measured.
//
// Two independent assertions:
//
//   - Format identity. buildTxhashIndex (the streaming rebuild) and a direct
//     txhash.BuildColdIndex over the SAME .bin inputs produce a byte-identical
//     .idx — same MPHF structure, same 3-byte payload, same 1-byte fingerprint,
//     same [MinLedger, MaxLedger] metadata. The streaming path adds catalog
//     bookkeeping around the build; it must not perturb the artifact.
//
//   - On-disk format pins. The .bin inputs match gettransaction §6.1
//     (uint64-LE count header, 20-byte [16-key|4-seq-LE] entries) and the .idx
//     matches §6.2 (16-byte routing key, 3-byte payload offset from MinLedger,
//     1-byte fingerprint), read back through the real reader.

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/streamhash"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash"
)

// ---------------------------------------------------------------------------
// Format identity: the streaming rebuild writes the same bytes as the merged
// cold path.
// ---------------------------------------------------------------------------

// TestStreamingRebuild_ByteIdenticalToColdPath is the heart of Issue 20. It
// freezes a set of per-chunk .bin runs through the one-write protocol (the real
// txhash.WriteColdBin codec), then builds the SAME coverage two ways:
//
//  1. the streaming rebuild — buildTxhashIndex, which the daemon's executor
//     drives on every boundary (build.go); and
//  2. a direct txhash.BuildColdIndex over the identical inputs — the merged
//     cold path the bench harness on rpc-hack measures.
//
// The two .idx files must be byte-for-byte identical. That is what licenses
// PERF.md to transfer the bench harness's measured ≈4.2 B/tx and ≈1-min
// figures to the streaming daemon: the streaming rebuild is not a re-derivation
// of the format, it is the same txhash.BuildColdIndex call wrapped in catalog
// bookkeeping, and the bookkeeping does not touch the artifact.
func TestStreamingRebuild_ByteIdenticalToColdPath(t *testing.T) {
	cat, _ := smallTxHashIndexCatalog(t, 4) // window 0 = chunks [0,3]
	cfg := testBuildConfig(cat)

	// Spread entries across several chunks so the build genuinely k-way merges
	// the runs (not a single trivial input).
	entriesByChunk := map[chunk.ID][]txEntry{
		0: {{hashAt(1), seqIn(0, 5)}, {hashAt(2), seqIn(0, 9000)}},
		1: {{hashAt(3), seqIn(1, 1)}, {hashAt(4), seqIn(1, 4321)}},
		2: {{hashAt(5), seqIn(2, 77)}},
	}
	var inputs []string
	for c := chunk.ID(0); c <= 2; c++ {
		freezeChunkBin(t, cat, c, entriesByChunk[c])
		inputs = append(inputs, cat.Layout().TxHashBinPath(c))
	}

	// (1) The streaming rebuild. Non-terminal coverage [0,2] (hi 2 < window-last
	// 3) so it keeps its inputs frozen — we reuse them for path (2).
	require.NoError(t, buildTxhashIndex(context.Background(), 0, 0, 2, cfg))
	frozen, ok, err := cat.FrozenTxHashIndex(0)
	require.NoError(t, err)
	require.True(t, ok)
	streamingIdx := cat.Layout().TxHashIndexFilePath(frozen)

	// (2) The merged cold path, over the SAME .bin inputs, with the SAME
	// MinLedger/MaxLedger anchor the streaming path derives (lo.FirstLedger,
	// hi.LastLedger — build.go step 3).
	minLedger := chunk.ID(0).FirstLedger()
	maxLedger := chunk.ID(2).LastLedger()
	directIdx := filepath.Join(t.TempDir(), "direct.idx")
	require.NoError(t, txhash.BuildColdIndex(context.Background(), inputs, directIdx, minLedger, maxLedger))

	streamingBytes, err := os.ReadFile(streamingIdx)
	require.NoError(t, err)
	directBytes, err := os.ReadFile(directIdx)
	require.NoError(t, err)

	require.Equal(t, directBytes, streamingBytes,
		"the streaming rebuild must write a byte-identical .idx to the merged cold path "+
			"(this is what lets PERF.md transfer the bench harness's measured figures)")
}

// ---------------------------------------------------------------------------
// On-disk format pins: §6.1 (.bin) and §6.2 (.idx).
// ---------------------------------------------------------------------------

// TestStreamingBin_MatchesSpecFormat asserts the .bin a frozen chunk leaves on
// disk matches gettransaction §6.1: a uint64-LE entry-count header followed by
// 20-byte [16-byte key | 4-byte LE seq] entries. freezeChunkBin uses the real
// txhash.WriteColdBin, so this is the producer's actual on-disk contract.
func TestStreamingBin_MatchesSpecFormat(t *testing.T) {
	cat, _ := smallTxHashIndexCatalog(t, 4)

	e0 := txEntry{hashAt(11), seqIn(0, 5)}
	e1 := txEntry{hashAt(12), seqIn(0, 9999)}
	freezeChunkBin(t, cat, 0, []txEntry{e0, e1})

	raw, err := os.ReadFile(cat.Layout().TxHashBinPath(0))
	require.NoError(t, err)

	// §6.1: 8-byte header + N * 20-byte entries.
	const (
		hdrSize   = 8
		keyW      = 16 // streamhash.MinKeySize
		seqW      = 4
		entryW    = keyW + seqW // 20 bytes exactly
		wantCount = 2
	)
	require.Equal(t, txhash.ColdKeySize, keyW, "spec pins the .bin key to 16 bytes")
	require.Equal(t, streamhash.MinKeySize, keyW, "16-byte key == streamhash routing-key width")
	require.Len(t, raw, hdrSize+wantCount*entryW, "header + 20-byte entries")

	count := binary.LittleEndian.Uint64(raw[:hdrSize])
	require.Equal(t, uint64(wantCount), count, "uint64-LE entry-count header")

	// Each entry: 16-byte truncated key, then a uint32-LE absolute seq. Entries
	// are written sorted lex by key, so locate each by its known key prefix.
	wantSeqByKey := map[[keyW]byte]uint32{}
	for _, e := range []txEntry{e0, e1} {
		var k [keyW]byte
		copy(k[:], e.hash[:keyW])
		wantSeqByKey[k] = e.seq
	}
	for i := range wantCount {
		off := hdrSize + i*entryW
		var k [keyW]byte
		copy(k[:], raw[off:off+keyW])
		gotSeq := binary.LittleEndian.Uint32(raw[off+keyW : off+entryW])
		require.Equal(t, wantSeqByKey[k], gotSeq, "entry %d: 16-byte key then uint32-LE seq", i)
	}
}

// TestStreamingIdx_MatchesSpecFormat asserts the .idx the streaming rebuild
// writes matches gettransaction §6.2 — the merged #728/#780 cold-index format —
// read back through the real streamhash reader and the cold metadata codec:
// 16-byte routing key, 3-byte payload (ledgerSeq - MinLedger), 1-byte
// fingerprint, [MinLedger, MaxLedger] in the user-metadata slot.
func TestStreamingIdx_MatchesSpecFormat(t *testing.T) {
	// Pin the spec constants themselves (a config change that moved a width
	// would break the bench-transferred figures, so fail here too).
	require.Equal(t, 3, txhash.ColdPayloadSize, "§6.2: 3-byte payload at the default window")
	require.Equal(t, 1, txhash.ColdFingerprintSize, "§6.2: 1-byte fingerprint default")
	require.Equal(t, 16, txhash.ColdKeySize, "§6.1/§6.2: 16-byte routing key")

	cat, _ := smallTxHashIndexCatalog(t, 4)
	cfg := testBuildConfig(cat)

	e0 := txEntry{hashAt(21), seqIn(0, 5)}
	e1 := txEntry{hashAt(22), seqIn(1, 4242)}
	freezeChunkBin(t, cat, 0, []txEntry{e0})
	freezeChunkBin(t, cat, 1, []txEntry{e1})

	require.NoError(t, buildTxhashIndex(context.Background(), 0, 0, 1, cfg))
	frozen, ok, err := cat.FrozenTxHashIndex(0)
	require.NoError(t, err)
	require.True(t, ok)

	idx, err := streamhash.OpenPayload(cat.Layout().TxHashIndexFilePath(frozen))
	require.NoError(t, err)
	t.Cleanup(func() { _ = idx.Close() })

	// Payload, fingerprint, metadata as written by the build.
	require.Equal(t, txhash.ColdPayloadSize, idx.PayloadSize(), "3-byte payload on disk")
	require.Equal(t, txhash.ColdFingerprintSize, idx.Stats().FingerprintSize, "1-byte fingerprint on disk")
	require.Equal(t, uint64(2), idx.NumKeys(), "one key per indexed transaction")

	gotMin, gotMax, err := txhash.ParseLedgerRange(idx.UserMetadata())
	require.NoError(t, err)
	require.Equal(t, chunk.ID(0).FirstLedger(), gotMin, "MinLedger anchor = lo.FirstLedger")
	require.Equal(t, chunk.ID(1).LastLedger(), gotMax, "MaxLedger = hi.LastLedger")

	// The 3-byte payload is the seq's offset from MinLedger, recovered as the
	// absolute seq by the reader.
	reader, err := txhash.OpenColdReader(cat.Layout().TxHashIndexFilePath(frozen))
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close() })
	for _, e := range []txEntry{e0, e1} {
		got, gerr := reader.Get(e.hash)
		require.NoError(t, gerr)
		require.Equal(t, e.seq, got, "payload decodes to absolute seq (offset + MinLedger)")
	}
}

// ---------------------------------------------------------------------------
// Sizing: bytes-per-tx consistent with the design's Part-4 number.
// ---------------------------------------------------------------------------

// TestColdIndexSizing_ConsistentWithPart4 asserts the .idx the streaming
// rebuild writes lands near the design's Part-4 ≈4.2 B/tx figure (PERF.md). The
// MPHF's per-key overhead has a fixed component that dominates at small key
// counts, so this is a small-N sanity band, not the asymptotic figure — at the
// dense full window (~3e9 keys) the bench harness measures ≈4.2 B/tx, and the
// width pins above guarantee the per-key payload+fingerprint contribution (4 B)
// is identical here. The band exists to catch a gross regression (e.g. a
// payload or fingerprint width change, or an MPHF parameter blow-up), not to
// re-measure the asymptote.
func TestColdIndexSizing_ConsistentWithPart4(t *testing.T) {
	const nKeys = 20_000

	cat, _ := smallTxHashIndexCatalog(t, 4)
	cfg := testBuildConfig(cat)

	// Spread nKeys across chunks 0..2, each seq inside its chunk's range.
	perChunk := nKeys / 3
	var n uint64
	for c := chunk.ID(0); c <= 2; c++ {
		entries := make([]txEntry, 0, perChunk)
		for i := range perChunk {
			entries = append(entries, txEntry{hashAt(uint64(c)<<40 | uint64(i)), seqIn(c, uint32(i)+1)})
		}
		freezeChunkBin(t, cat, c, entries)
		n += uint64(len(entries))
	}

	require.NoError(t, buildTxhashIndex(context.Background(), 0, 0, 2, cfg))
	frozen, ok, err := cat.FrozenTxHashIndex(0)
	require.NoError(t, err)
	require.True(t, ok)

	info, err := os.Stat(cat.Layout().TxHashIndexFilePath(frozen))
	require.NoError(t, err)
	bytesPerTx := float64(info.Size()) / float64(n)
	t.Logf("cold .idx: %d bytes over %d keys = %.3f B/tx (design Part-4 asymptote ≈4.2 B/tx at the dense window)",
		info.Size(), n, bytesPerTx)

	// The per-key contribution is 4 B (3-byte payload + 1-byte fingerprint) plus
	// the MPHF structure; at small N the fixed header + block overhead inflates
	// B/tx, so allow a generous upper band and a hard floor (payload+fingerprint
	// alone is 4 B, so anything <4 means a width regressed away).
	require.GreaterOrEqual(t, bytesPerTx, 4.0,
		"payload (3B) + fingerprint (1B) is an inviolable 4 B/tx floor")
	require.LessOrEqual(t, bytesPerTx, 8.0,
		"small-N .idx should stay within a small multiple of the ≈4.2 B/tx asymptote")
}
