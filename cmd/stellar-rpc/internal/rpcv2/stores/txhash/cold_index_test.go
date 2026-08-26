package txhash

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/streamhash"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

// ──────────────────────────────────────────────────────────────────
// Shared cold-store test fixtures (used by every cold *_test.go in
// this package).
// ──────────────────────────────────────────────────────────────────

// testCatalogSecret/testIndexID are the fixed keying inputs every fixture build
// and .bin writer share (deterministic, so fixtures are reproducible);
// testSecret is the derived per-index routing secret.
var testCatalogSecret = bytes.Repeat([]byte{0xA5}, 32)

const testIndexID uint32 = 7

func testSecret() [stores.SecretLen]byte { return ColdIndexSecret(testCatalogSecret, testIndexID) }

// randHash returns a 32-byte hash drawn from r. Cold tests use true
// random hashes (matching streamhash's own test patterns): structured
// hashes can produce correlated 16-byte prefixes that defeat the
// fingerprint check and confuse the not-found probes.
func randHash(r *rand.Rand) [32]byte {
	var h [32]byte
	for i := range h {
		h[i] = byte(r.UintN(256))
	}
	return h
}

// testRNG returns a deterministic rand source, so the random-looking
// fixtures are reproducible across runs.
func testRNG(seed uint64) *rand.Rand {
	return rand.New(rand.NewPCG(seed, seed*7919+1))
}

// fixtureEntry pairs a generated hash with the absolute ledgerSeq it
// is assigned in the fixture.
type fixtureEntry struct {
	hash [32]byte
	seq  uint32
}

// Fixture geometry. Entries spread across several per-chunk .bin files
// starting at fixtureBaseChunk so the build genuinely k-way merges them.
// The base chunk is non-zero so the MinLedger offset math is exercised —
// a bug returning the raw payload as the absolute seq surfaces as a large
// mismatch rather than passing by coincidence.
const (
	fixtureBaseChunk = chunk.ID(5)
	// fixtureSpreadChunks is how many per-chunk .bin files a fixture
	// spreads entries across. Kept > mergeFanIn so the standard fixtures
	// drive the multi-level fan-in merge, not just a single final merge.
	fixtureSpreadChunks = 6
)

// indexFileName names a scratch cold-index file for baseChunk. The on-disk
// layout is geometry.Layout.TxHashIndexFilePath; tests only need a stable name
// in their own TempDir, so this reuses chunk.ID's zero-padded String().
func indexFileName(baseChunk chunk.ID) string { return baseChunk.String() + "-txhash.idx" }

// fixtureMinLedger is the MinLedger anchor for the fixture index group.
func fixtureMinLedger() uint32 { return fixtureBaseChunk.FirstLedger() }

// fixtureMaxLedger is the coverage upper bound for the fixture group:
// the last ledger of its highest chunk. Every fixture seq is <= it.
func fixtureMaxLedger() uint32 {
	return (fixtureBaseChunk + chunk.ID(fixtureSpreadChunks-1)).LastLedger()
}

// makeFixtureEntries generates n entries with unique 16-byte key
// prefixes (the width the cold index keys on), each assigned a seq
// spread deterministically across the first fixtureSpreadChunks chunks
// of the group.
func makeFixtureEntries(n int) []fixtureEntry {
	r := testRNG(uint64(n) | 0xfeed)
	entries := make([]fixtureEntry, 0, n)
	seen := make(map[[ColdKeySize]byte]struct{}, n)
	for len(entries) < n {
		h := randHash(r)
		var k [ColdKeySize]byte
		copy(k[:], h[:ColdKeySize])
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}

		i := uint32(len(entries))
		chunkOffset := i % fixtureSpreadChunks
		inChunk := i / fixtureSpreadChunks // < n/spread; bounded well under LedgersPerChunk for test sizes
		seq := (fixtureBaseChunk + chunk.ID(chunkOffset)).FirstLedger() + inChunk
		entries = append(entries, fixtureEntry{hash: h, seq: seq})
	}
	return entries
}

// writeBinFile writes entries to a per-chunk .bin file in the input format
// BuildColdIndex consumes: each stored key is the secret-keyed routing key of
// the entry's hash prefix (mirroring ingest-time keying), sorted ascending
// (the per-file ordering the build's merge requires).
func writeBinFile(t *testing.T, path string, entries []fixtureEntry, secret [stores.SecretLen]byte) {
	t.Helper()
	keyed := make([]ColdEntry, len(entries))
	for i, e := range entries {
		keyed[i] = ColdEntry{Key: stores.BlindKey(secret, e.hash[:ColdKeySize]), Seq: e.seq}
	}
	sort.Slice(keyed, func(i, j int) bool {
		return bytes.Compare(keyed[i].Key[:], keyed[j].Key[:]) < 0
	})

	var buf bytes.Buffer
	var hdr [coldBinHeaderSize]byte
	binary.LittleEndian.PutUint64(hdr[:coldBinCountSize], uint64(len(keyed)))
	copy(hdr[coldBinCountSize:], secret[:])
	buf.Write(hdr[:])
	var seqBuf [coldBinSeqSize]byte
	for _, e := range keyed {
		buf.Write(e.Key[:])
		binary.LittleEndian.PutUint32(seqBuf[:], e.Seq)
		buf.Write(seqBuf[:])
	}
	require.NoError(t, os.WriteFile(path, buf.Bytes(), 0o600))
}

// writeFixtureBins partitions entries into per-chunk .bin files under
// dir (one file per chunk that received an entry) and returns their
// paths. The chunk a given entry belongs to is derived from its seq,
// mirroring how #765's per-chunk ingester would lay them out.
func writeFixtureBins(t *testing.T, dir string, entries []fixtureEntry) []string {
	t.Helper()
	return writeFixtureBinsKeyed(t, dir, entries, testSecret())
}

// writeFixtureBinsKeyed is writeFixtureBins under an explicit routing secret.
func writeFixtureBinsKeyed(t *testing.T, dir string, entries []fixtureEntry, secret [stores.SecretLen]byte) []string {
	t.Helper()
	byChunk := make(map[chunk.ID][]fixtureEntry)
	for _, e := range entries {
		c := chunk.IDFromLedger(e.seq)
		byChunk[c] = append(byChunk[c], e)
	}
	inputs := make([]string, 0, len(byChunk))
	for c, es := range byChunk {
		p := filepath.Join(dir, c.String()+".bin")
		writeBinFile(t, p, es, secret)
		inputs = append(inputs, p)
	}
	return inputs
}

// buildColdFixture builds a cold txhash index over n entries spread
// across several per-chunk .bin files, returning the index path and
// the entries (for round-trip assertions). It drives the real
// BuildColdIndex path so the merge + sorted build are under test.
func buildColdFixture(t *testing.T, n int) (string, []fixtureEntry) {
	t.Helper()
	dir := t.TempDir()
	entries := makeFixtureEntries(n)
	inputs := writeFixtureBins(t, dir, entries)
	require.Greater(t, len(inputs), 1, "fixture should span multiple .bin files to exercise the merge")

	idxPath := filepath.Join(dir, indexFileName(fixtureBaseChunk))
	require.NoError(t, BuildColdIndex(context.Background(), inputs, idxPath,
		fixtureMinLedger(), fixtureMaxLedger()))
	return idxPath, entries
}

// ──────────────────────────────────────────────────────────────────
// BuildColdIndex tests.
// ──────────────────────────────────────────────────────────────────

func TestBuildColdIndex_RoundTrip(t *testing.T) {
	// Builds from multiple .bin files and verifies every entry's
	// absolute seq round-trips through the merged index — the core
	// build→query contract over a chunk group.
	idxPath, entries := buildColdFixture(t, 300)

	info, err := os.Stat(idxPath)
	require.NoError(t, err)
	assert.Positive(t, info.Size(), "index file must be non-empty")

	r, err := OpenColdReader(idxPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })

	gotMin, gotMax := r.MinLedger(), r.MaxLedger()
	assert.Equal(t, fixtureMinLedger(), gotMin, "MinLedger anchor must survive the build")
	assert.Equal(t, fixtureMaxLedger(), gotMax, "MaxLedger anchor must survive the build")
	for _, e := range entries {
		got, err := r.Get(e.hash)
		require.NoError(t, err)
		assert.Equal(t, e.seq, got, "absolute ledgerSeq for hash %x must round-trip", e.hash[:8])
	}
}

func TestBuildColdIndex_LargeFilesSpanMultipleBuffers(t *testing.T) {
	// Enough entries per file that each .bin exceeds the merge read
	// buffer, so fileReader's block-aligned refill/advance path (and, on
	// Linux, the O_DIRECT aligned reads) is exercised — not just a single
	// read that swallows the whole file.
	const n = 60_000
	dir := t.TempDir()
	entries := makeFixtureEntries(n)
	inputs := writeFixtureBins(t, dir, entries)

	var maxSize int64
	for _, p := range inputs {
		fi, err := os.Stat(p)
		require.NoError(t, err)
		maxSize = max(maxSize, fi.Size())
	}
	require.Greater(t, maxSize, int64(mergeFileBufBytes),
		"fixture must produce a file larger than the read buffer to exercise refills")

	idxPath := filepath.Join(dir, indexFileName(fixtureBaseChunk))
	require.NoError(t, BuildColdIndex(context.Background(), inputs, idxPath,
		fixtureMinLedger(), fixtureMaxLedger()))

	r, err := OpenColdReader(idxPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })
	for _, e := range entries {
		got, err := r.Get(e.hash)
		require.NoError(t, err)
		require.Equal(t, e.seq, got)
	}
}

// assertEmptyColdIndex checks that idxPath is a valid empty cold index: it
// opens cleanly, carries its coverage's [wantMin, wantMax] metadata, and every
// lookup misses.
func assertEmptyColdIndex(t *testing.T, idxPath string, wantMin, wantMax uint32, wantSecret [stores.SecretLen]byte) {
	t.Helper()
	assert.FileExists(t, idxPath)
	r, err := OpenColdReader(idxPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })
	// The empty index must still carry correct coverage bounds and the adopted
	// index secret: a query blinds with the metadata secret, so dropping it on
	// the empty path would break lookups once the range fills.
	assert.Equal(t, wantMin, r.MinLedger(), "empty index minLedger")
	assert.Equal(t, wantMax, r.MaxLedger(), "empty index maxLedger")
	assert.Equal(t, wantSecret, r.secret, "empty index metadata secret")
	_, err = r.Get(randHash(testRNG(99)))
	require.ErrorIs(t, err, stores.ErrNotFound)
}

func TestBuildColdIndex_NoInputs(t *testing.T) {
	// With adopt-the-secret, the build reads the index secret from the .bin
	// headers, so it needs at least one input. Zero inputs is refused — it never
	// happens in production (a window always has its chunks' .bin files, empty or
	// not; an all-empty window is TestBuildColdIndex_AllEmptyInputs).
	idxPath := filepath.Join(t.TempDir(), indexFileName(0))
	err := BuildColdIndex(context.Background(), nil, idxPath, 2, 2)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no .bin inputs")
	assert.NoFileExists(t, idxPath)
}

func TestBuildColdIndex_MaxBelowMinErrors(t *testing.T) {
	dir := t.TempDir()
	inputs := writeFixtureBins(t, dir, makeFixtureEntries(8))
	idxPath := filepath.Join(dir, indexFileName(fixtureBaseChunk))
	err := BuildColdIndex(context.Background(), inputs, idxPath, 100, 99)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "maxLedger")
	assert.NoFileExists(t, idxPath)
}

func TestBuildColdIndex_AllEmptyInputs(t *testing.T) {
	// .bin files that exist but declare zero entries: the build produces a
	// valid empty index rather than failing, so the backfill advances past a
	// quiet range instead of stalling (#826).
	dir := t.TempDir()
	chunks := []chunk.ID{5, 6}
	inputs := make([]string, 0, len(chunks))
	for _, c := range chunks {
		p := filepath.Join(dir, c.String()+".bin")
		writeBinFile(t, p, nil, testSecret())
		inputs = append(inputs, p)
	}
	idxPath := filepath.Join(dir, indexFileName(fixtureBaseChunk))
	require.NoError(t, BuildColdIndex(context.Background(), inputs, idxPath,
		fixtureMinLedger(), fixtureMaxLedger()))
	assertEmptyColdIndex(t, idxPath, fixtureMinLedger(), fixtureMaxLedger(), testSecret())
}

func TestBuildColdIndex_MissingInputErrors(t *testing.T) {
	dir := t.TempDir()
	idxPath := filepath.Join(dir, indexFileName(fixtureBaseChunk))
	err := BuildColdIndex(context.Background(),
		[]string{filepath.Join(dir, "00000005.bin")}, idxPath, fixtureMinLedger(), fixtureMaxLedger())
	require.Error(t, err)
	assert.NoFileExists(t, idxPath)
}

func TestBuildColdIndex_SeqOutsideCoverageErrors(t *testing.T) {
	// An entry outside [minLedger, maxLedger] must be rejected — below would
	// underflow the payload, above would break the coverage claim — and
	// leave no partial index behind.
	for _, tc := range []struct {
		name string
		seq  uint32
	}{
		{"below min", fixtureMinLedger() - 1},
		{"above max", fixtureMaxLedger() + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			p := filepath.Join(dir, "00000004.bin")
			writeBinFile(t, p, []fixtureEntry{{hash: randHash(testRNG(1)), seq: tc.seq}}, testSecret())
			idxPath := filepath.Join(dir, indexFileName(fixtureBaseChunk))
			err := BuildColdIndex(context.Background(), []string{p}, idxPath,
				fixtureMinLedger(), fixtureMaxLedger())
			require.Error(t, err)
			assert.Contains(t, err.Error(), "outside index coverage")
			assert.NoFileExists(t, idxPath)
		})
	}
}

func TestBuildColdIndex_CoverageSpanExceedsBudgetErrors(t *testing.T) {
	// A [minLedger, maxLedger] span wider than the 3-byte payload can encode
	// is rejected up front, before any merge.
	dir := t.TempDir()
	minLedger := uint32(2)
	overflowSeq := minLedger + uint32(coldPayloadMax) + 1
	entries := []fixtureEntry{{hash: randHash(testRNG(2)), seq: overflowSeq}}
	p := filepath.Join(dir, "99999999.bin")
	writeBinFile(t, p, entries, testSecret())

	idxPath := filepath.Join(dir, indexFileName(0))
	err := BuildColdIndex(
		context.Background(), []string{p}, idxPath, minLedger, overflowSeq)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "payload budget")
	assert.NoFileExists(t, idxPath)
}

func TestBuildColdIndex_CallerOptsCannotOverrideFormat(t *testing.T) {
	// A caller opt that tries to repoint the cold format (here, bogus
	// metadata) must not win — ColdBuildOptions is pinned last, so the
	// reader still recovers the real coverage and resolves seqs correctly.
	dir := t.TempDir()
	entries := makeFixtureEntries(64)
	inputs := writeFixtureBins(t, dir, entries)
	idxPath := filepath.Join(dir, indexFileName(fixtureBaseChunk))
	err := BuildColdIndex(context.Background(), inputs, idxPath,
		fixtureMinLedger(), fixtureMaxLedger(),
		streamhash.WithMetadata(EncodeColdMetadata(7, 7, testSecret())))
	require.NoError(t, err)

	r, err := OpenColdReader(idxPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })

	gotMin, gotMax := r.MinLedger(), r.MaxLedger()
	assert.Equal(t, fixtureMinLedger(), gotMin, "pinned metadata must win over a caller's WithMetadata")
	assert.Equal(t, fixtureMaxLedger(), gotMax)
	for _, e := range entries {
		got, err := r.Get(e.hash)
		require.NoError(t, err)
		assert.Equal(t, e.seq, got)
	}
}

func TestBuildColdIndex_TruncatedFileErrors(t *testing.T) {
	// Header OVERSTATES the count (claims more entries than the body
	// holds): the open-time size cross-check rejects it; no index.
	dir := t.TempDir()
	var buf bytes.Buffer
	var hdr [coldBinHeaderSize]byte
	binary.LittleEndian.PutUint64(hdr[:], 5) // claim 5
	buf.Write(hdr[:])
	// ...but write only one entry.
	buf.Write(make([]byte, coldBinEntrySize))
	p := filepath.Join(dir, "00000005.bin")
	require.NoError(t, os.WriteFile(p, buf.Bytes(), 0o600))

	idxPath := filepath.Join(dir, indexFileName(fixtureBaseChunk))
	err := BuildColdIndex(context.Background(), []string{p}, idxPath,
		fixtureMinLedger(), fixtureMaxLedger())
	require.Error(t, err)
	assert.NoFileExists(t, idxPath)
}

func TestBuildColdIndex_HeaderUndercountErrors(t *testing.T) {
	// Header UNDERSTATES the count (file holds more entries than it
	// declares). Without the open-time size cross-check this would
	// silently drop the trailing entries; it must error instead.
	dir := t.TempDir()
	var buf bytes.Buffer
	var hdr [coldBinHeaderSize]byte
	binary.LittleEndian.PutUint64(hdr[:], 1) // declare 1...
	buf.Write(hdr[:])
	buf.Write(make([]byte, coldBinEntrySize*3)) // ...but write 3 entries
	p := filepath.Join(dir, "00000005.bin")
	require.NoError(t, os.WriteFile(p, buf.Bytes(), 0o600))

	idxPath := filepath.Join(dir, indexFileName(fixtureBaseChunk))
	err := BuildColdIndex(context.Background(), []string{p}, idxPath,
		fixtureMinLedger(), fixtureMaxLedger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "header claims")
	assert.NoFileExists(t, idxPath)
}

func TestBuildColdIndex_HeaderOverflowRejected(t *testing.T) {
	// A flipped high bit in the header count. The old `size == header + count*entry`
	// check multiplied the untrusted count, and coldBinEntrySize·2^62 ≡ 0 mod 2^64,
	// so such a count could pass a size check and feed ~2^62 into the builder (OOM,
	// then a crash loop). coldBinCount divides the trusted size instead, so it must
	// reject this cleanly (no allocation, no panic).
	dir := t.TempDir()
	var buf bytes.Buffer
	var hdr [coldBinHeaderSize]byte
	binary.LittleEndian.PutUint64(hdr[:], math.MaxUint64) // wildly overstated count
	buf.Write(hdr[:])
	buf.Write(make([]byte, coldBinEntrySize)) // one real entry
	p := filepath.Join(dir, "00000005.bin")
	require.NoError(t, os.WriteFile(p, buf.Bytes(), 0o600))

	idxPath := filepath.Join(dir, indexFileName(fixtureBaseChunk))
	err := BuildColdIndex(context.Background(), []string{p}, idxPath,
		fixtureMinLedger(), fixtureMaxLedger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "header claims")
	assert.NoFileExists(t, idxPath)
}

func TestBuildColdIndex_InputOrderIndependent(t *testing.T) {
	// The merge sorts globally, so feeding the same files in reversed
	// order must produce a byte-identical index.
	dir := t.TempDir()
	entries := makeFixtureEntries(120)
	inputs := writeFixtureBins(t, dir, entries)
	require.Greater(t, len(inputs), 1)

	forwardPath := filepath.Join(dir, "forward.idx")
	require.NoError(t, BuildColdIndex(context.Background(), inputs, forwardPath,
		fixtureMinLedger(), fixtureMaxLedger()))

	reversed := append([]string(nil), inputs...)
	for i, j := 0, len(reversed)-1; i < j; i, j = i+1, j-1 {
		reversed[i], reversed[j] = reversed[j], reversed[i]
	}
	reversedPath := filepath.Join(dir, "reversed.idx")
	require.NoError(t, BuildColdIndex(
		context.Background(), reversed, reversedPath, fixtureMinLedger(), fixtureMaxLedger()))

	forwardBytes, err := os.ReadFile(forwardPath)
	require.NoError(t, err)
	reversedBytes, err := os.ReadFile(reversedPath)
	require.NoError(t, err)
	assert.Equal(t, forwardBytes, reversedBytes, "index must not depend on input file order")
}

// TestBuildColdIndex_CatalogSecretChangesRoutingNotResults: two master keys route
// the same entries differently (different .idx bytes — the anti-grinding
// property), while each keyed build→query round trip still resolves every
// entry and rejects an unseen hash.
func TestBuildColdIndex_CatalogSecretChangesRoutingNotResults(t *testing.T) {
	dir := t.TempDir()
	entries := makeFixtureEntries(200)

	build := func(name string, catalogSecret []byte) string {
		binDir := filepath.Join(dir, name)
		require.NoError(t, os.MkdirAll(binDir, 0o755))
		inputs := writeFixtureBinsKeyed(t, binDir, entries, ColdIndexSecret(catalogSecret, testIndexID))
		idxPath := filepath.Join(dir, name+".idx")
		require.NoError(t, BuildColdIndex(context.Background(), inputs, idxPath,
			fixtureMinLedger(), fixtureMaxLedger()))
		return idxPath
	}
	pathA := build("a", testCatalogSecret)
	pathB := build("b", bytes.Repeat([]byte{0x5A}, 32))

	bytesA, err := os.ReadFile(pathA)
	require.NoError(t, err)
	bytesB, err := os.ReadFile(pathB)
	require.NoError(t, err)
	assert.NotEqual(t, bytesA, bytesB, "a different master key must produce different routing")

	for _, idxPath := range []string{pathA, pathB} {
		r, err := OpenColdReader(idxPath)
		require.NoError(t, err)
		for _, e := range entries {
			got, gerr := r.Get(e.hash)
			require.NoError(t, gerr)
			require.Equal(t, e.seq, got)
		}
		_, miss := r.Get(randHash(testRNG(0xabad1dea)))
		require.ErrorIs(t, miss, stores.ErrNotFound)
		require.NoError(t, r.Close())
	}
}
