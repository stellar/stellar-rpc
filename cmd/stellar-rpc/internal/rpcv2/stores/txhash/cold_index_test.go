package txhash

import (
	"bytes"
	"context"
	"maps"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
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
// starting at fixtureFirstChunk, so the build reads many files. The window
// starts at fixtureWindowChunk, below the first retained chunk, as it does
// once the retention floor has risen into the window: a reader that returned
// the raw payload, or counted it from the first retained ledger, would be
// thousands of ledgers off.
const (
	fixtureWindowChunk  = chunk.ID(3)
	fixtureFirstChunk   = chunk.ID(5)
	fixtureSpreadChunks = 6
)

// fixtureBin is what the fixture .bin of chunk c records.
func fixtureBin(c chunk.ID) ColdBin {
	return ColdBin{Secret: testSecret(), Base: fixtureMinLedger(), Chunk: c}
}

// indexFileName names a scratch cold-index file for firstChunk. The on-disk
// layout is geometry.Layout.TxHashIndexFilePath; tests only need a stable name
// in their own TempDir, so this reuses chunk.ID's zero-padded String().
func indexFileName(firstChunk chunk.ID) string { return firstChunk.String() + "-txhash.idx" }

// fixtureMinLedger is the first ledger of the fixture window, which the
// payloads count from.
func fixtureMinLedger() uint32 { return fixtureWindowChunk.FirstLedger() }

// fixtureMaxLedger is the coverage upper bound for the fixture group:
// the last ledger of its highest chunk. Every fixture seq is <= it.
func fixtureMaxLedger() uint32 {
	return (fixtureFirstChunk + chunk.ID(fixtureSpreadChunks-1)).LastLedger()
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
		seq := (fixtureFirstChunk + chunk.ID(chunkOffset)).FirstLedger() + inChunk
		entries = append(entries, fixtureEntry{hash: h, seq: seq})
	}
	return entries
}

// writeFixtureBins partitions entries into per-chunk .bin files under dir
// (one file per chunk that received an entry) and returns their paths in
// chunk order. The chunk a given entry belongs to is derived from its seq.
func writeFixtureBins(t *testing.T, dir string, entries []fixtureEntry) []string {
	t.Helper()
	return writeFixtureBinsAt(t, dir, entries, testSecret(), fixtureMinLedger())
}

// writeFixtureBinsAt is writeFixtureBins under an explicit routing secret and
// payload base. Each stored key is the secret-keyed routing key of the entry's
// hash prefix, mirroring ingest-time keying.
func writeFixtureBinsAt(
	t *testing.T, dir string, entries []fixtureEntry, secret [stores.SecretLen]byte, base uint32,
) []string {
	t.Helper()
	byChunk := make(map[chunk.ID][]ColdEntry)
	for _, e := range entries {
		c := chunk.IDFromLedger(e.seq)
		byChunk[c] = append(byChunk[c], ColdEntry{Key: stores.BlindKey(secret, e.hash[:ColdKeySize]), Seq: e.seq})
	}
	inputs := make([]string, 0, len(byChunk))
	for _, c := range slices.Sorted(maps.Keys(byChunk)) {
		p := filepath.Join(dir, ColdBinName(c))
		writeColdBin(t, p, ColdBin{Secret: secret, Base: base, Chunk: c}, byChunk[c])
		inputs = append(inputs, p)
	}
	return inputs
}

// buildColdFixture builds a cold txhash index over n entries spread
// across several per-chunk .bin files, returning the index path and
// the entries (for round-trip assertions). It drives the real
// BuildColdIndex path.
func buildColdFixture(t *testing.T, n int) (string, []fixtureEntry) {
	t.Helper()
	dir := t.TempDir()
	entries := makeFixtureEntries(n)
	inputs := writeFixtureBins(t, dir, entries)
	require.Greater(t, len(inputs), 1, "fixture should span multiple .bin files")

	idxPath := filepath.Join(dir, indexFileName(fixtureFirstChunk))
	require.NoError(t, BuildColdIndex(context.Background(), inputs, idxPath,
		fixtureMinLedger(), fixtureMaxLedger()))
	return idxPath, entries
}

// ──────────────────────────────────────────────────────────────────
// BuildColdIndex tests.
// ──────────────────────────────────────────────────────────────────

func TestBuildColdIndex_RoundTrip(t *testing.T) {
	// Builds from multiple .bin files and verifies every entry's
	// absolute seq round-trips through the index — the core
	// build→query contract over a chunk group.
	idxPath, entries := buildColdFixture(t, 300)

	info, err := os.Stat(idxPath)
	require.NoError(t, err)
	assert.Positive(t, info.Size(), "index file must be non-empty")

	r, err := OpenColdReader(idxPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })

	assert.Equal(t, fixtureMinLedger(), r.MinLedger(), "MinLedger must survive the build")
	assert.Equal(t, fixtureMaxLedger(), r.MaxLedger(), "MaxLedger must survive the build")
	for _, e := range entries {
		got, err := r.Get(e.hash)
		require.NoError(t, err)
		assert.Equal(t, e.seq, got, "absolute ledgerSeq for hash %x must round-trip", e.hash[:8])
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
	assert.Equal(t, wantSecret, r.meta.Secret, "empty index metadata secret")
	_, err = r.Get(randHash(testRNG(99)))
	require.ErrorIs(t, err, stores.ErrNotFound)
}

func TestBuildColdIndex_NoInputs(t *testing.T) {
	// The build reads the index secret and the payload base from the .bin
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
	idxPath := filepath.Join(dir, indexFileName(fixtureFirstChunk))
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
		p := filepath.Join(dir, ColdBinName(c))
		writeColdBin(t, p, fixtureBin(c), nil)
		inputs = append(inputs, p)
	}
	idxPath := filepath.Join(dir, indexFileName(fixtureFirstChunk))
	require.NoError(t, BuildColdIndex(context.Background(), inputs, idxPath,
		fixtureMinLedger(), fixtureMaxLedger()))
	assertEmptyColdIndex(t, idxPath, fixtureMinLedger(), fixtureMaxLedger(), testSecret())
}

func TestBuildColdIndex_MissingInputErrors(t *testing.T) {
	dir := t.TempDir()
	idxPath := filepath.Join(dir, indexFileName(fixtureFirstChunk))
	err := BuildColdIndex(context.Background(),
		[]string{filepath.Join(dir, "00000005.bin")}, idxPath, fixtureMinLedger(), fixtureMaxLedger())
	require.Error(t, err)
	assert.NoFileExists(t, idxPath)
}

// The inputs of one index share its secret, so a .bin keyed differently from
// the first fails the build rather than produce an index some of whose keys no
// query can hit.
func TestBuildColdIndex_RejectsMixedSecrets(t *testing.T) {
	for _, tc := range []struct {
		name   string
		second ColdBin
		want   string
	}{
		{"secret", ColdBin{Secret: [stores.SecretLen]byte{0xb2}, Chunk: 6}, "different index secret"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			first := filepath.Join(dir, ColdBinName(5))
			second := filepath.Join(dir, ColdBinName(6))
			writeColdBin(t, first, fixtureBin(5), nil)
			writeColdBin(t, second, tc.second, nil)
			idxPath := filepath.Join(dir, "out.idx")
			err := BuildColdIndex(context.Background(), []string{first, second}, idxPath,
				fixtureMinLedger(), fixtureMaxLedger())
			require.ErrorContains(t, err, tc.want)
			assert.NoFileExists(t, idxPath)
		})
	}
}

func TestBuildColdIndex_ChunkOutsideCoverageErrors(t *testing.T) {
	// The writers checked each key's ledger against its .bin's chunk, so a
	// chunk inside the coverage keeps every key inside it.
	for _, c := range []chunk.ID{fixtureWindowChunk - 1, fixtureFirstChunk + fixtureSpreadChunks} {
		t.Run(c.String(), func(t *testing.T) {
			dir := t.TempDir()
			p := filepath.Join(dir, ColdBinName(c))
			writeColdBin(t, p, fixtureBin(c), nil)
			idxPath := filepath.Join(dir, indexFileName(fixtureFirstChunk))
			err := BuildColdIndex(context.Background(), []string{p}, idxPath,
				fixtureMinLedger(), fixtureMaxLedger())
			require.ErrorContains(t, err, "outside the coverage")
			assert.NoFileExists(t, idxPath)
		})
	}
}

func TestBuildColdIndex_CoverageOutsideThePayloadsErrors(t *testing.T) {
	// Every ledger up to maxLedger must have a payload counted from
	// minLedger, so a coverage past the 3-byte payload's reach fails.
	first := chunk.ID(0).FirstLedger()
	dir := t.TempDir()
	p := filepath.Join(dir, ColdBinName(0))
	writeColdBin(t, p, ColdBin{Secret: testSecret(), Base: first, Chunk: 0}, nil)
	idxPath := filepath.Join(dir, indexFileName(0))
	err := BuildColdIndex(context.Background(), []string{p}, idxPath, first, first+uint32(coldPayloadMax)+1)
	require.ErrorContains(t, err, "cannot reach ledger")
	assert.NoFileExists(t, idxPath)
}

func TestBuildColdIndex_CallerOptsCannotOverrideFormat(t *testing.T) {
	// A caller opt that tries to repoint the cold format (here, bogus
	// metadata) must not win — the format options go last, so the reader
	// still recovers the real coverage and resolves seqs correctly.
	dir := t.TempDir()
	entries := makeFixtureEntries(64)
	inputs := writeFixtureBins(t, dir, entries)
	idxPath := filepath.Join(dir, indexFileName(fixtureFirstChunk))
	err := BuildColdIndex(context.Background(), inputs, idxPath,
		fixtureMinLedger(), fixtureMaxLedger(),
		streamhash.WithMetadata(EncodeColdMetadata(ColdMetadata{MinLedger: 7, MaxLedger: 7, Secret: testSecret()})))
	require.NoError(t, err)

	r, err := OpenColdReader(idxPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })

	assert.Equal(t, fixtureMinLedger(), r.MinLedger(), "pinned metadata must win over a caller's WithMetadata")
	assert.Equal(t, fixtureMaxLedger(), r.MaxLedger())
	for _, e := range entries {
		got, err := r.Get(e.hash)
		require.NoError(t, err)
		assert.Equal(t, e.seq, got)
	}
}

func TestBuildColdIndex_TruncatedFileErrors(t *testing.T) {
	// A .bin cut short of its header's record count, as a torn copy would
	// be, fails the build and leaves no index.
	dir := t.TempDir()
	inputs := writeFixtureBins(t, dir, makeFixtureEntries(60))
	data, err := os.ReadFile(inputs[0])
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(inputs[0], data[:len(data)-1], 0o600))

	idxPath := filepath.Join(dir, indexFileName(fixtureFirstChunk))
	err = BuildColdIndex(context.Background(), inputs, idxPath, fixtureMinLedger(), fixtureMaxLedger())
	require.ErrorIs(t, err, streamhash.ErrTruncatedFile)
	assert.NoFileExists(t, idxPath)
}

func TestBuildColdIndex_InputOrderIndependent(t *testing.T) {
	// The index is a function of the keys, so feeding the same files in
	// reversed order must produce a byte-identical index.
	dir := t.TempDir()
	entries := makeFixtureEntries(120)
	inputs := writeFixtureBins(t, dir, entries)
	require.Greater(t, len(inputs), 1)

	forwardPath := filepath.Join(dir, "forward.idx")
	require.NoError(t, BuildColdIndex(context.Background(), inputs, forwardPath,
		fixtureMinLedger(), fixtureMaxLedger()))

	reversed := slices.Clone(inputs)
	slices.Reverse(reversed)
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
		inputs := writeFixtureBinsAt(t, binDir, entries, ColdIndexSecret(catalogSecret, testIndexID), fixtureMinLedger())
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
