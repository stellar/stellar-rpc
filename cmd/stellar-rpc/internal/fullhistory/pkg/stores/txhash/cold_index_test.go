package txhash

import (
	"bytes"
	"context"
	"encoding/binary"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// ──────────────────────────────────────────────────────────────────
// Shared cold-store test fixtures (used by every cold *_test.go in
// this package).
// ──────────────────────────────────────────────────────────────────

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

// fixtureMinLedger is the MinLedger anchor for the fixture index group.
func fixtureMinLedger() uint32 { return fixtureBaseChunk.FirstLedger() }

// makeFixtureEntries generates n entries with unique 16-byte key
// prefixes (the width the cold index keys on), each assigned a seq
// spread deterministically across the first fixtureSpreadChunks chunks
// of the group.
func makeFixtureEntries(n int) []fixtureEntry {
	r := testRNG(uint64(n) | 0xfeed)
	entries := make([]fixtureEntry, 0, n)
	seen := make(map[[binKeySize]byte]struct{}, n)
	for len(entries) < n {
		h := randHash(r)
		var k [binKeySize]byte
		copy(k[:], h[:binKeySize])
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

// writeBinFile writes entries to a per-chunk .bin file in the input
// format BuildColdIndex consumes, sorted ascending by the 16-byte key
// prefix (the per-file ordering the build's merge requires).
func writeBinFile(t *testing.T, path string, entries []fixtureEntry) {
	t.Helper()
	sorted := append([]fixtureEntry(nil), entries...)
	sort.Slice(sorted, func(i, j int) bool {
		return bytes.Compare(sorted[i].hash[:binKeySize], sorted[j].hash[:binKeySize]) < 0
	})

	var buf bytes.Buffer
	var hdr [binHeaderSize]byte
	binary.LittleEndian.PutUint64(hdr[:], uint64(len(sorted)))
	buf.Write(hdr[:])
	var seqBuf [binSeqSize]byte
	for _, e := range sorted {
		buf.Write(e.hash[:binKeySize])
		binary.LittleEndian.PutUint32(seqBuf[:], e.seq)
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
	byChunk := make(map[chunk.ID][]fixtureEntry)
	for _, e := range entries {
		c := chunk.IDFromLedger(e.seq)
		byChunk[c] = append(byChunk[c], e)
	}
	inputs := make([]string, 0, len(byChunk))
	for c, es := range byChunk {
		p := filepath.Join(dir, c.String()+".bin")
		writeBinFile(t, p, es)
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

	idxPath := filepath.Join(dir, IndexFileName(fixtureBaseChunk))
	require.NoError(t, BuildColdIndex(context.Background(), inputs, idxPath, fixtureMinLedger()))
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

	m, err := openColdMPHF(idxPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.close() })

	assert.Equal(t, fixtureMinLedger(), m.minLedger, "MinLedger anchor must survive the build")
	for _, e := range entries {
		got, err := m.lookup(e.hash)
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

	idxPath := filepath.Join(dir, IndexFileName(fixtureBaseChunk))
	require.NoError(t, BuildColdIndex(context.Background(), inputs, idxPath, fixtureMinLedger()))

	r, err := OpenColdReader(idxPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })
	for _, e := range entries {
		got, err := r.Lookup(e.hash)
		require.NoError(t, err)
		require.Equal(t, e.seq, got)
	}
}

func TestBuildColdIndex_NoInputs(t *testing.T) {
	idxPath := filepath.Join(t.TempDir(), IndexFileName(0))
	err := BuildColdIndex(context.Background(), nil, idxPath, 2)
	require.ErrorIs(t, err, ErrEmptyBuildSet)
	assert.NoFileExists(t, idxPath)
}

func TestBuildColdIndex_AllEmptyInputs(t *testing.T) {
	// .bin files that exist but declare zero entries: the group has no
	// keys, so the build refuses with ErrEmptyBuildSet and writes no
	// index.
	dir := t.TempDir()
	chunks := []chunk.ID{5, 6}
	inputs := make([]string, 0, len(chunks))
	for _, c := range chunks {
		p := filepath.Join(dir, c.String()+".bin")
		writeBinFile(t, p, nil)
		inputs = append(inputs, p)
	}
	idxPath := filepath.Join(dir, IndexFileName(fixtureBaseChunk))
	err := BuildColdIndex(context.Background(), inputs, idxPath, fixtureMinLedger())
	require.ErrorIs(t, err, ErrEmptyBuildSet)
	assert.NoFileExists(t, idxPath)
}

func TestBuildColdIndex_MissingInputErrors(t *testing.T) {
	dir := t.TempDir()
	idxPath := filepath.Join(dir, IndexFileName(fixtureBaseChunk))
	err := BuildColdIndex(context.Background(),
		[]string{filepath.Join(dir, "00000005.bin")}, idxPath, fixtureMinLedger())
	require.Error(t, err)
	assert.NoFileExists(t, idxPath)
}

func TestBuildColdIndex_SeqBelowMinLedgerErrors(t *testing.T) {
	// An entry whose seq is below the index's MinLedger anchor would
	// underflow the payload offset; the build must reject it and leave
	// no partial index behind.
	dir := t.TempDir()
	entries := []fixtureEntry{{hash: randHash(testRNG(1)), seq: fixtureMinLedger() - 1}}
	p := filepath.Join(dir, "00000004.bin")
	writeBinFile(t, p, entries)

	idxPath := filepath.Join(dir, IndexFileName(fixtureBaseChunk))
	err := BuildColdIndex(context.Background(), []string{p}, idxPath, fixtureMinLedger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "below index MinLedger")
	assert.NoFileExists(t, idxPath)
}

func TestBuildColdIndex_PayloadOverflowErrors(t *testing.T) {
	// An offset past the 3-byte payload budget must be rejected at
	// build time rather than silently truncated.
	dir := t.TempDir()
	minLedger := uint32(2)
	overflowSeq := minLedger + uint32(coldPayloadMax) + 1
	entries := []fixtureEntry{{hash: randHash(testRNG(2)), seq: overflowSeq}}
	p := filepath.Join(dir, "99999999.bin")
	writeBinFile(t, p, entries)

	idxPath := filepath.Join(dir, IndexFileName(0))
	err := BuildColdIndex(context.Background(), []string{p}, idxPath, minLedger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "payload budget")
	assert.NoFileExists(t, idxPath)
}

func TestBuildColdIndex_TruncatedFileErrors(t *testing.T) {
	// Header OVERSTATES the count (claims more entries than the body
	// holds): the open-time size cross-check rejects it; no index.
	dir := t.TempDir()
	var buf bytes.Buffer
	var hdr [binHeaderSize]byte
	binary.LittleEndian.PutUint64(hdr[:], 5) // claim 5
	buf.Write(hdr[:])
	// ...but write only one entry.
	buf.Write(make([]byte, binEntrySize))
	p := filepath.Join(dir, "00000005.bin")
	require.NoError(t, os.WriteFile(p, buf.Bytes(), 0o600))

	idxPath := filepath.Join(dir, IndexFileName(fixtureBaseChunk))
	err := BuildColdIndex(context.Background(), []string{p}, idxPath, fixtureMinLedger())
	require.Error(t, err)
	assert.NoFileExists(t, idxPath)
}

func TestBuildColdIndex_HeaderUndercountErrors(t *testing.T) {
	// Header UNDERSTATES the count (file holds more entries than it
	// declares). Without the open-time size cross-check this would
	// silently drop the trailing entries; it must error instead.
	dir := t.TempDir()
	var buf bytes.Buffer
	var hdr [binHeaderSize]byte
	binary.LittleEndian.PutUint64(hdr[:], 1) // declare 1...
	buf.Write(hdr[:])
	buf.Write(make([]byte, binEntrySize*3)) // ...but write 3 entries
	p := filepath.Join(dir, "00000005.bin")
	require.NoError(t, os.WriteFile(p, buf.Bytes(), 0o600))

	idxPath := filepath.Join(dir, IndexFileName(fixtureBaseChunk))
	err := BuildColdIndex(context.Background(), []string{p}, idxPath, fixtureMinLedger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bytes, want")
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
	require.NoError(t, BuildColdIndex(context.Background(), inputs, forwardPath, fixtureMinLedger()))

	reversed := append([]string(nil), inputs...)
	for i, j := 0, len(reversed)-1; i < j; i, j = i+1, j-1 {
		reversed[i], reversed[j] = reversed[j], reversed[i]
	}
	reversedPath := filepath.Join(dir, "reversed.idx")
	require.NoError(t, BuildColdIndex(context.Background(), reversed, reversedPath, fixtureMinLedger()))

	forwardBytes, err := os.ReadFile(forwardPath)
	require.NoError(t, err)
	reversedBytes, err := os.ReadFile(reversedPath)
	require.NoError(t, err)
	assert.Equal(t, forwardBytes, reversedBytes, "index must not depend on input file order")
}
