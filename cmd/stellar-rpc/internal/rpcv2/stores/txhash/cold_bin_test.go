package txhash

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

// testBinSecret is the index secret the .bin writer tests key their entries
// with; the header records it and the reader/build validate it.
var testBinSecret = [stores.SecretLen]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

// readColdBin reads back a cold .bin file, validating its header count against
// the file size via the shared coldBinCount. It is the test-side mirror of the
// .bin codec: production consumes .bin files through the index builder's
// streaming pre-scan, never a full read-back, so this read path lives only in
// the tests that pin the writer's output.
func readColdBin(path string) ([]ColdEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("txhash: open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	br := bufio.NewReaderSize(f, 1<<20)
	var header [coldBinHeaderSize]byte
	if _, err := io.ReadFull(br, header[:]); err != nil {
		return nil, fmt.Errorf("txhash: read header of %s: %w", path, err)
	}
	count := binary.LittleEndian.Uint64(header[coldBinPreludeSize : coldBinPreludeSize+coldBinCountSize])

	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("txhash: stat %s: %w", path, err)
	}
	if _, err := coldBinCount(path, info.Size(), count); err != nil {
		return nil, err
	}

	entries := make([]ColdEntry, count)
	var entryBuf [coldBinEntrySize]byte
	for i := range entries {
		if _, err := io.ReadFull(br, entryBuf[:]); err != nil {
			return nil, fmt.Errorf("txhash: read entry %d of %s: %w", i, path, err)
		}
		copy(entries[i].Key[:], entryBuf[:ColdKeySize])
		p := entryBuf[ColdKeySize:]
		entries[i].Seq = uint32(p[0]) | uint32(p[1])<<8 | uint32(p[2])<<16
	}
	return entries, nil
}

// TestColdBin_RoundTrip writes entries and reads them back through the
// matching reader, pinning the writer/reader codec to each other.
func TestColdBin_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ColdBinName(chunk.ID(0)))
	entries := []ColdEntry{
		{Key: [ColdKeySize]byte{0x01}, Seq: 10},
		{Key: [ColdKeySize]byte{0x02}, Seq: 11},
		{Key: [ColdKeySize]byte{0x02}, Seq: 12}, // duplicate truncated key preserved
	}
	require.NoError(t, WriteColdBin(path, testBinSecret, 0, 0, entries))

	got, err := readColdBin(path)
	require.NoError(t, err)
	assert.Equal(t, entries, got)
}

// TestColdBin_HeaderAndLayout pins the raw on-disk layout, streamhash's sorted
// file: the "STSF" magic, version, payload size, reserved byte, uint64 LE count,
// metadata length and metadata (version, chunk ID, secret), zero padding to
// 4096 bytes, then fixed-width (key, 3-byte LE payload) entries.
func TestColdBin_HeaderAndLayout(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.bin")
	entries := []ColdEntry{
		{Key: [ColdKeySize]byte{0xaa}, Seq: 7},
		{Key: [ColdKeySize]byte{0xbb}, Seq: 8},
	}
	require.NoError(t, WriteColdBin(path, testBinSecret, 9, 5, entries))

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Len(t, data, 4096+2*19)
	assert.Equal(t, []byte("FSTS"), data[:4], "magic 0x53545346 little-endian")
	assert.Equal(t, []byte{1, 0, 3, 0}, data[4:8], "version, payload size, reserved")
	assert.Equal(t, uint64(2), binary.LittleEndian.Uint64(data[8:16]))
	assert.Equal(t, uint32(21), binary.LittleEndian.Uint32(data[16:20]), "metadata length")
	assert.Equal(t, []byte{1, 9, 0, 0, 0}, data[20:25], "metadata version and chunk ID")
	assert.Equal(t, testBinSecret[:], data[25:41], "secret recorded in the metadata")
	assert.Equal(t, make([]byte, 4096-41), data[41:4096], "zero padding")
	assert.Equal(t, byte(0xaa), data[4096])
	assert.Equal(t, []byte{2, 0, 0}, data[4096+16:4096+19], "payload = seq - base")
}

// TestColdBin_ScanRejectsForeignHeader pins the prelude refusals in BOTH .bin
// consumers, the pre-scan and the self-defending merge reader: a foreign magic,
// a newer version byte, and a set reserved byte each fail loudly instead of
// being misread as entry data.
func TestColdBin_ScanRejectsForeignHeader(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.bin")
	require.NoError(t, WriteColdBin(path, testBinSecret, 0, 0, []ColdEntry{{Key: [ColdKeySize]byte{1}, Seq: 2}}))
	data, err := os.ReadFile(path)
	require.NoError(t, err)

	cases := []struct {
		name   string
		mutate func([]byte)
		want   string
	}{
		{"bad magic", func(b []byte) { copy(b, "JUNK") }, "not a cold txhash .bin"},
		{"newer version", func(b []byte) { b[4] = 2 }, "written by a newer stellar-rpc"},
		{"reserved byte set", func(b []byte) { b[7] = 0x01 }, "unexpected .bin header"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bad := append([]byte(nil), data...)
			tc.mutate(bad)
			badPath := filepath.Join(dir, tc.name+".bin")
			require.NoError(t, os.WriteFile(badPath, bad, 0o600))
			_, _, err := scanBinHeader(badPath)
			require.ErrorContains(t, err, tc.want)
			_, err = newFileReader(badPath, 0)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

// TestColdBin_RejectsLedgerBelowBase pins the writer's refusal of a ledger its
// payload can't hold.
func TestColdBin_RejectsLedgerBelowBase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.bin")
	err := WriteColdBin(path, testBinSecret, 0, 10, []ColdEntry{{Key: [ColdKeySize]byte{1}, Seq: 9}})
	require.ErrorContains(t, err, "outside the window")
}

// TestColdBin_CreateFails forces os.Create on the destination to fail by
// pre-creating the final path as a DIRECTORY (so create returns EISDIR). The
// error must propagate; the pre-existing directory is untouched.
func TestColdBin_CreateFails(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.bin")
	require.NoError(t, os.Mkdir(path, 0o755)) // create() will hit EISDIR

	err := WriteColdBin(path, testBinSecret, 0, 0, []ColdEntry{{Key: [ColdKeySize]byte{0x01}, Seq: 7}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "create")

	info, statErr := os.Stat(path)
	require.NoError(t, statErr)
	require.True(t, info.IsDir(), "destination untouched on create failure")
}

// TestColdBin_OverwritesPriorAttempt pins the in-place overwrite semantics:
// WriteColdBin truncates whatever a prior attempt left at the destination
// (os.Create is O_TRUNC) — under the artifact model, a stale or partial file
// from a failed run is inert scratch and the retry's overwrite IS the
// cleanup, so there is no tmp+rename step.
func TestColdBin_OverwritesPriorAttempt(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.bin")
	// A prior attempt left garbage longer than the new file, so a
	// non-truncating write would leave trailing bytes behind.
	require.NoError(t, os.WriteFile(path, make([]byte, 4096), 0o600))

	entries := []ColdEntry{{Key: [ColdKeySize]byte{0x03}, Seq: 21}}
	require.NoError(t, WriteColdBin(path, testBinSecret, 0, 0, entries))

	got, err := readColdBin(path)
	require.NoError(t, err)
	assert.Equal(t, entries, got)
}

// TestColdBin_ReadRejectsTruncated asserts the reader rejects a file whose
// header count disagrees with its size (e.g. a torn copy).
func TestColdBin_ReadRejectsTruncated(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.bin")
	require.NoError(t, WriteColdBin(path, testBinSecret, 0, 0, []ColdEntry{
		{Key: [ColdKeySize]byte{0x01}, Seq: 1},
		{Key: [ColdKeySize]byte{0x02}, Seq: 2},
	}))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data[:len(data)-4], 0o644)) // tear the tail off

	_, err = readColdBin(path)
	require.Error(t, err)
}

// TestBuildColdIndex_RejectsMixedSecrets pins the H3 guard: BuildColdIndex
// adopts the secret from the .bin headers and requires every input in a window
// to carry the same one. Inputs blinded under different secrets (a catalog
// remint or geometry drift between ingest passes) must fail the build loudly
// rather than produce an index no query can hit.
func TestBuildColdIndex_RejectsMixedSecrets(t *testing.T) {
	dir := t.TempDir()
	secretA := [stores.SecretLen]byte{0xa1}
	secretB := [stores.SecretLen]byte{0xb2}
	binA := filepath.Join(dir, "a.bin")
	binB := filepath.Join(dir, "b.bin")
	require.NoError(t, WriteColdBin(binA, secretA, 0, 0, []ColdEntry{{Key: [ColdKeySize]byte{0x01}, Seq: 1}}))
	require.NoError(t, WriteColdBin(binB, secretB, 0, 0, []ColdEntry{{Key: [ColdKeySize]byte{0x02}, Seq: 2}}))

	err := BuildColdIndex(context.Background(), []string{binA, binB}, filepath.Join(dir, "out.idx"), 0, 100)
	require.Error(t, err)
	require.Contains(t, err.Error(), "different index secret")
}
