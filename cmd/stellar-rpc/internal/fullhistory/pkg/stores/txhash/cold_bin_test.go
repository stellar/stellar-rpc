package txhash

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

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
	require.NoError(t, WriteColdBin(path, entries))

	got, err := ReadColdBin(path)
	require.NoError(t, err)
	assert.Equal(t, entries, got)
}

// TestColdBin_HeaderAndLayout pins the raw on-disk layout: uint64 LE count
// header followed by fixed-width (key, uint32 LE seq) entries.
func TestColdBin_HeaderAndLayout(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.bin")
	entries := []ColdEntry{
		{Key: [ColdKeySize]byte{0xaa}, Seq: 7},
		{Key: [ColdKeySize]byte{0xbb}, Seq: 8},
	}
	require.NoError(t, WriteColdBin(path, entries))

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Len(t, data, coldBinHeaderSize+2*coldBinEntrySize)
	assert.Equal(t, uint64(2), binary.LittleEndian.Uint64(data[:coldBinHeaderSize]))
	assert.Equal(t, byte(0xaa), data[coldBinHeaderSize])
	assert.Equal(t, uint32(7),
		binary.LittleEndian.Uint32(data[coldBinHeaderSize+ColdKeySize:coldBinHeaderSize+coldBinEntrySize]))
}

// TestColdBin_CreateFails forces os.Create on the destination to fail by
// pre-creating the final path as a DIRECTORY (so create returns EISDIR). The
// error must propagate; the pre-existing directory is untouched.
func TestColdBin_CreateFails(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.bin")
	require.NoError(t, os.Mkdir(path, 0o755)) // create() will hit EISDIR

	err := WriteColdBin(path, []ColdEntry{{Key: [ColdKeySize]byte{0x01}, Seq: 7}})
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
	require.NoError(t, WriteColdBin(path, entries))

	got, err := ReadColdBin(path)
	require.NoError(t, err)
	assert.Equal(t, entries, got)
}

// TestColdBin_ReadRejectsTruncated asserts the reader rejects a file whose
// header count disagrees with its size (e.g. a torn copy).
func TestColdBin_ReadRejectsTruncated(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.bin")
	require.NoError(t, WriteColdBin(path, []ColdEntry{
		{Key: [ColdKeySize]byte{0x01}, Seq: 1},
		{Key: [ColdKeySize]byte{0x02}, Seq: 2},
	}))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data[:len(data)-4], 0o644)) // tear the tail off

	_, err = ReadColdBin(path)
	require.Error(t, err)
}
