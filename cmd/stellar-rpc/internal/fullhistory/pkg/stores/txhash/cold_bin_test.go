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

	// No stray temp file remains after a successful publish.
	_, statErr := os.Stat(path + ".tmp")
	assert.True(t, os.IsNotExist(statErr), "temp file must not survive a successful publish")
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
	require.Equal(t, coldBinHeaderSize+2*coldBinEntrySize, len(data))
	assert.Equal(t, uint64(2), binary.LittleEndian.Uint64(data[:coldBinHeaderSize]))
	assert.Equal(t, byte(0xaa), data[coldBinHeaderSize])
	assert.Equal(t, uint32(7),
		binary.LittleEndian.Uint32(data[coldBinHeaderSize+ColdKeySize:coldBinHeaderSize+coldBinEntrySize]))
}

// TestColdBin_CreateFails forces os.Create to fail by pre-creating the
// "<path>.tmp" sibling as a DIRECTORY (so create returns EISDIR). The error
// must propagate and no final .bin must exist.
func TestColdBin_CreateFails(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.bin")
	tmp := path + ".tmp"
	require.NoError(t, os.Mkdir(tmp, 0o755)) // create() will hit EISDIR

	err := WriteColdBin(path, []ColdEntry{{Key: [ColdKeySize]byte{0x01}, Seq: 7}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "create")

	// No final .bin produced.
	_, statErr := os.Stat(path)
	require.True(t, os.IsNotExist(statErr), "no final .bin on create failure")
}

// TestColdBin_RenameFails forces os.Rename to fail by pre-creating the
// FINAL path as a non-empty DIRECTORY (rename onto a non-empty dir fails).
// The error must propagate, the temp file must be cleaned up, and no valid
// final .bin (file) should remain.
func TestColdBin_RenameFails(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.bin")
	// Final path is a non-empty directory → os.Rename(tmp, path) fails.
	require.NoError(t, os.Mkdir(path, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(path, "blocker"), []byte("x"), 0o644))

	err := WriteColdBin(path, []ColdEntry{{Key: [ColdKeySize]byte{0x02}, Seq: 9}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "rename")

	// The temp file must have been removed (no stray .tmp).
	_, statErr := os.Stat(path + ".tmp")
	require.True(t, os.IsNotExist(statErr), "leftover .tmp after rename failure")

	// The final path is still the (pre-existing) directory, not a published file.
	info, statErr := os.Stat(path)
	require.NoError(t, statErr)
	require.True(t, info.IsDir(), "rename must not have published a .bin file")
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
