package txhash

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/streamhash"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

// testBinSecret is the index secret the .bin tests key their entries with.
var testBinSecret = [stores.SecretLen]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

// writeColdBin writes entries, in any order, as the .bin at path with bin in
// its header, as the writers do.
func writeColdBin(t *testing.T, path string, bin ColdBin, entries []ColdEntry) {
	t.Helper()
	sorted := slices.Clone(entries)
	slices.SortFunc(sorted, func(a, b ColdEntry) int { return bytes.Compare(a.Key[:], b.Key[:]) })
	w, err := streamhash.NewSortedFileWriter(path, bin.SortedFileConfig(0))
	require.NoError(t, err)
	defer w.Close()
	for _, e := range sorted {
		payload, err := bin.Payload(e.Seq)
		require.NoError(t, err)
		require.NoError(t, w.Add(e.Key[:], payload))
	}
	require.NoError(t, w.Finish())
}

// A .bin is a sorted file of 19-byte records, a key and its ledger's 3-byte
// offset from the payload base, whose metadata records the chunk and the
// secret.
func TestColdBin_Layout(t *testing.T) {
	dir := t.TempDir()
	bin := ColdBin{Secret: testBinSecret, Base: chunk.ID(3).FirstLedger(), Chunk: 5}
	entries := []ColdEntry{
		{Key: [ColdKeySize]byte{0xbb}, Seq: bin.Chunk.FirstLedger() + 7},
		{Key: [ColdKeySize]byte{0xaa}, Seq: bin.Chunk.LastLedger()},
	}
	path, empty := filepath.Join(dir, "full.bin"), filepath.Join(dir, "empty.bin")
	writeColdBin(t, path, bin, entries)
	writeColdBin(t, empty, bin, nil)

	got, err := ReadColdBinHeader(path)
	require.NoError(t, err)
	require.Equal(t, ColdBin{Secret: bin.Secret, Chunk: bin.Chunk}, got)
	h, err := streamhash.ReadSortedFileHeader(path)
	require.NoError(t, err)
	require.Equal(t, ColdPayloadSize, h.PayloadSize)
	require.Len(t, h.Metadata, 21)
	require.Equal(t, coldBinVersion, h.Metadata[0])
	require.Equal(t, uint32(bin.Chunk), binary.LittleEndian.Uint32(h.Metadata[1:5]))
	require.Equal(t, testBinSecret[:], h.Metadata[5:21])

	fullInfo, err := os.Stat(path)
	require.NoError(t, err)
	emptyInfo, err := os.Stat(empty)
	require.NoError(t, err)
	require.EqualValues(t, 2*19, fullInfo.Size()-emptyInfo.Size())

	r, err := streamhash.OpenSortedFile(path)
	require.NoError(t, err)
	defer r.Close()
	for _, want := range []ColdEntry{entries[1], entries[0]} {
		key, payload, err := r.Next()
		require.NoError(t, err)
		require.Equal(t, want.Key, key)
		require.Equal(t, uint64(want.Seq-bin.Base), payload)
	}
	_, _, err = r.Next()
	require.ErrorIs(t, err, io.EOF)
}

func TestColdBin_PayloadRefusesALedgerOutsideTheChunk(t *testing.T) {
	bin := ColdBin{Base: chunk.ID(3).FirstLedger(), Chunk: 5}
	payload, err := bin.Payload(bin.Chunk.FirstLedger())
	require.NoError(t, err)
	require.Equal(t, uint64(2*chunk.LedgersPerChunk), payload)
	for _, seq := range []uint32{bin.Chunk.FirstLedger() - 1, bin.Chunk.LastLedger() + 1} {
		_, err := bin.Payload(seq)
		require.ErrorContains(t, err, "outside chunk 00000005")
	}
}

func TestReadColdBinHeader_Refusals(t *testing.T) {
	dir := t.TempDir()
	write := func(name string, cfg streamhash.SortedFileConfig) string {
		path := filepath.Join(dir, name)
		w, err := streamhash.NewSortedFileWriter(path, cfg)
		require.NoError(t, err)
		require.NoError(t, w.Finish())
		return path
	}
	good := ColdBin{Secret: testBinSecret, Base: 2, Chunk: 0}.SortedFileConfig(0)

	wide := good
	wide.PayloadSize = 4
	newer := good
	newer.Metadata = append([]byte{coldBinVersion + 1}, good.Metadata[1:]...)
	short := good
	short.Metadata = good.Metadata[:20]
	for _, tc := range []struct {
		name string
		cfg  streamhash.SortedFileConfig
		want string
	}{
		{"another payload size", wide, "4-byte payloads"},
		{"a newer metadata version", newer, "written by a newer stellar-rpc"},
		{"short metadata", short, "metadata is 20 bytes"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ReadColdBinHeader(write(tc.name+".bin", tc.cfg))
			require.ErrorContains(t, err, tc.want)
		})
	}

	foreign := filepath.Join(dir, "foreign.bin")
	require.NoError(t, os.WriteFile(foreign, make([]byte, 64), 0o600))
	_, err := ReadColdBinHeader(foreign)
	require.ErrorIs(t, err, streamhash.ErrInvalidMagic)
}
