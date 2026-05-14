package ledger

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/packfile"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/zstd"
)

// newTestEncoderFactory returns a fresh zstd compressor per worker.
// Production code wires the same shape; tests reuse it so the
// codec round-trips realistically.
func newTestEncoderFactory() func() packfile.RecordEncoder {
	return func() packfile.RecordEncoder { return zstd.NewCompressor() }
}

func openTestColdWriter(t *testing.T, firstSeq uint32) (*ColdWriter, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ledgers.pack")
	w, err := NewColdWriter(path, firstSeq, newTestEncoderFactory(), silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	return w, path
}

func TestNewColdWriter_ValidatesInputs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.pack")
	enc := newTestEncoderFactory()
	log := silentLogger()

	_, err := NewColdWriter("", 0, enc, log)
	require.ErrorIs(t, err, rocksdb.ErrInvalidConfig)

	_, err = NewColdWriter(path, 0, nil, log)
	require.ErrorIs(t, err, rocksdb.ErrInvalidConfig)

	_, err = NewColdWriter(path, 0, enc, nil)
	assert.ErrorIs(t, err, rocksdb.ErrInvalidConfig)
}

func TestColdWriter_AppendOneLedgerRoundTrip(t *testing.T) {
	const firstSeq uint32 = 1_234_567
	lcm, _ := makeRandomLedgerCloseMeta(firstSeq, 2)
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)

	w, path := openTestColdWriter(t, firstSeq)
	require.NoError(t, w.AppendLedger(firstSeq, raw))
	require.NoError(t, w.Finalize())

	c, err := OpenColdStore(path, zstd.NewDecompressor(), silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	got, err := c.GetLedgerRaw(firstSeq)
	require.NoError(t, err)
	assert.Equal(t, raw, got)
}

func TestColdWriter_AppendRejectsGapAndKeepsCounter(t *testing.T) {
	const firstSeq uint32 = 100
	w, path := openTestColdWriter(t, firstSeq)

	require.NoError(t, w.AppendLedger(100, []byte("a")))
	// Expected next is 101; a gap to 103 must be rejected.
	require.Error(t, w.AppendLedger(103, []byte("c")))
	// Counter did NOT advance: 101 is still the expected next.
	require.NoError(t, w.AppendLedger(101, []byte("b")))
	require.NoError(t, w.Finalize())

	c, err := OpenColdStore(path, zstd.NewDecompressor(), silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	assert.Equal(t, uint32(100), c.FirstSeq())
	assert.Equal(t, uint32(101), c.LastSeq())
}

func TestColdWriter_AppendRejectsOutOfOrder(t *testing.T) {
	const firstSeq uint32 = 500
	w, _ := openTestColdWriter(t, firstSeq)

	require.NoError(t, w.AppendLedger(500, []byte("a")))
	require.NoError(t, w.AppendLedger(501, []byte("b")))
	// seq < expected next (502) must be rejected.
	require.Error(t, w.AppendLedger(500, []byte("dup")))
	require.Error(t, w.AppendLedger(499, []byte("before-first")))
}

func TestColdWriter_FinalizeEmitsTrailerAndAppData(t *testing.T) {
	const firstSeq uint32 = 9_876_543
	const n uint32 = 10
	w, path := openTestColdWriter(t, firstSeq)
	for i := range n {
		require.NoError(t, w.AppendLedger(firstSeq+i, []byte{byte(i)}))
	}
	require.NoError(t, w.Finalize())

	// Inspect via raw packfile.Open — no ColdStore involvement.
	r := packfile.Open(path, packfile.ReaderOptions{RecordDecoder: zstd.NewDecompressor()})
	t.Cleanup(func() { _ = r.Close() })

	total, err := r.TotalItems()
	require.NoError(t, err)
	assert.Equal(t, int(n), total)

	ad, err := r.AppData()
	require.NoError(t, err)
	require.Len(t, ad, 4)
	assert.Equal(t, firstSeq, binary.BigEndian.Uint32(ad))

	tr, err := r.Trailer()
	require.NoError(t, err)
	assert.True(t, tr.HasContentHash)
}

func TestColdWriter_CloseBeforeFinalizeRemovesFile(t *testing.T) {
	w, path := openTestColdWriter(t, 1)
	require.NoError(t, w.AppendLedger(1, []byte("partial")))
	require.NoError(t, w.Close())

	_, err := os.Stat(path)
	assert.True(t, os.IsNotExist(err), "partial .pack must be removed; got err=%v", err)
}

func TestColdWriter_CloseAfterFinalizeIsNoop(t *testing.T) {
	w, path := openTestColdWriter(t, 1)
	require.NoError(t, w.AppendLedger(1, []byte("v")))
	require.NoError(t, w.Finalize())

	// Close after Finalize: no-op, no error.
	// Twice for good measure.
	assert.NoError(t, w.Close())
	assert.NoError(t, w.Close())

	// File still on disk (Finalize wrote a complete pack).
	_, err := os.Stat(path)
	assert.NoError(t, err)
}

func TestColdWriter_AppendAfterCloseReturnsErrStoreClosed(t *testing.T) {
	w, _ := openTestColdWriter(t, 1)
	require.NoError(t, w.Close())
	err := w.AppendLedger(1, []byte("v"))
	require.ErrorIs(t, err, stores.ErrStoreClosed)

	// Finalize after Close: same sentinel.
	assert.ErrorIs(t, w.Finalize(), stores.ErrStoreClosed)
}

func TestColdWriter_AppendAfterFinalizeReturnsErrStoreClosed(t *testing.T) {
	w, _ := openTestColdWriter(t, 1)
	require.NoError(t, w.AppendLedger(1, []byte("v")))
	require.NoError(t, w.Finalize())

	// Finalize sets the closed fence — Append should reject.
	require.ErrorIs(t, w.AppendLedger(2, []byte("v")), stores.ErrStoreClosed)
	// Re-Finalize: same sentinel.
	assert.ErrorIs(t, w.Finalize(), stores.ErrStoreClosed)
}
