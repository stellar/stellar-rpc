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

func openTestColdWriter(t *testing.T, firstSeq uint32) (*ColdWriter, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ledgers.pack")
	w, err := NewColdWriter(path, firstSeq, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	return w, path
}

func TestNewColdWriter_ValidatesInputs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.pack")
	log := silentLogger()

	_, err := NewColdWriter("", 0, log)
	require.ErrorIs(t, err, rocksdb.ErrInvalidConfig)

	_, err = NewColdWriter(path, 0, nil)
	assert.ErrorIs(t, err, rocksdb.ErrInvalidConfig)
}

func TestColdWriter_AppendRejectsGapAndKeepsCounter(t *testing.T) {
	const firstSeq uint32 = 100
	w, path := openTestColdWriter(t, firstSeq)

	require.NoError(t, w.AppendLedger(100, []byte("a")))
	require.Error(t, w.AppendLedger(103, []byte("c")))
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

	r := packfile.Open(path, packfile.ReaderOptions{})
	t.Cleanup(func() { _ = r.Close() })

	total, err := r.TotalItems()
	require.NoError(t, err)
	assert.Equal(t, int(n), total)

	ad, err := r.AppData()
	require.NoError(t, err)
	require.Len(t, ad, 4)
	assert.Equal(t, firstSeq, binary.BigEndian.Uint32(ad))
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

	assert.NoError(t, w.Close())
	assert.NoError(t, w.Close())

	_, err := os.Stat(path)
	assert.NoError(t, err)
}

func TestColdWriter_AppendAfterCloseReturnsErrStoreClosed(t *testing.T) {
	w, _ := openTestColdWriter(t, 1)
	require.NoError(t, w.Close())
	err := w.AppendLedger(1, []byte("v"))
	require.ErrorIs(t, err, stores.ErrStoreClosed)

	assert.ErrorIs(t, w.Finalize(), stores.ErrStoreClosed)
}

func TestNewColdWriter_TruncatesPreexistingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledgers.pack")

	crashed, err := NewColdWriter(path, 1, silentLogger())
	require.NoError(t, err)
	for i := range uint32(100) {
		require.NoError(t, crashed.AppendLedger(1+i, []byte("stale-ledger-payload-padding-padding-padding")))
	}
	_ = crashed

	info, err := os.Stat(path)
	require.NoError(t, err)
	partialSize := info.Size()
	require.Positive(t, partialSize)

	fresh, err := NewColdWriter(path, 999, silentLogger())
	require.NoError(t, err)
	require.NoError(t, fresh.AppendLedger(999, []byte("fresh")))
	require.NoError(t, fresh.Finalize())

	final, err := os.Stat(path)
	require.NoError(t, err)
	assert.Less(t, final.Size(), partialSize)

	c, err := OpenColdStore(path, zstd.NewDecompressor(), silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	assert.Equal(t, uint32(999), c.FirstSeq())
	assert.Equal(t, uint32(999), c.LastSeq())
	got, err := c.GetLedgerRaw(999)
	require.NoError(t, err)
	assert.Equal(t, []byte("fresh"), got)
}

func TestColdWriter_AppendAfterFinalizeReturnsErrStoreClosed(t *testing.T) {
	w, _ := openTestColdWriter(t, 1)
	require.NoError(t, w.AppendLedger(1, []byte("v")))
	require.NoError(t, w.Finalize())

	require.ErrorIs(t, w.AppendLedger(2, []byte("v")), stores.ErrStoreClosed)
	assert.ErrorIs(t, w.Finalize(), stores.ErrStoreClosed)
}
