package ledger

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/packfile"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/storage/stores"
)

func newTestColdWriter(t *testing.T, firstSeq uint32) (*ColdWriter, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ledgers.pack")
	w, err := NewColdWriter(path, firstSeq, ColdWriterOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	return w, path
}

func writeFixturePack(t *testing.T, firstSeq uint32, n int) (string, [][]byte) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ledgers.pack")
	w, err := NewColdWriter(path, firstSeq, ColdWriterOptions{})
	require.NoError(t, err)

	raws := make([][]byte, n)
	for i := range n {
		lcm, _ := makeRandomLedgerCloseMeta(firstSeq+uint32(i), 2)
		b, err := lcm.MarshalBinary()
		require.NoError(t, err)
		raws[i] = b
		require.NoError(t, w.AppendLedger(firstSeq+uint32(i), b))
	}
	require.NoError(t, w.Commit())
	return path, raws
}

func TestNewColdWriter_ValidatesInputs(t *testing.T) {
	_, err := NewColdWriter("", 0, ColdWriterOptions{})
	assert.ErrorIs(t, err, stores.ErrInvalidConfig)
}

func TestColdWriter_AppendRejectsGapAndKeepsCounter(t *testing.T) {
	const firstSeq uint32 = 100
	w, path := newTestColdWriter(t, firstSeq)

	require.NoError(t, w.AppendLedger(100, []byte("a")))
	require.Error(t, w.AppendLedger(103, []byte("c")))
	require.NoError(t, w.AppendLedger(101, []byte("b")))
	require.NoError(t, w.Commit())

	c, err := OpenColdReader(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	got, err := c.GetLedgerRaw(100)
	require.NoError(t, err)
	assert.Equal(t, []byte("a"), got)
	last, err := c.LastSeq()
	require.NoError(t, err)
	assert.Equal(t, uint32(101), last)
}

func TestColdWriter_AppendRejectsOutOfOrder(t *testing.T) {
	const firstSeq uint32 = 500
	w, _ := newTestColdWriter(t, firstSeq)

	require.NoError(t, w.AppendLedger(500, []byte("a")))
	require.NoError(t, w.AppendLedger(501, []byte("b")))
	require.Error(t, w.AppendLedger(500, []byte("dup")))
	require.Error(t, w.AppendLedger(499, []byte("before-first")))
}

func TestColdWriter_CommitRejectsZeroAppends(t *testing.T) {
	w, _ := newTestColdWriter(t, 1)
	err := w.Commit()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no appends")
}

func TestColdWriter_CommitEmitsTrailerAndAppData(t *testing.T) {
	const firstSeq uint32 = 9_876_543
	const n uint32 = 10
	w, path := newTestColdWriter(t, firstSeq)
	for i := range n {
		require.NoError(t, w.AppendLedger(firstSeq+i, []byte{byte(i)}))
	}
	require.NoError(t, w.Commit())

	r := packfile.Open(path, packfile.ReaderOptions{})
	t.Cleanup(func() { _ = r.Close() })

	total, err := r.TotalItems()
	require.NoError(t, err)
	assert.Equal(t, int(n), total)

	ad, err := r.AppData()
	require.NoError(t, err)
	require.Len(t, ad, appDataSize)
	assert.Equal(t, firstSeq, binary.BigEndian.Uint32(ad))
}

func TestNewColdWriter_TruncatesPreexistingFile(t *testing.T) {
	// Seed the target path with junk bytes (simulating a partial
	// crashed-writer file left on disk from a previous run) and
	// verify NewColdWriter truncates it before writing fresh
	// content. Using os.WriteFile rather than a half-driven writer
	// keeps the test race-free: no leaked recordWorker goroutine
	// can flush stale bytes after the fresh writer's truncate.
	path := filepath.Join(t.TempDir(), "ledgers.pack")

	junk := make([]byte, 64*1024)
	for i := range junk {
		junk[i] = 0xAB
	}
	require.NoError(t, os.WriteFile(path, junk, 0o644))

	info, err := os.Stat(path)
	require.NoError(t, err)
	preSize := info.Size()
	require.Equal(t, int64(len(junk)), preSize)

	fresh, err := NewColdWriter(path, 999, ColdWriterOptions{})
	require.NoError(t, err)
	require.NoError(t, fresh.AppendLedger(999, []byte("fresh")))
	require.NoError(t, fresh.Commit())

	final, err := os.Stat(path)
	require.NoError(t, err)
	assert.Less(t, final.Size(), preSize, "fresh pack must be smaller than the 64 KiB junk seed")

	c, err := OpenColdReader(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	last, err := c.LastSeq()
	require.NoError(t, err)
	assert.Equal(t, uint32(999), last)
	got, err := c.GetLedgerRaw(999)
	require.NoError(t, err)
	assert.Equal(t, []byte("fresh"), got)
}

// TestColdWriter_CloseAfterCommitIsNoop pins behavior: Close
// after a successful Commit returns nil and leaves the committed
// pack on disk (packfile.Writer.Close detects the post-Finish
// state and skips removal).
func TestColdWriter_CloseAfterCommitIsNoop(t *testing.T) {
	w, path := newTestColdWriter(t, 1)
	require.NoError(t, w.AppendLedger(1, []byte("v")))
	require.NoError(t, w.Commit())

	assert.NoError(t, w.Close())
	assert.NoError(t, w.Close())

	_, err := os.Stat(path)
	assert.NoError(t, err, "committed pack must remain on disk after Close")
}

func TestColdWriter_CommitAfterCommitReturnsErrStoreClosed(t *testing.T) {
	w, _ := newTestColdWriter(t, 1)
	require.NoError(t, w.AppendLedger(1, []byte("v")))
	require.NoError(t, w.Commit())
	assert.ErrorIs(t, w.Commit(), stores.ErrStoreClosed)
}

func TestColdWriter_AppendAfterCommitReturnsErrStoreClosed(t *testing.T) {
	w, _ := newTestColdWriter(t, 1)
	require.NoError(t, w.AppendLedger(1, []byte("v")))
	require.NoError(t, w.Commit())
	assert.ErrorIs(t, w.AppendLedger(2, []byte("v")), stores.ErrStoreClosed)
}

func TestColdWriter_AppendAfterCloseReturnsErrStoreClosed(t *testing.T) {
	w, _ := newTestColdWriter(t, 1)
	require.NoError(t, w.Close())
	assert.ErrorIs(t, w.AppendLedger(1, []byte("v")), stores.ErrStoreClosed)
}

// TestColdWriterOptions_Plumbing exercises the
// ColdWriterOptions.Concurrency and ColdWriterOptions.BytesPerSync
// fields. A non-default concurrency value forces the packfile worker
// pipeline; if the option wiring is broken the roundtrip would
// either fail or produce an unreadable pack.
func TestColdWriterOptions_Plumbing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledgers.pack")
	const firstSeq uint32 = 1
	const n = 20

	w, err := NewColdWriter(path, firstSeq, ColdWriterOptions{
		Concurrency:  4,
		BytesPerSync: 64 * 1024,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	raws := make([][]byte, n)
	for i := range n {
		raws[i] = fmt.Appendf(nil, "ledger-payload-%d", i)
		require.NoError(t, w.AppendLedger(firstSeq+uint32(i), raws[i]))
	}
	require.NoError(t, w.Commit())

	c := newTestColdReader(t, path)
	last, err := c.LastSeq()
	require.NoError(t, err)
	assert.Equal(t, firstSeq+n-1, last)

	for i := range n {
		got, err := c.GetLedgerRaw(firstSeq + uint32(i))
		require.NoError(t, err)
		assert.Equal(t, raws[i], got)
	}
}
