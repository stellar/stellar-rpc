package ledger

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/packfile"
)

func newTestColdStoreWriter(t *testing.T, firstSeq uint32) (*ColdStoreWriter, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ledgers.pack")
	w, err := NewColdStoreWriter(path, firstSeq, ColdWriterOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	return w, path
}

func newTestColdStoreReader(t *testing.T, path string) *ColdStoreReader {
	t.Helper()
	c, err := NewColdStoreReader(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func writeFixturePack(t *testing.T, firstSeq uint32, n int) (string, [][]byte) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ledgers.pack")
	w, err := NewColdStoreWriter(path, firstSeq, ColdWriterOptions{})
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

func TestNewColdStoreWriter_ValidatesInputs(t *testing.T) {
	_, err := NewColdStoreWriter("", 0, ColdWriterOptions{})
	assert.ErrorIs(t, err, stores.ErrInvalidConfig)
}

func TestColdStoreWriter_AppendRejectsGapAndKeepsCounter(t *testing.T) {
	const firstSeq uint32 = 100
	w, path := newTestColdStoreWriter(t, firstSeq)

	require.NoError(t, w.AppendLedger(100, []byte("a")))
	require.Error(t, w.AppendLedger(103, []byte("c")))
	require.NoError(t, w.AppendLedger(101, []byte("b")))
	require.NoError(t, w.Commit())

	c, err := NewColdStoreReader(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	first, err := c.FirstSeq()
	require.NoError(t, err)
	assert.Equal(t, uint32(100), first)
	last, err := c.LastSeq()
	require.NoError(t, err)
	assert.Equal(t, uint32(101), last)
}

func TestColdStoreWriter_AppendRejectsOutOfOrder(t *testing.T) {
	const firstSeq uint32 = 500
	w, _ := newTestColdStoreWriter(t, firstSeq)

	require.NoError(t, w.AppendLedger(500, []byte("a")))
	require.NoError(t, w.AppendLedger(501, []byte("b")))
	require.Error(t, w.AppendLedger(500, []byte("dup")))
	require.Error(t, w.AppendLedger(499, []byte("before-first")))
}

func TestColdStoreWriter_CommitRejectsZeroAppends(t *testing.T) {
	w, _ := newTestColdStoreWriter(t, 1)
	err := w.Commit()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no appends")
}

func TestColdStoreWriter_CommitEmitsTrailerAndAppData(t *testing.T) {
	const firstSeq uint32 = 9_876_543
	const n uint32 = 10
	w, path := newTestColdStoreWriter(t, firstSeq)
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

func TestNewColdStoreWriter_TruncatesPreexistingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledgers.pack")

	// Crashed-writer simulation: no Close, no Commit. The writer's
	// open fd and its single recordWorker goroutine leak for the
	// rest of the test binary's lifetime.
	crashed, err := NewColdStoreWriter(path, 1, ColdWriterOptions{})
	require.NoError(t, err)
	for i := range uint32(100) {
		require.NoError(t, crashed.AppendLedger(1+i, []byte("stale-ledger-payload-padding-padding-padding")))
	}

	info, err := os.Stat(path)
	require.NoError(t, err)
	partialSize := info.Size()
	require.Positive(t, partialSize)

	fresh, err := NewColdStoreWriter(path, 999, ColdWriterOptions{})
	require.NoError(t, err)
	require.NoError(t, fresh.AppendLedger(999, []byte("fresh")))
	require.NoError(t, fresh.Commit())

	final, err := os.Stat(path)
	require.NoError(t, err)
	assert.Less(t, final.Size(), partialSize)

	c, err := NewColdStoreReader(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	first, err := c.FirstSeq()
	require.NoError(t, err)
	assert.Equal(t, uint32(999), first)
	last, err := c.LastSeq()
	require.NoError(t, err)
	assert.Equal(t, uint32(999), last)
	got, err := c.GetLedgerRaw(999)
	require.NoError(t, err)
	assert.Equal(t, []byte("fresh"), got)
}

func TestNewColdStoreReader_ValidatesInputs(t *testing.T) {
	_, err := NewColdStoreReader("")
	require.ErrorIs(t, err, stores.ErrInvalidConfig)
}

func TestColdStoreReader_RoundTripVariousSizes(t *testing.T) {
	for _, n := range []int{1, 3, 7, 10} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			const firstSeq uint32 = 1_000_000
			path, raws := writeFixturePack(t, firstSeq, n)
			c := newTestColdStoreReader(t, path)

			first, err := c.FirstSeq()
			require.NoError(t, err)
			assert.Equal(t, firstSeq, first)
			last, err := c.LastSeq()
			require.NoError(t, err)
			assert.Equal(t, firstSeq+uint32(n)-1, last)

			for i := range n {
				seq := firstSeq + uint32(i)
				got, err := c.GetLedgerRaw(seq)
				require.NoError(t, err)
				assert.Equal(t, raws[i], got, "ledger %d byte-equality", seq)

				var decoded xdr.LedgerCloseMeta
				require.NoError(t, decoded.UnmarshalBinary(got))
				require.NotNil(t, decoded.V1)
				assert.Equal(t, xdr.Uint32(seq), decoded.V1.LedgerHeader.Header.LedgerSeq)
			}

			var seen [][]byte
			for e, err := range c.IterateLedgers(firstSeq, firstSeq+uint32(n)-1) {
				require.NoError(t, err)
				seen = append(seen, e.Bytes)
			}
			assert.Equal(t, raws, seen)
		})
	}
}

func TestColdStoreReader_GetLedgerRawOutOfRangeReturnsErrNotFound(t *testing.T) {
	const firstSeq uint32 = 1_000
	path, _ := writeFixturePack(t, firstSeq, 10)
	c := newTestColdStoreReader(t, path)

	_, err := c.GetLedgerRaw(firstSeq - 1)
	require.ErrorIs(t, err, stores.ErrNotFound)

	last, err := c.LastSeq()
	require.NoError(t, err)
	_, err = c.GetLedgerRaw(last + 1)
	assert.ErrorIs(t, err, stores.ErrNotFound)
}

func TestColdStoreReader_IterateLedgersStartGreaterThanEndIsNoop(t *testing.T) {
	path, _ := writeFixturePack(t, 100, 10)
	c := newTestColdStoreReader(t, path)

	count := 0
	for _, err := range c.IterateLedgers(50, 10) {
		require.NoError(t, err)
		count++
	}
	assert.Zero(t, count)
}

func TestColdStoreReader_IterateLedgersClampsToStoreBounds(t *testing.T) {
	const firstSeq uint32 = 100
	path, raws := writeFixturePack(t, firstSeq, 10)
	c := newTestColdStoreReader(t, path)

	var seenSeqs []uint32
	var seenBytes [][]byte
	for e, err := range c.IterateLedgers(50, 200) {
		require.NoError(t, err)
		seenSeqs = append(seenSeqs, e.Seq)
		seenBytes = append(seenBytes, e.Bytes)
	}
	assert.Equal(t,
		[]uint32{100, 101, 102, 103, 104, 105, 106, 107, 108, 109},
		seenSeqs)
	assert.Equal(t, raws, seenBytes)

	var below []uint32
	for e, err := range c.IterateLedgers(0, 99) {
		require.NoError(t, err)
		below = append(below, e.Seq)
	}
	assert.Empty(t, below)

	var above []uint32
	for e, err := range c.IterateLedgers(200, 300) {
		require.NoError(t, err)
		above = append(above, e.Seq)
	}
	assert.Empty(t, above)
}

func TestColdStoreReader_IterateLedgersBreakMidWalk(t *testing.T) {
	const firstSeq uint32 = 1
	path, _ := writeFixturePack(t, firstSeq, 10)
	c := newTestColdStoreReader(t, path)

	var seen []uint32
	for e, err := range c.IterateLedgers(firstSeq, firstSeq+9) {
		require.NoError(t, err)
		seen = append(seen, e.Seq)
		if len(seen) == 3 {
			break
		}
	}
	assert.Equal(t, []uint32{1, 2, 3}, seen)
}

// TestColdStoreReader_LazyOpen — NewColdStoreReader on a bogus path
// succeeds (no I/O); the failure surfaces only on the first
// method call.
func TestColdStoreReader_LazyOpen(t *testing.T) {
	c, err := NewColdStoreReader(filepath.Join(t.TempDir(), "does-not-exist.pack"))
	require.NoError(t, err, "NewColdStoreReader must not do I/O")
	t.Cleanup(func() { _ = c.Close() })

	_, err = c.FirstSeq()
	require.Error(t, err)
}

func TestColdStoreReader_RejectsWrongAppDataSize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad-appdata.pack")
	pw, err := packfile.Create(path, packfile.WriterOptions{
		ItemsPerRecord: 1,
		Format:         formatLedgerCold,
	})
	require.NoError(t, err)
	require.NoError(t, pw.AppendItem([]byte("v")))
	// 7-byte payload — appDataSize is 4.
	require.NoError(t, pw.Finish([]byte("seven-b")))

	c, err := NewColdStoreReader(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	_, err = c.FirstSeq()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "AppData")
}

func TestColdStoreReader_RejectsWrongFormat(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wrong-format.pack")
	pw, err := packfile.Create(path, packfile.WriterOptions{
		ItemsPerRecord: 1,
		Format:         formatLedgerCold + 1,
	})
	require.NoError(t, err)
	require.NoError(t, pw.AppendItem([]byte("v")))
	require.NoError(t, pw.Finish(make([]byte, 4)))

	c, err := NewColdStoreReader(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	_, err = c.FirstSeq()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "format")
}

// TestColdStoreWriter_CloseAfterCommitIsNoop pins behavior: Close
// after a successful Commit returns nil and leaves the committed
// pack on disk (packfile.Writer.Close detects the post-Finish
// state and skips removal).
func TestColdStoreWriter_CloseAfterCommitIsNoop(t *testing.T) {
	w, path := newTestColdStoreWriter(t, 1)
	require.NoError(t, w.AppendLedger(1, []byte("v")))
	require.NoError(t, w.Commit())

	assert.NoError(t, w.Close())
	assert.NoError(t, w.Close())

	_, err := os.Stat(path)
	assert.NoError(t, err, "committed pack must remain on disk after Close")
}

func TestColdStoreWriter_CommitAfterCommitReturnsErrStoreClosed(t *testing.T) {
	w, _ := newTestColdStoreWriter(t, 1)
	require.NoError(t, w.AppendLedger(1, []byte("v")))
	require.NoError(t, w.Commit())
	assert.ErrorIs(t, w.Commit(), stores.ErrStoreClosed)
}

func TestColdStoreWriter_AppendAfterCommitReturnsErrStoreClosed(t *testing.T) {
	w, _ := newTestColdStoreWriter(t, 1)
	require.NoError(t, w.AppendLedger(1, []byte("v")))
	require.NoError(t, w.Commit())
	assert.ErrorIs(t, w.AppendLedger(2, []byte("v")), stores.ErrStoreClosed)
}

func TestColdStoreWriter_AppendAfterCloseReturnsErrStoreClosed(t *testing.T) {
	w, _ := newTestColdStoreWriter(t, 1)
	require.NoError(t, w.Close())
	assert.ErrorIs(t, w.AppendLedger(1, []byte("v")), stores.ErrStoreClosed)
}

func TestColdStoreReader_CloseIsIdempotent(t *testing.T) {
	path, _ := writeFixturePack(t, 1, 1)
	c, err := NewColdStoreReader(path)
	require.NoError(t, err)
	require.NoError(t, c.Close())
	require.NoError(t, c.Close())
}

// TestColdWriterOptions_Plumbing exercises the Concurrency and
// BytesPerSync options. A non-default Concurrency value forces the
// packfile worker pipeline; if the option wiring is broken the
// roundtrip would either fail or produce an unreadable pack.
func TestColdWriterOptions_Plumbing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledgers.pack")
	const firstSeq uint32 = 1
	const n = 20

	w, err := NewColdStoreWriter(path, firstSeq, ColdWriterOptions{
		Concurrency:  4,
		BytesPerSync: 64 * 1024,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	raws := make([][]byte, n)
	for i := range n {
		raws[i] = []byte(fmt.Sprintf("ledger-payload-%d", i))
		require.NoError(t, w.AppendLedger(firstSeq+uint32(i), raws[i]))
	}
	require.NoError(t, w.Commit())

	c := newTestColdStoreReader(t, path)
	last, err := c.LastSeq()
	require.NoError(t, err)
	assert.Equal(t, firstSeq+n-1, last)

	for i := range n {
		got, err := c.GetLedgerRaw(firstSeq + uint32(i))
		require.NoError(t, err)
		assert.Equal(t, raws[i], got)
	}
}

// TestColdStoreReader_LastSeqOverflowRejected constructs a pack
// where firstSeq + TotalItems would overflow uint32 and asserts
// the reader rejects on first method call. Bypasses
// ColdStoreWriter (whose own nextSeq counter would also overflow)
// by writing the pack at the packfile layer directly.
func TestColdStoreReader_LastSeqOverflowRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "overflow.pack")
	pw, err := packfile.Create(path, packfile.WriterOptions{
		ItemsPerRecord: 1,
		Format:         formatLedgerCold,
	})
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		require.NoError(t, pw.AppendItem([]byte{byte(i)}))
	}
	var ad [appDataSize]byte
	binary.BigEndian.PutUint32(ad[:], math.MaxUint32-1) // firstSeq = MaxUint32-1, items=3 → overflow
	require.NoError(t, pw.Finish(ad[:]))

	c, err := NewColdStoreReader(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	_, err = c.FirstSeq()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "overflows")
}

func TestColdStoreReader_ConcurrentReadsRaceFree(t *testing.T) {
	const firstSeq uint32 = 0
	const n = 50
	path, _ := writeFixturePack(t, firstSeq, n)
	c, err := NewColdStoreReader(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	var wg sync.WaitGroup
	var stop atomic.Bool
	const workers = 4
	for range workers {
		wg.Go(func() {
			for i := uint32(0); !stop.Load(); i++ {
				_, _ = c.GetLedgerRaw(i % n)
			}
		})
		wg.Go(func() {
			for !stop.Load() {
				for _, err := range c.IterateLedgers(firstSeq, firstSeq+n-1) {
					if err != nil {
						break
					}
				}
			}
		})
	}

	time.Sleep(50 * time.Millisecond)
	stop.Store(true)
	wg.Wait()
}

func TestColdStoreReader_SharedDecompressorAcrossPacks(t *testing.T) {
	// Two readers built from different packs implicitly share the
	// package-level coldPackDecoder. This test guards against a
	// regression where a future change gives each reader its own
	// decoder and breaks the lookup-bytes-match contract.
	pathA, rawA := writeFixturePack(t, 1_000, 5)
	pathB, rawB := writeFixturePack(t, 9_000, 5)

	cA, err := NewColdStoreReader(pathA)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cA.Close() })

	cB, err := NewColdStoreReader(pathB)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cB.Close() })

	for i := range 5 {
		gotA, err := cA.GetLedgerRaw(1_000 + uint32(i))
		require.NoError(t, err)
		assert.Equal(t, rawA[i], gotA)

		gotB, err := cB.GetLedgerRaw(9_000 + uint32(i))
		require.NoError(t, err)
		assert.Equal(t, rawB[i], gotB)
	}

	require.NoError(t, cA.Close())
	got, err := cB.GetLedgerRaw(9_002)
	require.NoError(t, err)
	assert.Equal(t, rawB[2], got)
}
