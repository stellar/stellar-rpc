package ledger

import (
	"encoding/binary"
	"fmt"
	"math"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/packfile"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

func newTestColdReader(t *testing.T, path string) *ColdReader {
	t.Helper()
	c, err := OpenColdReader(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestOpenColdReader_ValidatesInputs(t *testing.T) {
	_, err := OpenColdReader("")
	require.ErrorIs(t, err, stores.ErrInvalidConfig)
}

func TestColdReader_RoundTripVariousSizes(t *testing.T) {
	for _, n := range []int{1, 3, 7, 10} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			const firstSeq uint32 = 1_000_000
			path, raws := writeFixturePack(t, firstSeq, n)
			c := newTestColdReader(t, path)

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
				// Entry.Bytes is borrowed and reused across iterations; copy to retain.
				seen = append(seen, append([]byte(nil), e.Bytes...))
			}
			assert.Equal(t, raws, seen)
		})
	}
}

func TestColdReader_GetLedgerRawOutOfRangeErrors(t *testing.T) {
	const firstSeq uint32 = 1_000
	path, _ := writeFixturePack(t, firstSeq, 10)
	c := newTestColdReader(t, path)

	_, err := c.GetLedgerRaw(firstSeq - 1)
	require.ErrorIs(t, err, stores.ErrOutOfRange)

	last, err := c.LastSeq()
	require.NoError(t, err)
	_, err = c.GetLedgerRaw(last + 1)
	assert.ErrorIs(t, err, stores.ErrOutOfRange)
}

func TestColdReader_IterateLedgersStartGreaterThanEndErrors(t *testing.T) {
	path, _ := writeFixturePack(t, 100, 10)
	c := newTestColdReader(t, path)

	var sawErr error
	count := 0
	for _, err := range c.IterateLedgers(50, 10) {
		if err != nil {
			sawErr = err
			continue
		}
		count++
	}
	require.ErrorIs(t, sawErr, stores.ErrOutOfRange)
	assert.Zero(t, count)
}

func TestColdReader_IterateLedgersOutOfRangeErrors(t *testing.T) {
	const firstSeq uint32 = 100
	path, raws := writeFixturePack(t, firstSeq, 10)
	c := newTestColdReader(t, path)

	// In-bounds happy path still works.
	var seenSeqs []uint32
	var seenBytes [][]byte
	for e, err := range c.IterateLedgers(firstSeq, firstSeq+9) {
		require.NoError(t, err)
		seenSeqs = append(seenSeqs, e.Seq)
		// Entry.Bytes is borrowed and reused across iterations; copy to retain.
		seenBytes = append(seenBytes, append([]byte(nil), e.Bytes...))
	}
	assert.Equal(t,
		[]uint32{100, 101, 102, 103, 104, 105, 106, 107, 108, 109},
		seenSeqs)
	assert.Equal(t, raws, seenBytes)

	// Any out-of-range portion errors and yields no entries.
	cases := []struct {
		name       string
		start, end uint32
	}{
		{"start below firstSeq, end in bounds", 50, 105},
		{"start in bounds, end above lastSeq", 105, 200},
		{"both straddling", 50, 200},
		{"fully below", 0, 99},
		{"fully above", 200, 300},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var sawErr error
			var seen []uint32
			for e, err := range c.IterateLedgers(tc.start, tc.end) {
				if err != nil {
					sawErr = err
					continue
				}
				seen = append(seen, e.Seq)
			}
			require.ErrorIs(t, sawErr, stores.ErrOutOfRange)
			assert.Empty(t, seen)
		})
	}
}

func TestColdReader_IterateLedgersBreakMidWalk(t *testing.T) {
	const firstSeq uint32 = 1
	path, _ := writeFixturePack(t, firstSeq, 10)
	c := newTestColdReader(t, path)

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

// TestColdReader_LazyOpen — OpenColdReader on a bogus path
// succeeds (no I/O); the failure surfaces only on the first
// method call.
func TestColdReader_LazyOpen(t *testing.T) {
	c, err := OpenColdReader(filepath.Join(t.TempDir(), "does-not-exist.pack"))
	require.NoError(t, err, "OpenColdReader must not do I/O")
	t.Cleanup(func() { _ = c.Close() })

	_, err = c.LastSeq()
	require.Error(t, err)
}

func TestColdReader_RejectsWrongAppDataSize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad-appdata.pack")
	pw, err := packfile.Create(path, packfile.WriterOptions{
		ItemsPerRecord: 1,
		Format:         formatLedgerCold,
	})
	require.NoError(t, err)
	require.NoError(t, pw.AppendItem([]byte("v")))
	// 7-byte payload — appDataSize is 4.
	require.NoError(t, pw.Finish([]byte("seven-b")))

	c, err := OpenColdReader(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	_, err = c.LastSeq()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "AppData")
}

func TestColdReader_RejectsWrongFormat(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wrong-format.pack")
	pw, err := packfile.Create(path, packfile.WriterOptions{
		ItemsPerRecord: 1,
		Format:         formatLedgerCold + 1,
	})
	require.NoError(t, err)
	require.NoError(t, pw.AppendItem([]byte("v")))
	require.NoError(t, pw.Finish(make([]byte, 4)))

	c, err := OpenColdReader(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	_, err = c.LastSeq()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "format")
}

func TestColdReader_CloseIsIdempotent(t *testing.T) {
	path, _ := writeFixturePack(t, 1, 1)
	c, err := OpenColdReader(path)
	require.NoError(t, err)
	require.NoError(t, c.Close())
	require.NoError(t, c.Close())
}

// TestColdReader_LastSeqOverflowRejected constructs a pack where
// firstSeq + TotalItems would overflow uint32 and asserts the
// reader rejects on first method call. Bypasses ColdWriter (whose
// own nextSeq counter would also overflow) by writing the pack at
// the packfile layer directly.
func TestColdReader_LastSeqOverflowRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "overflow.pack")
	pw, err := packfile.Create(path, packfile.WriterOptions{
		ItemsPerRecord: 1,
		Format:         formatLedgerCold,
	})
	require.NoError(t, err)
	for i := range 3 {
		require.NoError(t, pw.AppendItem([]byte{byte(i)}))
	}
	var ad [appDataSize]byte
	binary.BigEndian.PutUint32(ad[:], math.MaxUint32-1) // firstSeq = MaxUint32-1, items=3 → overflow
	require.NoError(t, pw.Finish(ad[:]))

	c, err := OpenColdReader(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	_, err = c.LastSeq()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "overflows")
}

func TestColdReader_ConcurrentReadsRaceFree(t *testing.T) {
	const firstSeq uint32 = 0
	const n = 50
	path, _ := writeFixturePack(t, firstSeq, n)
	c, err := OpenColdReader(path)
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

func TestColdReader_SharedDecompressorAcrossPacks(t *testing.T) {
	// Two readers built from different packs implicitly share the
	// package-level coldPackDecoder. This test guards against a
	// regression where a future change gives each reader its own
	// decoder and breaks the lookup-bytes-match contract.
	pathA, rawA := writeFixturePack(t, 1_000, 5)
	pathB, rawB := writeFixturePack(t, 9_000, 5)

	cA, err := OpenColdReader(pathA)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cA.Close() })

	cB, err := OpenColdReader(pathB)
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
