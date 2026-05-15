package ledger

import (
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/packfile"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/zstd"
)

func writeFixturePack(t *testing.T, firstSeq uint32, n int) (string, [][]byte) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ledgers.pack")
	w, err := NewColdWriter(path, firstSeq, silentLogger())
	require.NoError(t, err)

	raws := make([][]byte, n)
	for i := range n {
		lcm, _ := makeRandomLedgerCloseMeta(firstSeq+uint32(i), 2)
		b, err := lcm.MarshalBinary()
		require.NoError(t, err)
		raws[i] = b
		require.NoError(t, w.AppendLedger(firstSeq+uint32(i), b))
	}
	require.NoError(t, w.Finalize())
	return path, raws
}

func openTestColdStore(t *testing.T, path string) *ColdStore {
	t.Helper()
	c, err := OpenColdStore(path, zstd.NewDecompressor(), silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestOpenColdStore_ValidatesInputs(t *testing.T) {
	dec := zstd.NewDecompressor()
	log := silentLogger()
	path, _ := writeFixturePack(t, 1, 1)

	_, err := OpenColdStore("", dec, log)
	require.ErrorIs(t, err, rocksdb.ErrInvalidConfig)

	_, err = OpenColdStore(path, nil, log)
	require.ErrorIs(t, err, rocksdb.ErrInvalidConfig)

	_, err = OpenColdStore(path, dec, nil)
	assert.ErrorIs(t, err, rocksdb.ErrInvalidConfig)
}

func TestColdStore_RoundTripVariousSizes(t *testing.T) {
	for _, n := range []int{1, 3, 7, 10} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			const firstSeq uint32 = 1_000_000
			path, raws := writeFixturePack(t, firstSeq, n)
			c := openTestColdStore(t, path)

			assert.Equal(t, firstSeq, c.FirstSeq())
			assert.Equal(t, firstSeq+uint32(n)-1, c.LastSeq())

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

func TestColdStore_GetLedgerRawOutOfRangeReturnsErrNotFound(t *testing.T) {
	const firstSeq uint32 = 1_000
	path, _ := writeFixturePack(t, firstSeq, 10)
	c := openTestColdStore(t, path)

	_, err := c.GetLedgerRaw(firstSeq - 1)
	require.ErrorIs(t, err, stores.ErrNotFound)

	_, err = c.GetLedgerRaw(c.LastSeq() + 1)
	assert.ErrorIs(t, err, stores.ErrNotFound)
}

func TestColdStore_GetLedgerRawClosedReturnsErrStoreClosed(t *testing.T) {
	path, _ := writeFixturePack(t, 1, 1)
	c, err := OpenColdStore(path, zstd.NewDecompressor(), silentLogger())
	require.NoError(t, err)
	require.NoError(t, c.Close())

	_, err = c.GetLedgerRaw(1)
	require.ErrorIs(t, err, stores.ErrStoreClosed)

	assert.NoError(t, c.Close())
}

func TestColdStore_IterateLedgersStartGreaterThanEndIsNoop(t *testing.T) {
	path, _ := writeFixturePack(t, 100, 10)
	c := openTestColdStore(t, path)

	count := 0
	for _, err := range c.IterateLedgers(50, 10) {
		require.NoError(t, err)
		count++
	}
	assert.Zero(t, count)
}

func TestColdStore_IterateLedgersClampsToStoreBounds(t *testing.T) {
	const firstSeq uint32 = 100
	path, raws := writeFixturePack(t, firstSeq, 10)
	c := openTestColdStore(t, path)

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

func TestColdStore_IterateLedgersClosedYieldsErrStoreClosed(t *testing.T) {
	path, _ := writeFixturePack(t, 1, 5)
	c, err := OpenColdStore(path, zstd.NewDecompressor(), silentLogger())
	require.NoError(t, err)
	require.NoError(t, c.Close())

	var seen []error
	count := 0
	for _, e := range c.IterateLedgers(1, 5) {
		seen = append(seen, e)
		count++
	}
	assert.Equal(t, 1, count, "iterator must yield exactly once on closed store")
	require.Len(t, seen, 1)
	assert.ErrorIs(t, seen[0], stores.ErrStoreClosed)
}

func TestColdStore_IterateLedgersBreakMidWalk(t *testing.T) {
	const firstSeq uint32 = 1
	path, _ := writeFixturePack(t, firstSeq, 10)
	c := openTestColdStore(t, path)

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

func TestOpenColdStore_RejectsWrongAppDataSize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad-appdata.pack")
	pw, err := packfile.Create(path, packfile.WriterOptions{
		ItemsPerRecord: 1,
		Format:         formatLedgerCold,
	})
	require.NoError(t, err)
	require.NoError(t, pw.AppendItem([]byte("v")))
	require.NoError(t, pw.Finish([]byte("eight-by")))

	_, err = OpenColdStore(path, zstd.NewDecompressor(), silentLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "AppData")
}

func TestOpenColdStore_RejectsWrongFormat(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wrong-format.pack")
	pw, err := packfile.Create(path, packfile.WriterOptions{
		ItemsPerRecord: 1,
		Format:         formatLedgerCold + 1,
	})
	require.NoError(t, err)
	require.NoError(t, pw.AppendItem([]byte("v")))
	require.NoError(t, pw.Finish([]byte("ABCD")))

	_, err = OpenColdStore(path, zstd.NewDecompressor(), silentLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "format")
}

func TestColdStore_SharedDecompressorAcrossPacks(t *testing.T) {
	sharedDec := zstd.NewDecompressor()

	pathA, rawA := writeFixturePack(t, 1_000, 5)
	pathB, rawB := writeFixturePack(t, 9_000, 5)

	cA, err := OpenColdStore(pathA, sharedDec, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = cA.Close() })

	cB, err := OpenColdStore(pathB, sharedDec, silentLogger())
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

func TestColdStore_ConcurrentReadsRaceFree(t *testing.T) {
	const firstSeq uint32 = 0
	const n = 50
	path, _ := writeFixturePack(t, firstSeq, n)
	c, err := OpenColdStore(path, zstd.NewDecompressor(), silentLogger())
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
	require.NoError(t, c.Close())
	stop.Store(true)
	wg.Wait()

	_, err = c.GetLedgerRaw(0)
	assert.ErrorIs(t, err, stores.ErrStoreClosed)
}
