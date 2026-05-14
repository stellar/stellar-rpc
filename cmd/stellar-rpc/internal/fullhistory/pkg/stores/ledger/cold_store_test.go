package ledger

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/packfile"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/zstd"
)

// writeFixturePack appends N ledgers starting at firstSeq via
// ColdWriter and returns the path plus the raw bytes (post-marshal)
// indexed by [seq - firstSeq] so callers can assert byte-equality
// per ledger.
func writeFixturePack(t *testing.T, firstSeq uint32, n int) (string, [][]byte) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ledgers.pack")
	w, err := NewColdWriter(path, firstSeq, newTestEncoderFactory(), silentLogger())
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

func TestOpenColdStore_RecoversFirstAndLastSeq(t *testing.T) {
	const firstSeq uint32 = 5_000_000
	const n = 10
	path, _ := writeFixturePack(t, firstSeq, n)

	c := openTestColdStore(t, path)
	assert.Equal(t, firstSeq, c.FirstSeq())
	assert.Equal(t, firstSeq+n-1, c.LastSeq())
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

	// Close is idempotent.
	assert.NoError(t, c.Close())
}

func TestColdStore_IterateLedgersHappyPath(t *testing.T) {
	const firstSeq uint32 = 100
	path, raws := writeFixturePack(t, firstSeq, 10)
	c := openTestColdStore(t, path)

	var seenSeqs []uint32
	var seenBytes [][]byte
	for e, err := range c.IterateLedgers(firstSeq, firstSeq+9) {
		require.NoError(t, err)
		seenSeqs = append(seenSeqs, e.Seq)
		seenBytes = append(seenBytes, e.Bytes)
	}
	assert.Equal(t,
		[]uint32{100, 101, 102, 103, 104, 105, 106, 107, 108, 109},
		seenSeqs)
	assert.Equal(t, raws, seenBytes)
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

	// start below firstSeq, end above lastSeq → clamp to [100, 109].
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

	// Window entirely below the store: no yields.
	var below []uint32
	for e, err := range c.IterateLedgers(0, 99) {
		require.NoError(t, err)
		below = append(below, e.Seq)
	}
	assert.Empty(t, below)

	// Window entirely above the store: no yields.
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

func TestColdStore_EndToEndRoundTripTenLedgers(t *testing.T) {
	const firstSeq uint32 = 7_000_000
	const n = 10
	path, raws := writeFixturePack(t, firstSeq, n)
	c := openTestColdStore(t, path)

	for i := range n {
		got, err := c.GetLedgerRaw(firstSeq + uint32(i))
		require.NoError(t, err)
		assert.Equal(t, raws[i], got, "ledger %d byte-equality", firstSeq+uint32(i))
	}
}

func TestColdStore_VerifyContentHashPasses(t *testing.T) {
	path, _ := writeFixturePack(t, 1, 10)
	// Hand the underlying packfile.Reader a decoder + run Verify.
	c := openTestColdStore(t, path)
	require.NoError(t, c.reader.Verify(context.Background()))
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

func TestColdStore_XDRRoundTrip(t *testing.T) {
	const firstSeq uint32 = 4_242_424
	const txCount = 3
	lcm, wantHashes := makeRandomLedgerCloseMeta(firstSeq, txCount)
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)

	w, path := openTestColdWriter(t, firstSeq)
	require.NoError(t, w.AppendLedger(firstSeq, raw))
	require.NoError(t, w.Finalize())

	c := openTestColdStore(t, path)
	got, err := c.GetLedgerRaw(firstSeq)
	require.NoError(t, err)
	assert.Equal(t, raw, got, "cold-store bytes must round-trip verbatim")

	var decoded xdr.LedgerCloseMeta
	require.NoError(t, decoded.UnmarshalBinary(got))
	require.NotNil(t, decoded.V1)
	assert.Equal(t, xdr.Uint32(firstSeq), decoded.V1.LedgerHeader.Header.LedgerSeq)

	require.NotNil(t, decoded.V1.TxSet.V1TxSet)
	require.Len(t, decoded.V1.TxSet.V1TxSet.Phases, 1)
	comps := decoded.V1.TxSet.V1TxSet.Phases[0].V0Components
	require.NotNil(t, comps)
	require.Len(t, *comps, 1)
	gotEnvs := (*comps)[0].TxsMaybeDiscountedFee.Txs
	require.Len(t, gotEnvs, txCount)

	gotHashes := make([][32]byte, len(gotEnvs))
	for i, env := range gotEnvs {
		h, err := network.HashTransactionInEnvelope(env, network.TestNetworkPassphrase)
		require.NoError(t, err)
		gotHashes[i] = h
	}
	assert.Equal(t, wantHashes, gotHashes, "tx hashes must survive cold-store round-trip")
}

func TestOpenColdStore_RejectsWrongAppDataSize(t *testing.T) {
	// Build a packfile that satisfies packfile.Open but carries
	// non-4-byte AppData — bypass ColdWriter so we can plant the
	// malformed payload directly.
	path := filepath.Join(t.TempDir(), "bad-appdata.pack")
	pw, err := packfile.Create(path, packfile.WriterOptions{
		Format:           formatLedgerCold,
		NewRecordEncoder: newTestEncoderFactory(),
		ContentHash:      true,
	})
	require.NoError(t, err)
	require.NoError(t, pw.AppendItem([]byte("v")))
	require.NoError(t, pw.Finish([]byte("eight-by"))) // 8 bytes, not 4

	_, err = OpenColdStore(path, zstd.NewDecompressor(), silentLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "AppData")
}

func TestOpenColdStore_RejectsCorruptFile(t *testing.T) {
	// Truncated garbage — too short to even hold a trailer.
	path := filepath.Join(t.TempDir(), "garbage.pack")
	require.NoError(t, os.WriteFile(path, []byte("not a packfile"), 0o644))

	_, err := OpenColdStore(path, zstd.NewDecompressor(), silentLogger())
	assert.Error(t, err)
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

	// Give the readers time to interleave, then close FIRST (matches
	// the hot-store race shape — Close races with in-flight reads).
	// packfile.Reader.Close is documented as unsafe-with-concurrent-reads
	// in the sense that in-flight reads see a file-closed error rather
	// than completing cleanly; that's a behavioral contract, not a
	// data race, so -race must still come up clean.
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, c.Close())
	stop.Store(true)
	wg.Wait()

	_, err = c.GetLedgerRaw(0)
	assert.ErrorIs(t, err, stores.ErrStoreClosed)
}
