package catalog

import (
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
)

// The catalog's string-KV layer (kv.go), exercised through Open.

// openKVAt opens a catalog at an explicit KV path (for reopen/persistence
// cases); the artifact layout is irrelevant to the KV tests.
func openKVAt(t *testing.T, path string) (*Catalog, error) {
	t.Helper()
	idxLayout, err := geometry.NewTxHashIndexLayout(geometry.ChunksPerTxhashIndex)
	require.NoError(t, err)
	return Open(path, geometry.NewLayout(t.TempDir()), idxLayout, silentLogger())
}

func openTestKV(t *testing.T) *Catalog {
	t.Helper()
	c, err := openKVAt(t, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// kvGetHit asserts key is present and returns its value.
func kvGetHit(t *testing.T, c *Catalog, key string) string {
	t.Helper()
	v, ok, err := c.get(key)
	require.NoError(t, err)
	require.True(t, ok, "key %q absent", key)
	return v
}

// kvGetMiss asserts key is cleanly absent (ok=false, no error).
func kvGetMiss(t *testing.T, c *Catalog, key string) {
	t.Helper()
	_, ok, err := c.get(key)
	require.NoError(t, err)
	require.False(t, ok, "key %q unexpectedly present", key)
}

func TestOpen_ValidatesInputs(t *testing.T) {
	idxLayout, err := geometry.NewTxHashIndexLayout(geometry.ChunksPerTxhashIndex)
	require.NoError(t, err)

	_, err = Open("", geometry.NewLayout(t.TempDir()), idxLayout, silentLogger())
	require.ErrorIs(t, err, rocksdb.ErrInvalidConfig)

	_, err = Open(t.TempDir(), geometry.NewLayout(t.TempDir()), idxLayout, nil)
	require.ErrorIs(t, err, rocksdb.ErrInvalidConfig)
}

func TestOpen_CreatesMissingDirectory(t *testing.T) {
	c, err := openKVAt(t, filepath.Join(t.TempDir(), "subdir-never-created"))
	require.NoError(t, err)
	require.NotNil(t, c)
	t.Cleanup(func() { _ = c.Close() })
}

func TestKV_CloseIsIdempotent(t *testing.T) {
	c, err := openKVAt(t, t.TempDir())
	require.NoError(t, err)

	require.NoError(t, c.Close())
	require.NoError(t, c.Close())
}

func TestKV_GetMissReportsAbsent(t *testing.T) {
	c := openTestKV(t)
	kvGetMiss(t, c, "nothing-here")
}

func TestKV_PutGetRoundTrip(t *testing.T) {
	c := openTestKV(t)

	require.NoError(t, c.put("streaming:last_committed_ledger", "123456"))
	assert.Equal(t, "123456", kvGetHit(t, c, "streaming:last_committed_ledger"))
}

func TestKV_PutOverwritesPriorValue(t *testing.T) {
	c := openTestKV(t)

	require.NoError(t, c.put("k", "v1"))
	require.NoError(t, c.put("k", "v2"))
	assert.Equal(t, "v2", kvGetHit(t, c, "k"))
}

func TestKV_PutEmptyValueIsDistinctFromMissing(t *testing.T) {
	c := openTestKV(t)

	kvGetMiss(t, c, "key-empty")

	require.NoError(t, c.put("key-empty", ""))
	assert.Empty(t, kvGetHit(t, c, "key-empty"))
}

func TestKV_PutBinaryValueRoundTrips(t *testing.T) {
	c := openTestKV(t)

	// Non-UTF-8 byte sequence inside a Go string — valid since Go
	// strings can hold arbitrary bytes.
	binary := string([]byte{0x00, 0xff, 0x7f, 0x80, 0xfe})
	require.NoError(t, c.put("binary-key", binary))
	assert.Equal(t, binary, kvGetHit(t, c, "binary-key"))
}

func TestKV_DeleteRemovesKey(t *testing.T) {
	c := openTestKV(t)

	require.NoError(t, c.put("k", "v"))
	require.NoError(t, c.del("k"))

	kvGetMiss(t, c, "k")
}

func TestKV_DeleteMissingKeyIsIdempotent(t *testing.T) {
	c := openTestKV(t)

	require.NoError(t, c.del("never-existed"))
	require.NoError(t, c.del("never-existed"))
}

func TestKV_NumericRoundTripWithStrconv(t *testing.T) {
	c := openTestKV(t)
	const want uint32 = 4_294_967_290

	require.NoError(t, c.put("streaming:last_committed_ledger", strconv.FormatUint(uint64(want), 10)))
	raw := kvGetHit(t, c, "streaming:last_committed_ledger")
	got, err := strconv.ParseUint(raw, 10, 32)
	require.NoError(t, err)
	assert.Equal(t, want, uint32(got))
}

func TestKV_BatchCommitsAtomically(t *testing.T) {
	c := openTestKV(t)
	require.NoError(t, c.put("chunk:00000010:txhash", "1"))
	require.NoError(t, c.put("chunk:00000011:txhash", "1"))

	require.NoError(t, c.batch(func(b batchWriter) error {
		b.Put("index:00000002:txhash", "9")
		b.Delete("chunk:00000010:txhash")
		b.Delete("chunk:00000011:txhash")
		return nil
	}))

	assert.Equal(t, "9", kvGetHit(t, c, "index:00000002:txhash"))
	kvGetMiss(t, c, "chunk:00000010:txhash")
	kvGetMiss(t, c, "chunk:00000011:txhash")
}

func TestKV_BatchEmptyCallbackCommitsCleanly(t *testing.T) {
	c := openTestKV(t)
	require.NoError(t, c.batch(func(batchWriter) error { return nil }))
}

func TestKV_BatchCallbackErrorPropagatesNoWrites(t *testing.T) {
	c := openTestKV(t)
	require.NoError(t, c.put("k1", "before"))

	cbErr := errors.New("caller bailing out")
	err := c.batch(func(b batchWriter) error {
		b.Put("k1", "after")
		b.Put("k2", "new")
		return cbErr
	})
	require.ErrorIs(t, err, cbErr)

	// Neither write committed — k1 is unchanged, k2 absent.
	assert.Equal(t, "before", kvGetHit(t, c, "k1"))
	kvGetMiss(t, c, "k2")
}

func TestKV_PrefixScanFiltersByPrefix(t *testing.T) {
	c := openTestKV(t)

	require.NoError(t, c.put("chunk:00000001:lfs", "1"))
	require.NoError(t, c.put("chunk:00000002:lfs", "1"))
	require.NoError(t, c.put("chunk:00000003:lfs", "1"))
	require.NoError(t, c.put("index:00000001:txhash", "1"))

	var keys []string
	for e, err := range c.prefixScan("chunk:") {
		require.NoError(t, err)
		keys = append(keys, e.Key)
	}
	assert.Equal(t, []string{
		"chunk:00000001:lfs",
		"chunk:00000002:lfs",
		"chunk:00000003:lfs",
	}, keys)
}

func TestKV_PrefixScanEmptyPrefixWalksWholeStore(t *testing.T) {
	c := openTestKV(t)

	require.NoError(t, c.put("a", "1"))
	require.NoError(t, c.put("b", "2"))
	require.NoError(t, c.put("c", "3"))

	var seen []string
	for e, err := range c.prefixScan("") {
		require.NoError(t, err)
		seen = append(seen, e.Key+"="+e.Value)
	}
	assert.Equal(t, []string{"a=1", "b=2", "c=3"}, seen)
}

func TestKV_PrefixScanNoMatchesYieldsNothing(t *testing.T) {
	c := openTestKV(t)
	require.NoError(t, c.put("a", "1"))

	count := 0
	for _, err := range c.prefixScan("never-matches:") {
		require.NoError(t, err)
		count++
	}
	assert.Equal(t, 0, count)
}

func TestKV_PrefixScanShowsGapsInKeyspace(t *testing.T) {
	c := openTestKV(t)
	// Gap at chunk:00000002.
	require.NoError(t, c.put("chunk:00000001:lfs", "1"))
	require.NoError(t, c.put("chunk:00000003:lfs", "1"))
	require.NoError(t, c.put("chunk:00000004:lfs", "1"))

	var keys []string
	for e, err := range c.prefixScan("chunk:") {
		require.NoError(t, err)
		keys = append(keys, e.Key)
	}
	assert.Equal(t, []string{
		"chunk:00000001:lfs",
		"chunk:00000003:lfs",
		"chunk:00000004:lfs",
	}, keys)
}

func TestKV_PrefixScanCallerBreakStopsCleanly(t *testing.T) {
	c := openTestKV(t)
	for i := range 5 {
		require.NoError(t, c.put(fmt.Sprintf("k%02d", i), "v"))
	}

	var seen []string
	for e, err := range c.prefixScan("k") {
		require.NoError(t, err)
		seen = append(seen, e.Key)
		if len(seen) == 2 {
			break
		}
	}
	assert.Equal(t, []string{"k00", "k01"}, seen)
}

func TestKV_PostCloseOps(t *testing.T) {
	c, err := openKVAt(t, t.TempDir())
	require.NoError(t, err)
	require.NoError(t, c.Close())

	require.ErrorIs(t, c.put("k", "v"), rocksdb.ErrStoreClosed)
	require.ErrorIs(t, c.del("k"), rocksdb.ErrStoreClosed)
	_, _, err = c.get("k")
	require.ErrorIs(t, err, rocksdb.ErrStoreClosed)
	require.ErrorIs(t, c.batch(func(batchWriter) error { return nil }), rocksdb.ErrStoreClosed)

	var iterErr error
	for _, e := range c.prefixScan("") {
		iterErr = e
	}
	require.ErrorIs(t, iterErr, rocksdb.ErrStoreClosed)
}

func TestKV_GracefulCloseAndReopen(t *testing.T) {
	path := t.TempDir()

	first, err := openKVAt(t, path)
	require.NoError(t, err)
	require.NoError(t, first.put("streaming:last_committed_ledger", "999"))
	require.NoError(t, first.put("config:ledgers_per_tx_index", "100000"))
	require.NoError(t, first.put("chunk:00000001:lfs", "1"))
	require.NoError(t, first.Close())

	second, err := openKVAt(t, path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = second.Close() })

	assert.Equal(t, "999", kvGetHit(t, second, "streaming:last_committed_ledger"))
	assert.Equal(t, "100000", kvGetHit(t, second, "config:ledgers_per_tx_index"))
	assert.Equal(t, "1", kvGetHit(t, second, "chunk:00000001:lfs"))
}

func TestKV_ConcurrentOpsAndCloseRaceFree(t *testing.T) {
	c := openTestKV(t)
	require.NoError(t, c.put("seed", "1"))

	var wg sync.WaitGroup
	var stop atomic.Bool
	const workers = 4
	for w := range workers {
		wg.Go(func() {
			for i := 0; !stop.Load(); i++ {
				_ = c.put(fmt.Sprintf("w%d-k%05d", w, i), "v")
			}
		})
		wg.Go(func() {
			for !stop.Load() {
				_, _, _ = c.get("seed")
			}
		})
		wg.Go(func() {
			for !stop.Load() {
				_ = c.batch(func(b batchWriter) error {
					b.Put("batched", "1")
					return nil
				})
			}
		})
		wg.Go(func() {
			for !stop.Load() {
				for _, err := range c.prefixScan("w") {
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

	require.ErrorIs(t, c.put("k", "v"), rocksdb.ErrStoreClosed)
}
