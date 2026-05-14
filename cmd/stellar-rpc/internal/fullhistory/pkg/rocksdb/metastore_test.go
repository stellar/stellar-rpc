package rocksdb

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

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
)

var _ stores.MetaStore = (*MetaStore)(nil)

func openTestMetaStore(t *testing.T) *MetaStore {
	t.Helper()
	m, err := NewMetaStore(t.TempDir(), silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })
	return m
}

func TestNewMetaStore_ValidatesInputs(t *testing.T) {
	_, err := NewMetaStore("", silentLogger())
	require.ErrorIs(t, err, ErrInvalidConfig)

	_, err = NewMetaStore(t.TempDir(), nil)
	require.ErrorIs(t, err, ErrInvalidConfig)
}

func TestNewMetaStore_CreatesMissingDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "subdir-never-created")
	m, err := NewMetaStore(path, silentLogger())
	require.NoError(t, err)
	require.NotNil(t, m)
	t.Cleanup(func() { _ = m.Close() })
}

func TestMetaStore_CloseIsIdempotent(t *testing.T) {
	m, err := NewMetaStore(t.TempDir(), silentLogger())
	require.NoError(t, err)

	require.NoError(t, m.Close())
	require.NoError(t, m.Close())
}

func TestMetaStore_DefaultTuningInstallsNoCacheOrFilter(t *testing.T) {
	m := openTestMetaStore(t)
	assert.Nil(t, m.store.cache)
	assert.Nil(t, m.store.filter)
}

func TestMetaStore_GetMissReturnsErrNotFound(t *testing.T) {
	m := openTestMetaStore(t)
	_, err := m.Get("nothing-here")
	require.ErrorIs(t, err, stores.ErrNotFound)
}

func TestMetaStore_PutGetRoundTrip(t *testing.T) {
	m := openTestMetaStore(t)

	require.NoError(t, m.Put("streaming:last_committed_ledger", "123456"))
	got, err := m.Get("streaming:last_committed_ledger")
	require.NoError(t, err)
	assert.Equal(t, "123456", got)
}

func TestMetaStore_PutOverwritesPriorValue(t *testing.T) {
	m := openTestMetaStore(t)

	require.NoError(t, m.Put("k", "v1"))
	require.NoError(t, m.Put("k", "v2"))
	got, err := m.Get("k")
	require.NoError(t, err)
	assert.Equal(t, "v2", got)
}

func TestMetaStore_PutEmptyValueIsDistinctFromMissing(t *testing.T) {
	m := openTestMetaStore(t)

	_, err := m.Get("key-empty")
	require.ErrorIs(t, err, stores.ErrNotFound)

	require.NoError(t, m.Put("key-empty", ""))
	got, err := m.Get("key-empty")
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestMetaStore_PutBinaryValueRoundTrips(t *testing.T) {
	m := openTestMetaStore(t)

	// Non-UTF-8 byte sequence inside a Go string — valid since Go
	// strings can hold arbitrary bytes.
	binary := string([]byte{0x00, 0xff, 0x7f, 0x80, 0xfe})
	require.NoError(t, m.Put("binary-key", binary))
	got, err := m.Get("binary-key")
	require.NoError(t, err)
	assert.Equal(t, binary, got)
}

func TestMetaStore_DeleteRemovesKey(t *testing.T) {
	m := openTestMetaStore(t)

	require.NoError(t, m.Put("k", "v"))
	require.NoError(t, m.Delete("k"))

	_, err := m.Get("k")
	require.ErrorIs(t, err, stores.ErrNotFound)
}

func TestMetaStore_DeleteMissingKeyIsIdempotent(t *testing.T) {
	m := openTestMetaStore(t)

	require.NoError(t, m.Delete("never-existed"))
	require.NoError(t, m.Delete("never-existed"))
}

func TestMetaStore_NumericRoundTripWithSprintf(t *testing.T) {
	m := openTestMetaStore(t)
	const want uint32 = 4_294_967_290

	require.NoError(t, m.Put("streaming:last_committed_ledger", strconv.FormatUint(uint64(want), 10)))
	raw, err := m.Get("streaming:last_committed_ledger")
	require.NoError(t, err)
	got, err := strconv.ParseUint(raw, 10, 32)
	require.NoError(t, err)
	assert.Equal(t, want, uint32(got))
}

func TestMetaStore_BatchCommitsAtomically(t *testing.T) {
	m := openTestMetaStore(t)
	require.NoError(t, m.Put("chunk:00000010:txhash", "1"))
	require.NoError(t, m.Put("chunk:00000011:txhash", "1"))

	require.NoError(t, m.Batch(func(b stores.MetaStoreBatch) error {
		b.Put("index:00000002:txhash", "9")
		b.Delete("chunk:00000010:txhash")
		b.Delete("chunk:00000011:txhash")
		return nil
	}))

	got, err := m.Get("index:00000002:txhash")
	require.NoError(t, err)
	assert.Equal(t, "9", got)

	_, err = m.Get("chunk:00000010:txhash")
	require.ErrorIs(t, err, stores.ErrNotFound)
	_, err = m.Get("chunk:00000011:txhash")
	require.ErrorIs(t, err, stores.ErrNotFound)
}

func TestMetaStore_BatchEmptyCallbackCommitsCleanly(t *testing.T) {
	m := openTestMetaStore(t)
	require.NoError(t, m.Batch(func(stores.MetaStoreBatch) error { return nil }))
}

func TestMetaStore_BatchCallbackErrorPropagatesNoWrites(t *testing.T) {
	m := openTestMetaStore(t)
	require.NoError(t, m.Put("k1", "before"))

	cbErr := errors.New("caller bailing out")
	err := m.Batch(func(b stores.MetaStoreBatch) error {
		b.Put("k1", "after")
		b.Put("k2", "new")
		return cbErr
	})
	require.ErrorIs(t, err, cbErr)

	// Neither write committed — k1 is unchanged, k2 absent.
	got, err := m.Get("k1")
	require.NoError(t, err)
	assert.Equal(t, "before", got)

	_, err = m.Get("k2")
	require.ErrorIs(t, err, stores.ErrNotFound)
}

func TestMetaStore_PrefixScanFiltersByPrefix(t *testing.T) {
	m := openTestMetaStore(t)

	require.NoError(t, m.Put("chunk:00000001:lfs", "1"))
	require.NoError(t, m.Put("chunk:00000002:lfs", "1"))
	require.NoError(t, m.Put("chunk:00000003:lfs", "1"))
	require.NoError(t, m.Put("index:00000001:txhash", "1"))

	var keys []string
	for e, err := range m.PrefixScan("chunk:") {
		require.NoError(t, err)
		keys = append(keys, e.Key)
	}
	assert.Equal(t, []string{
		"chunk:00000001:lfs",
		"chunk:00000002:lfs",
		"chunk:00000003:lfs",
	}, keys)
}

func TestMetaStore_PrefixScanEmptyPrefixWalksWholeStore(t *testing.T) {
	m := openTestMetaStore(t)

	require.NoError(t, m.Put("a", "1"))
	require.NoError(t, m.Put("b", "2"))
	require.NoError(t, m.Put("c", "3"))

	var seen []string
	for e, err := range m.PrefixScan("") {
		require.NoError(t, err)
		seen = append(seen, e.Key+"="+e.Value)
	}
	assert.Equal(t, []string{"a=1", "b=2", "c=3"}, seen)
}

func TestMetaStore_PrefixScanNoMatchesYieldsNothing(t *testing.T) {
	m := openTestMetaStore(t)
	require.NoError(t, m.Put("a", "1"))

	count := 0
	for _, err := range m.PrefixScan("never-matches:") {
		require.NoError(t, err)
		count++
	}
	assert.Equal(t, 0, count)
}

func TestMetaStore_PrefixScanShowsGapsInKeyspace(t *testing.T) {
	m := openTestMetaStore(t)
	// Gap at chunk:00000002.
	require.NoError(t, m.Put("chunk:00000001:lfs", "1"))
	require.NoError(t, m.Put("chunk:00000003:lfs", "1"))
	require.NoError(t, m.Put("chunk:00000004:lfs", "1"))

	var keys []string
	for e, err := range m.PrefixScan("chunk:") {
		require.NoError(t, err)
		keys = append(keys, e.Key)
	}
	assert.Equal(t, []string{
		"chunk:00000001:lfs",
		"chunk:00000003:lfs",
		"chunk:00000004:lfs",
	}, keys)
}

func TestMetaStore_PrefixScanCallerBreakStopsCleanly(t *testing.T) {
	m := openTestMetaStore(t)
	for i := range 5 {
		require.NoError(t, m.Put(fmt.Sprintf("k%02d", i), "v"))
	}

	var seen []string
	for e, err := range m.PrefixScan("k") {
		require.NoError(t, err)
		seen = append(seen, e.Key)
		if len(seen) == 2 {
			break
		}
	}
	assert.Equal(t, []string{"k00", "k01"}, seen)
}

func TestMetaStore_PostCloseOps(t *testing.T) {
	m, err := NewMetaStore(t.TempDir(), silentLogger())
	require.NoError(t, err)
	require.NoError(t, m.Close())

	require.ErrorIs(t, m.Put("k", "v"), stores.ErrStoreClosed)
	require.ErrorIs(t, m.Delete("k"), stores.ErrStoreClosed)
	_, err = m.Get("k")
	require.ErrorIs(t, err, stores.ErrStoreClosed)
	require.ErrorIs(t, m.Batch(func(stores.MetaStoreBatch) error { return nil }), stores.ErrStoreClosed)

	var iterErr error
	for _, e := range m.PrefixScan("") {
		iterErr = e
	}
	require.ErrorIs(t, iterErr, stores.ErrStoreClosed)
}

func TestMetaStore_GracefulCloseAndReopen(t *testing.T) {
	path := t.TempDir()

	first, err := NewMetaStore(path, silentLogger())
	require.NoError(t, err)
	require.NoError(t, first.Put("streaming:last_committed_ledger", "999"))
	require.NoError(t, first.Put("config:ledgers_per_tx_index", "100000"))
	require.NoError(t, first.Put("chunk:00000001:lfs", "1"))
	require.NoError(t, first.Close())

	second, err := NewMetaStore(path, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = second.Close() })

	v, err := second.Get("streaming:last_committed_ledger")
	require.NoError(t, err)
	assert.Equal(t, "999", v)

	v, err = second.Get("config:ledgers_per_tx_index")
	require.NoError(t, err)
	assert.Equal(t, "100000", v)

	v, err = second.Get("chunk:00000001:lfs")
	require.NoError(t, err)
	assert.Equal(t, "1", v)
}

func TestMetaStore_ConcurrentOpsAndCloseRaceFree(t *testing.T) {
	m := openTestMetaStore(t)
	require.NoError(t, m.Put("seed", "1"))

	var wg sync.WaitGroup
	var stop atomic.Bool
	const workers = 4
	for w := range workers {
		wg.Go(func() {
			for i := 0; !stop.Load(); i++ {
				_ = m.Put(fmt.Sprintf("w%d-k%05d", w, i), "v")
			}
		})
		wg.Go(func() {
			for !stop.Load() {
				_, _ = m.Get("seed")
			}
		})
		wg.Go(func() {
			for !stop.Load() {
				_ = m.Batch(func(b stores.MetaStoreBatch) error {
					b.Put("batched", "1")
					return nil
				})
			}
		})
		wg.Go(func() {
			for !stop.Load() {
				for _, err := range m.PrefixScan("w") {
					if err != nil {
						break
					}
				}
			}
		})
	}

	time.Sleep(50 * time.Millisecond)
	require.NoError(t, m.Close())
	stop.Store(true)
	wg.Wait()

	require.ErrorIs(t, m.Put("k", "v"), stores.ErrStoreClosed)
}

func TestMetaStore_CloseWaitsForInflightOp(t *testing.T) {
	m := openTestMetaStore(t)

	batchParked := make(chan struct{})
	releaseBatch := make(chan struct{})
	batchDone := make(chan struct{})

	go func() {
		defer close(batchDone)
		assert.NoError(t, m.Batch(func(b stores.MetaStoreBatch) error {
			b.Put("dummy", "1")
			close(batchParked)
			<-releaseBatch
			return nil
		}))
	}()

	<-batchParked
	closeDone := make(chan struct{})
	go func() {
		defer close(closeDone)
		assert.NoError(t, m.Close())
	}()

	select {
	case <-closeDone:
		t.Fatal("Close completed while an in-flight Batch held the read-lock")
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseBatch)
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("Close did not complete after Batch released its read-lock")
	}
	<-batchDone
}
