package metastore

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
)

func silentLogger() *supportlog.Entry {
	var buf bytes.Buffer
	log := supportlog.New()
	log.SetLevel(logrus.DebugLevel)
	log.SetOutput(&buf)
	return log
}

func openTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := New(t.TempDir(), silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestNew_ValidatesInputs(t *testing.T) {
	_, err := New("", silentLogger())
	require.ErrorIs(t, err, rocksdb.ErrInvalidConfig)

	_, err = New(t.TempDir(), nil)
	require.ErrorIs(t, err, rocksdb.ErrInvalidConfig)
}

func TestNew_CreatesMissingDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "subdir-never-created")
	s, err := New(path, silentLogger())
	require.NoError(t, err)
	require.NotNil(t, s)
	t.Cleanup(func() { _ = s.Close() })
}

func TestStore_CloseIsIdempotent(t *testing.T) {
	s, err := New(t.TempDir(), silentLogger())
	require.NoError(t, err)

	require.NoError(t, s.Close())
	require.NoError(t, s.Close())
}

func TestStore_GetMissReturnsErrNotFound(t *testing.T) {
	s := openTestStore(t)
	_, err := s.Get("nothing-here")
	require.ErrorIs(t, err, stores.ErrNotFound)
}

func TestStore_PutGetRoundTrip(t *testing.T) {
	s := openTestStore(t)

	require.NoError(t, s.Put("streaming:last_committed_ledger", "123456"))
	got, err := s.Get("streaming:last_committed_ledger")
	require.NoError(t, err)
	assert.Equal(t, "123456", got)
}

func TestStore_PutOverwritesPriorValue(t *testing.T) {
	s := openTestStore(t)

	require.NoError(t, s.Put("k", "v1"))
	require.NoError(t, s.Put("k", "v2"))
	got, err := s.Get("k")
	require.NoError(t, err)
	assert.Equal(t, "v2", got)
}

func TestStore_PutEmptyValueIsDistinctFromMissing(t *testing.T) {
	s := openTestStore(t)

	_, err := s.Get("key-empty")
	require.ErrorIs(t, err, stores.ErrNotFound)

	require.NoError(t, s.Put("key-empty", ""))
	got, err := s.Get("key-empty")
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestStore_PutBinaryValueRoundTrips(t *testing.T) {
	s := openTestStore(t)

	// Non-UTF-8 byte sequence inside a Go string — valid since Go
	// strings can hold arbitrary bytes.
	binary := string([]byte{0x00, 0xff, 0x7f, 0x80, 0xfe})
	require.NoError(t, s.Put("binary-key", binary))
	got, err := s.Get("binary-key")
	require.NoError(t, err)
	assert.Equal(t, binary, got)
}

func TestStore_DeleteRemovesKey(t *testing.T) {
	s := openTestStore(t)

	require.NoError(t, s.Put("k", "v"))
	require.NoError(t, s.Delete("k"))

	_, err := s.Get("k")
	require.ErrorIs(t, err, stores.ErrNotFound)
}

func TestStore_DeleteMissingKeyIsIdempotent(t *testing.T) {
	s := openTestStore(t)

	require.NoError(t, s.Delete("never-existed"))
	require.NoError(t, s.Delete("never-existed"))
}

func TestStore_NumericRoundTripWithStrconv(t *testing.T) {
	s := openTestStore(t)
	const want uint32 = 4_294_967_290

	require.NoError(t, s.Put("streaming:last_committed_ledger", strconv.FormatUint(uint64(want), 10)))
	raw, err := s.Get("streaming:last_committed_ledger")
	require.NoError(t, err)
	got, err := strconv.ParseUint(raw, 10, 32)
	require.NoError(t, err)
	assert.Equal(t, want, uint32(got))
}

func TestStore_BatchCommitsAtomically(t *testing.T) {
	s := openTestStore(t)
	require.NoError(t, s.Put("chunk:00000010:txhash", "1"))
	require.NoError(t, s.Put("chunk:00000011:txhash", "1"))

	require.NoError(t, s.Batch(func(b *BatchWriter) error {
		b.Put("index:00000002:txhash", "9")
		b.Delete("chunk:00000010:txhash")
		b.Delete("chunk:00000011:txhash")
		return nil
	}))

	got, err := s.Get("index:00000002:txhash")
	require.NoError(t, err)
	assert.Equal(t, "9", got)

	_, err = s.Get("chunk:00000010:txhash")
	require.ErrorIs(t, err, stores.ErrNotFound)
	_, err = s.Get("chunk:00000011:txhash")
	require.ErrorIs(t, err, stores.ErrNotFound)
}

func TestStore_BatchEmptyCallbackCommitsCleanly(t *testing.T) {
	s := openTestStore(t)
	require.NoError(t, s.Batch(func(*BatchWriter) error { return nil }))
}

func TestStore_BatchCallbackErrorPropagatesNoWrites(t *testing.T) {
	s := openTestStore(t)
	require.NoError(t, s.Put("k1", "before"))

	cbErr := errors.New("caller bailing out")
	err := s.Batch(func(b *BatchWriter) error {
		b.Put("k1", "after")
		b.Put("k2", "new")
		return cbErr
	})
	require.ErrorIs(t, err, cbErr)

	// Neither write committed — k1 is unchanged, k2 absent.
	got, err := s.Get("k1")
	require.NoError(t, err)
	assert.Equal(t, "before", got)

	_, err = s.Get("k2")
	require.ErrorIs(t, err, stores.ErrNotFound)
}

func TestStore_PrefixScanFiltersByPrefix(t *testing.T) {
	s := openTestStore(t)

	require.NoError(t, s.Put("chunk:00000001:lfs", "1"))
	require.NoError(t, s.Put("chunk:00000002:lfs", "1"))
	require.NoError(t, s.Put("chunk:00000003:lfs", "1"))
	require.NoError(t, s.Put("index:00000001:txhash", "1"))

	var keys []string
	for e, err := range s.PrefixScan("chunk:") {
		require.NoError(t, err)
		keys = append(keys, e.Key)
	}
	assert.Equal(t, []string{
		"chunk:00000001:lfs",
		"chunk:00000002:lfs",
		"chunk:00000003:lfs",
	}, keys)
}

func TestStore_PrefixScanEmptyPrefixWalksWholeStore(t *testing.T) {
	s := openTestStore(t)

	require.NoError(t, s.Put("a", "1"))
	require.NoError(t, s.Put("b", "2"))
	require.NoError(t, s.Put("c", "3"))

	var seen []string
	for e, err := range s.PrefixScan("") {
		require.NoError(t, err)
		seen = append(seen, e.Key+"="+e.Value)
	}
	assert.Equal(t, []string{"a=1", "b=2", "c=3"}, seen)
}

func TestStore_PrefixScanNoMatchesYieldsNothing(t *testing.T) {
	s := openTestStore(t)
	require.NoError(t, s.Put("a", "1"))

	count := 0
	for _, err := range s.PrefixScan("never-matches:") {
		require.NoError(t, err)
		count++
	}
	assert.Equal(t, 0, count)
}

func TestStore_PrefixScanShowsGapsInKeyspace(t *testing.T) {
	s := openTestStore(t)
	// Gap at chunk:00000002.
	require.NoError(t, s.Put("chunk:00000001:lfs", "1"))
	require.NoError(t, s.Put("chunk:00000003:lfs", "1"))
	require.NoError(t, s.Put("chunk:00000004:lfs", "1"))

	var keys []string
	for e, err := range s.PrefixScan("chunk:") {
		require.NoError(t, err)
		keys = append(keys, e.Key)
	}
	assert.Equal(t, []string{
		"chunk:00000001:lfs",
		"chunk:00000003:lfs",
		"chunk:00000004:lfs",
	}, keys)
}

func TestStore_PrefixScanCallerBreakStopsCleanly(t *testing.T) {
	s := openTestStore(t)
	for i := range 5 {
		require.NoError(t, s.Put(fmt.Sprintf("k%02d", i), "v"))
	}

	var seen []string
	for e, err := range s.PrefixScan("k") {
		require.NoError(t, err)
		seen = append(seen, e.Key)
		if len(seen) == 2 {
			break
		}
	}
	assert.Equal(t, []string{"k00", "k01"}, seen)
}

func TestStore_PostCloseOps(t *testing.T) {
	s, err := New(t.TempDir(), silentLogger())
	require.NoError(t, err)
	require.NoError(t, s.Close())

	require.ErrorIs(t, s.Put("k", "v"), rocksdb.ErrStoreClosed)
	require.ErrorIs(t, s.Delete("k"), rocksdb.ErrStoreClosed)
	_, err = s.Get("k")
	require.ErrorIs(t, err, rocksdb.ErrStoreClosed)
	require.ErrorIs(t, s.Batch(func(*BatchWriter) error { return nil }), rocksdb.ErrStoreClosed)

	var iterErr error
	for _, e := range s.PrefixScan("") {
		iterErr = e
	}
	require.ErrorIs(t, iterErr, rocksdb.ErrStoreClosed)
}

func TestStore_GracefulCloseAndReopen(t *testing.T) {
	path := t.TempDir()

	first, err := New(path, silentLogger())
	require.NoError(t, err)
	require.NoError(t, first.Put("streaming:last_committed_ledger", "999"))
	require.NoError(t, first.Put("config:ledgers_per_tx_index", "100000"))
	require.NoError(t, first.Put("chunk:00000001:lfs", "1"))
	require.NoError(t, first.Close())

	second, err := New(path, silentLogger())
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

func TestStore_ConcurrentOpsAndCloseRaceFree(t *testing.T) {
	s := openTestStore(t)
	require.NoError(t, s.Put("seed", "1"))

	var wg sync.WaitGroup
	var stop atomic.Bool
	const workers = 4
	for w := range workers {
		wg.Go(func() {
			for i := 0; !stop.Load(); i++ {
				_ = s.Put(fmt.Sprintf("w%d-k%05d", w, i), "v")
			}
		})
		wg.Go(func() {
			for !stop.Load() {
				_, _ = s.Get("seed")
			}
		})
		wg.Go(func() {
			for !stop.Load() {
				_ = s.Batch(func(b *BatchWriter) error {
					b.Put("batched", "1")
					return nil
				})
			}
		})
		wg.Go(func() {
			for !stop.Load() {
				for _, err := range s.PrefixScan("w") {
					if err != nil {
						break
					}
				}
			}
		})
	}

	time.Sleep(50 * time.Millisecond)
	require.NoError(t, s.Close())
	stop.Store(true)
	wg.Wait()

	require.ErrorIs(t, s.Put("k", "v"), rocksdb.ErrStoreClosed)
}
