package rocksdb

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
)

func newTestLogger(buf *bytes.Buffer) *supportlog.Entry {
	log := supportlog.New()
	log.SetLevel(logrus.DebugLevel)
	log.SetOutput(buf)
	return log
}

func silentLogger() *supportlog.Entry {
	var buf bytes.Buffer
	return newTestLogger(&buf)
}

func txhashCFNames() []string {
	const hex = "0123456789abcdef"
	names := make([]string, 16)
	for i := range 16 {
		names[i] = "cf-" + string(hex[i])
	}
	return names
}

func openTestStore(t *testing.T, cfNames []string) *Store {
	t.Helper()
	s, err := New(Config{Path: t.TempDir(), ColumnFamilies: cfNames, Logger: silentLogger()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestMain(m *testing.M) {
	if os.Getenv("ROCKSDB_LOCK_PROBE") == "1" {
		_, err := New(Config{
			Path:   os.Getenv("ROCKSDB_LOCK_PROBE_PATH"),
			Logger: silentLogger(),
		})
		if err != nil {
			os.Stderr.WriteString(err.Error())
			os.Exit(2)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestNew_RejectsEmptyPath(t *testing.T) {
	_, err := New(Config{Logger: silentLogger()})
	assert.ErrorIs(t, err, ErrInvalidConfig)
}

func TestNew_RejectsMissingLogger(t *testing.T) {
	_, err := New(Config{Path: t.TempDir()})
	assert.ErrorIs(t, err, ErrInvalidConfig)
}

func TestNew_HappyPathDefaultCF(t *testing.T) {
	s, err := New(Config{Path: t.TempDir(), Logger: silentLogger()})
	require.NoError(t, err)
	assert.NoError(t, s.Close())
}

func TestNew_TwoStoresSamePathCollide(t *testing.T) {
	dir := t.TempDir()
	s1, err := New(Config{Path: dir, Logger: silentLogger()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s1.Close() })

	s2, err := New(Config{Path: dir, Logger: silentLogger()})
	require.Error(t, err)
	assert.Nil(t, s2)
}

func TestStore_ConstructAndOpenFailureFreesCacheAndFilter(t *testing.T) {
	dir := t.TempDir()
	tuning := Tuning{BlockCacheMB: 4, BloomFilterBitsPerKey: 10}

	holder, err := New(Config{Path: dir, Logger: silentLogger(), Tuning: tuning})
	require.NoError(t, err)
	t.Cleanup(func() { _ = holder.Close() })

	collider := &Store{cfg: Config{Path: dir, Logger: silentLogger(), Tuning: tuning}}
	require.Error(t, collider.constructAndOpen())

	assert.Nil(t, collider.cache)
	assert.Nil(t, collider.filter)
}

func TestNew_CreatesMissingDirectoryWithParents(t *testing.T) {
	parent := t.TempDir()
	target := filepath.Join(parent, "active", "ledgers")

	s, err := New(Config{Path: target, Logger: silentLogger()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	info, err := os.Stat(target)
	require.NoError(t, err)
	assert.True(t, info.IsDir())
	assert.Equal(t, os.FileMode(0o700), info.Mode().Perm())
}

func TestStore_PutGet_DefaultCF(t *testing.T) {
	s := openTestStore(t, nil)

	// Empty CF name normalizes to default.
	require.NoError(t, s.Put("", []byte("k1"), []byte("v1")))
	val, found, err := s.Get("", []byte("k1"))
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, []byte("v1"), val)

	// Explicit defaultCFName reads the same key.
	val2, found2, err := s.Get(defaultCFName, []byte("k1"))
	require.NoError(t, err)
	assert.True(t, found2)
	assert.Equal(t, []byte("v1"), val2)

	// Missing key: (nil, false, nil) — absence is not an error.
	_, found3, err := s.Get(defaultCFName, []byte("never-written"))
	require.NoError(t, err)
	assert.False(t, found3)
}

func TestStore_FirstLastKey(t *testing.T) {
	s := openTestStore(t, nil)

	// Empty default CF: ok=false, no error, at both ends.
	_, ok, err := s.FirstKey("")
	require.NoError(t, err)
	require.False(t, ok)
	_, ok, err = s.LastKey("")
	require.NoError(t, err)
	require.False(t, ok)

	// EncodeUint32 is big-endian, so byte-lex key order is numeric order:
	// insert out of order and expect the min/max back.
	for _, n := range []uint32{500, 1, 9999, 42} {
		require.NoError(t, s.Put("", EncodeUint32(n), []byte{byte(n)}))
	}
	first, ok, err := s.FirstKey("")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint32(1), DecodeUint32(first))

	last, ok, err := s.LastKey("")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint32(9999), DecodeUint32(last))
}

func TestStore_FlushSucceedsOnOpenStore(t *testing.T) {
	s := openTestStore(t, nil)
	require.NoError(t, s.Put(defaultCFName, []byte("k"), []byte("v")))
	assert.NoError(t, s.Flush())
}

func TestStore_16CF_IsolatedWrites(t *testing.T) {
	cfNames := txhashCFNames()
	s := openTestStore(t, cfNames)

	key := []byte("k")
	for i, cf := range cfNames {
		require.NoError(t, s.Put(cf, key, []byte{byte(i)}))
	}
	for i, cf := range cfNames {
		val, found, err := s.Get(cf, key)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, []byte{byte(i)}, val)
	}

	// Per-CF absence: write a key to cf-0 only.
	require.NoError(t, s.Put("cf-0", []byte("only-in-cf-0"), []byte("a")))
	for _, cf := range cfNames[1:] {
		_, found, err := s.Get(cf, []byte("only-in-cf-0"))
		require.NoError(t, err)
		assert.False(t, found)
	}
}

func TestStore_MultiNamedCFs(t *testing.T) {
	s := openTestStore(t, []string{"basic", "offsets", "hot-tx"})

	assert.NoError(t, s.Put("basic", []byte("k"), []byte("v-basic")))
	assert.NoError(t, s.Put("offsets", []byte("k"), []byte("v-offsets")))
	assert.NoError(t, s.Put("hot-tx", []byte("k"), []byte("v-hot-tx")))

	for cf, want := range map[string]string{
		"basic":   "v-basic",
		"offsets": "v-offsets",
		"hot-tx":  "v-hot-tx",
	} {
		got, found, err := s.Get(cf, []byte("k"))
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, []byte(want), got)
	}

	require.ErrorIs(t, s.Put("not-configured", []byte("k"), []byte("v")), ErrCFNotFound)
	_, _, err := s.Get("not-configured", []byte("k"))
	assert.ErrorIs(t, err, ErrCFNotFound)
}

func TestStore_BatchMultiGet_RoundTrip(t *testing.T) {
	s := openTestStore(t, nil)

	for i := range 5 {
		require.NoError(t, s.Put(defaultCFName, []byte{byte(i)}, []byte{byte(100 + i)}))
	}

	keys := [][]byte{{0}, {1}, {2}, {3}, {4}}
	values, err := s.BatchMultiGet(defaultCFName, keys)
	require.NoError(t, err)
	require.Len(t, values, len(keys))
	for i, v := range values {
		assert.Equal(t, []byte{byte(100 + i)}, v)
	}
}

func TestStore_BatchMultiGet_MissingKeysReturnNil(t *testing.T) {
	s := openTestStore(t, nil)

	require.NoError(t, s.Put(defaultCFName, []byte{1}, []byte("one")))
	require.NoError(t, s.Put(defaultCFName, []byte{3}, []byte("three")))

	// Keys {0, 2, 4} are absent; {1, 3} are present. Strict ascending order.
	keys := [][]byte{{0}, {1}, {2}, {3}, {4}}
	values, err := s.BatchMultiGet(defaultCFName, keys)
	require.NoError(t, err)
	require.Len(t, values, 5)
	assert.Nil(t, values[0])
	assert.Equal(t, []byte("one"), values[1])
	assert.Nil(t, values[2])
	assert.Equal(t, []byte("three"), values[3])
	assert.Nil(t, values[4])
}

func TestStore_BatchMultiGet_EmptyInput(t *testing.T) {
	s := openTestStore(t, nil)
	values, err := s.BatchMultiGet(defaultCFName, nil)
	require.NoError(t, err)
	assert.Nil(t, values)
}

func TestStore_BatchMultiGet_ClosedStoreErrors(t *testing.T) {
	s := openTestStore(t, nil)
	require.NoError(t, s.Close())

	_, err := s.BatchMultiGet(defaultCFName, [][]byte{{0}})
	require.ErrorIs(t, err, ErrStoreClosed)
}

func TestStore_BatchMultiGet_UnknownCFErrors(t *testing.T) {
	s := openTestStore(t, nil)
	_, err := s.BatchMultiGet("missing", [][]byte{{0}})
	require.ErrorIs(t, err, ErrCFNotFound)
}

func TestStore_DeleteIsIdempotent(t *testing.T) {
	s := openTestStore(t, nil)

	assert.NoError(t, s.Delete(defaultCFName, []byte("never-written")))

	assert.NoError(t, s.Put(defaultCFName, []byte("k"), []byte("v")))
	assert.NoError(t, s.Delete(defaultCFName, []byte("k")))
	_, found, err := s.Get(defaultCFName, []byte("k"))
	require.NoError(t, err)
	assert.False(t, found)

	assert.NoError(t, s.Delete(defaultCFName, []byte("k")))
}

func TestStore_Iterate_SortedPrefixScan(t *testing.T) {
	s := openTestStore(t, nil)

	inserts := map[string]string{
		"chunk:00000000:lfs":    "1",
		"chunk:00000001:lfs":    "1",
		"chunk:00000002:lfs":    "1",
		"chunk:00000005:txhash": "1", // matches prefix despite the :txhash suffix; expected in scan
		"index:00000000:txhash": "1", // does NOT match prefix; expected to be excluded
	}
	for k, v := range inserts {
		require.NoError(t, s.Put(defaultCFName, []byte(k), []byte(v)))
	}

	var got []string
	for e, err := range s.Iterate(defaultCFName, []byte("chunk:0000000")) {
		require.NoError(t, err)
		got = append(got, string(e.Key))
	}

	assert.Equal(t, []string{
		"chunk:00000000:lfs",
		"chunk:00000001:lfs",
		"chunk:00000002:lfs",
		"chunk:00000005:txhash",
	}, got)
}

func TestStore_DataPersistsAcrossNewOnSamePath(t *testing.T) {
	dir := t.TempDir()

	first, err := New(Config{Path: dir, Logger: silentLogger()})
	require.NoError(t, err)
	assert.NoError(t, first.Put(defaultCFName, []byte("persist"), []byte("yes")))
	assert.NoError(t, first.Close())

	second, err := New(Config{Path: dir, Logger: silentLogger()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = second.Close() })

	val, found, err := second.Get(defaultCFName, []byte("persist"))
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, []byte("yes"), val)
}

func TestStore_OpsAfterCloseFailWithErrStoreClosed(t *testing.T) {
	s := openTestStore(t, nil)
	require.NoError(t, s.Close())

	tests := []struct {
		name string
		run  func() error
	}{
		{"Put", func() error { return s.Put(defaultCFName, []byte("k"), []byte("v")) }},
		{"Get", func() error { _, _, err := s.Get(defaultCFName, []byte("k")); return err }},
		{"Delete", func() error { return s.Delete(defaultCFName, []byte("k")) }},
		{"Iterate", func() error {
			for _, err := range s.Iterate(defaultCFName, nil) {
				return err
			}
			return nil
		}},
		{"Batch", func() error {
			return s.Batch(func(*BatchWriter) error { return nil })
		}},
		{"Flush", func() error { return s.Flush() }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.ErrorIs(t, tc.run(), ErrStoreClosed)
		})
	}
}

func TestStore_CloseIsIdempotent(t *testing.T) {
	s, err := New(Config{Path: t.TempDir(), Logger: silentLogger()})
	require.NoError(t, err)
	assert.NoError(t, s.Close())
	assert.NoError(t, s.Close())
}

func TestStore_CloseAutoFlushesMemtable(t *testing.T) {
	dir := t.TempDir()

	s, err := New(Config{Path: dir, Logger: silentLogger()})
	require.NoError(t, err)
	assert.False(t, s.IsClosed())

	for i := range 50 {
		require.NoError(t, s.Put(defaultCFName, fmt.Appendf(nil, "k%03d", i), []byte("v")))
	}

	require.NoError(t, s.Close())
	assert.True(t, s.IsClosed())
	require.NoError(t, s.Close())

	s2, err := New(Config{Path: dir, Logger: silentLogger()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close() })

	for i := range 50 {
		v, found, err := s2.Get(defaultCFName, fmt.Appendf(nil, "k%03d", i))
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, []byte("v"), v)
	}
}

func TestStore_IsClosed(t *testing.T) {
	s, err := New(Config{Path: t.TempDir(), Logger: silentLogger()})
	require.NoError(t, err)
	assert.False(t, s.IsClosed())

	require.NoError(t, s.Close())
	assert.True(t, s.IsClosed())

	require.NoError(t, s.Close())
	assert.True(t, s.IsClosed())
}

func TestStore_IterateCorners(t *testing.T) {
	t.Run("empty prefix scans whole CF", func(t *testing.T) {
		s := openTestStore(t, nil)
		require.NoError(t, s.Put(defaultCFName, []byte("k1"), []byte("v")))
		require.NoError(t, s.Put(defaultCFName, []byte("k2"), []byte("v")))
		require.NoError(t, s.Put(defaultCFName, []byte("k3"), []byte("v")))

		var got []string
		for e, err := range s.Iterate(defaultCFName, nil) {
			require.NoError(t, err)
			got = append(got, string(e.Key))
		}
		assert.Equal(t, []string{"k1", "k2", "k3"}, got)
	})

	t.Run("empty CF returns no keys, no error", func(t *testing.T) {
		s := openTestStore(t, nil)

		count := 0
		for _, err := range s.Iterate(defaultCFName, nil) {
			require.NoError(t, err)
			count++
		}
		assert.Equal(t, 0, count)
	})

	t.Run("unknown CF yields ErrCFNotFound and stops", func(t *testing.T) {
		s := openTestStore(t, nil)

		var sawErr error
		yields := 0
		for _, err := range s.Iterate("not-configured", nil) {
			yields++
			sawErr = err
		}
		assert.Equal(t, 1, yields, "the closure yields exactly once with the error")
		assert.ErrorIs(t, sawErr, ErrCFNotFound)
	})
}

func TestNew_FlockBlocksOtherProcess(t *testing.T) {
	dir := t.TempDir()
	primary, err := New(Config{Path: dir, Logger: silentLogger()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = primary.Close() })

	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(),
		"ROCKSDB_LOCK_PROBE=1",
		"ROCKSDB_LOCK_PROBE_PATH="+dir,
	)
	out, runErr := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	require.ErrorAs(t, runErr, &exitErr)
	assert.Equal(t, 2, exitErr.ExitCode())
	assert.Contains(t, strings.ToLower(string(out)), "lock")
}

func TestStore_ConcurrentOpsAndCloseRaceFree(t *testing.T) {
	s := openTestStore(t, nil)
	// Pre-populate so the Iterate workers have something to scan.
	for i := range 100 {
		require.NoError(t, s.Put(defaultCFName, fmt.Appendf(nil, "k%03d", i), []byte("v")))
	}

	var wg sync.WaitGroup
	var stop atomic.Bool

	// Four worker types running concurrently: pure writers, pure
	// readers, iterators, and a batch writer. Each loops until stop
	// is set. ErrStoreClosed from any op after Close is expected.
	const workers = 4
	for w := range workers {
		wg.Go(func() {
			for i := 0; !stop.Load(); i++ {
				_ = s.Put(defaultCFName, fmt.Appendf(nil, "w%d-k%05d", w, i), []byte("v"))
			}
		})
		wg.Go(func() {
			for i := 0; !stop.Load(); i++ {
				_, _, _ = s.Get(defaultCFName, fmt.Appendf(nil, "k%03d", i%100))
			}
		})
		wg.Go(func() {
			for !stop.Load() {
				for _, err := range s.Iterate(defaultCFName, []byte("k")) {
					if err != nil {
						return
					}
				}
			}
		})
		wg.Go(func() {
			for i := 0; !stop.Load(); i++ {
				_ = s.Batch(func(b *BatchWriter) error {
					b.Put(defaultCFName, fmt.Appendf(nil, "b%d-k%05d", w, i), []byte("v"))
					return nil
				})
			}
		})
	}

	// Let the workers run for a bit so plenty of ops are in flight.
	// Then call Close while they're hammering. Close MUST complete
	// cleanly: it waits for every in-flight RLock to release before
	// tearing down.
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, s.Close())

	// Signal the workers to wind down and join them. The post-Close
	// loop iterations all return ErrStoreClosed and exit promptly.
	stop.Store(true)
	wg.Wait()

	// Final sanity: any new op against the closed store returns
	// ErrStoreClosed without any C-side memory access.
	assert.ErrorIs(t, s.Put(defaultCFName, []byte("k"), []byte("v")), ErrStoreClosed)
}

func TestStore_CloseWaitsForInflightIterate(t *testing.T) {
	s := openTestStore(t, nil)
	for i := range 10 {
		require.NoError(t, s.Put(defaultCFName, fmt.Appendf(nil, "k%03d", i), []byte("v")))
	}

	iterParked := make(chan struct{})
	releaseIter := make(chan struct{})
	iterDone := make(chan struct{})

	go func() {
		defer close(iterDone)
		first := true
		for _, err := range s.Iterate(defaultCFName, []byte("k")) {
			assert.NoError(t, err)
			if first {
				// Park inside the first iteration step. The producer
				// closure holds the lifecycle RLock until the range
				// loop exits, so blocking here keeps the lock held.
				close(iterParked)
				<-releaseIter
				first = false
			}
		}
	}()

	// Wait until the iteration is inside its loop body (RLock held).
	<-iterParked

	// Start Close in a goroutine. With the in-flight iteration
	// holding RLock, Close's mu.Lock() will block.
	closeDone := make(chan struct{})
	go func() {
		defer close(closeDone)
		assert.NoError(t, s.Close())
	}()

	// Close must NOT have completed yet — the iteration still holds
	// RLock. 50ms is generous; the test should reliably detect a
	// non-waiting Close in well under 1ms.
	select {
	case <-closeDone:
		t.Fatal("Close completed while an in-flight Iterate held the read-lock")
	case <-time.After(50 * time.Millisecond):
		// Good — Close is blocked.
	}

	// Release the iterator. The range loop drains its remaining keys
	// (no further blocking; the closed releaseIter unblocks every
	// subsequent select), the producer closure returns, RUnlock
	// fires via defer, and Close finally acquires WLock and tears
	// down.
	close(releaseIter)

	select {
	case <-closeDone:
		// Close finished after Iterate released its RLock.
	case <-time.After(time.Second):
		t.Fatal("Close did not complete after Iterate released its read-lock")
	}
	<-iterDone
}

func TestStore_TuningRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	s, err := New(Config{
		Path:   t.TempDir(),
		Logger: newTestLogger(&buf),
		Tuning: Tuning{
			WriteBufferMB:                  8,
			MaxWriteBufferNumber:           2,
			Level0FileNumCompactionTrigger: 4,
			Level0SlowdownWritesTrigger:    20,
			Level0StopWritesTrigger:        36,
			TargetFileSizeMB:               16,
			MaxBytesForLevelBaseMB:         64,
			MaxBackgroundJobs:              2,
			MaxOpenFiles:                   500,
			BlockCacheMB:                   4,
			BloomFilterBitsPerKey:          10,
			MaxTotalWalSizeMB:              16,
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	require.NoError(t, s.Put(defaultCFName, []byte("k"), []byte("v")))
	v, found, err := s.Get(defaultCFName, []byte("k"))
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, []byte("v"), v)
}

func TestStore_TuningZeroValue(t *testing.T) {
	var buf bytes.Buffer
	s, err := New(Config{Path: t.TempDir(), Logger: newTestLogger(&buf)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	assert.Nil(t, s.cache)
	assert.Nil(t, s.filter)

	require.NoError(t, s.Put(defaultCFName, []byte("k"), []byte("v")))
	v, found, err := s.Get(defaultCFName, []byte("k"))
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, []byte("v"), v)
}

func seedUint32Keys(t *testing.T, s *Store, seqs ...uint32) {
	t.Helper()
	for _, seq := range seqs {
		require.NoError(t, s.Put(defaultCFName, EncodeUint32(seq), []byte("v")))
	}
}

func collectIterateRange(t *testing.T, s *Store, start, end []byte) []uint32 {
	t.Helper()
	var seen []uint32
	for e, err := range s.IterateRange(defaultCFName, start, end) {
		require.NoError(t, err)
		seen = append(seen, DecodeUint32(e.Key))
	}
	return seen
}

func TestStore_IterateRange_Bounds(t *testing.T) {
	t.Run("empty CF yields nothing", func(t *testing.T) {
		s := openTestStore(t, nil)
		assert.Empty(t, collectIterateRange(t, s, nil, nil))
	})

	t.Run("nil start and nil end walks the whole CF in order", func(t *testing.T) {
		s := openTestStore(t, nil)
		seedUint32Keys(t, s, 10, 20, 30, 40, 50)
		assert.Equal(t, []uint32{10, 20, 30, 40, 50}, collectIterateRange(t, s, nil, nil))
	})

	t.Run("non-nil start with nil end walks from start to end of CF", func(t *testing.T) {
		s := openTestStore(t, nil)
		seedUint32Keys(t, s, 10, 20, 30, 40, 50)
		assert.Equal(t, []uint32{30, 40, 50}, collectIterateRange(t, s, EncodeUint32(25), nil))
	})

	t.Run("inclusive bounds [start, end] yield both ends when present", func(t *testing.T) {
		s := openTestStore(t, nil)
		seedUint32Keys(t, s, 10, 20, 30, 40, 50)
		assert.Equal(t, []uint32{20, 30, 40}, collectIterateRange(t, s, EncodeUint32(20), EncodeUint32(40)))
	})

	t.Run("end key not present in CF stops at the largest key <= end", func(t *testing.T) {
		s := openTestStore(t, nil)
		seedUint32Keys(t, s, 10, 20, 30, 40, 50)
		assert.Equal(t, []uint32{20, 30}, collectIterateRange(t, s, EncodeUint32(15), EncodeUint32(35)))
	})

	t.Run("end < start yields nothing", func(t *testing.T) {
		s := openTestStore(t, nil)
		seedUint32Keys(t, s, 10, 20, 30)
		assert.Empty(t, collectIterateRange(t, s, EncodeUint32(40), EncodeUint32(10)))
	})
}

func TestStore_IterateRange_BreakAndUnknownCF(t *testing.T) {
	t.Run("caller break stops the walk cleanly", func(t *testing.T) {
		s := openTestStore(t, nil)
		seedUint32Keys(t, s, 10, 20, 30, 40, 50)
		var seen []uint32
		for e, err := range s.IterateRange(defaultCFName, nil, nil) {
			require.NoError(t, err)
			seen = append(seen, DecodeUint32(e.Key))
			if len(seen) == 2 {
				break
			}
		}
		assert.Equal(t, []uint32{10, 20}, seen)
	})

	t.Run("unknown CF yields ErrCFNotFound once and stops", func(t *testing.T) {
		s := openTestStore(t, nil)
		var sawErr error
		yields := 0
		for _, err := range s.IterateRange("not-configured", nil, nil) {
			yields++
			sawErr = err
		}
		assert.Equal(t, 1, yields)
		require.ErrorIs(t, sawErr, ErrCFNotFound)
	})
}
