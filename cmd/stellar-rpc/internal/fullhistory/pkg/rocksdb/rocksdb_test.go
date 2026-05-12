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

// newTestLogger returns a fresh logger writing into buf so tests can
// assert log content without any fixture machinery.
func newTestLogger(buf *bytes.Buffer) *supportlog.Entry {
	log := supportlog.New()
	log.SetLevel(logrus.DebugLevel)
	log.SetOutput(buf)
	return log
}

// silentLogger returns a fresh logger that drops everything — used by
// tests that don't care about log output.
func silentLogger() *supportlog.Entry {
	var buf bytes.Buffer
	return newTestLogger(&buf)
}

// txhashCFNames returns the 16-CF naming scheme used by the hot
// txhash store: lower-case hex nibbles "cf-0" through "cf-f", with
// transactions routed to a CF by `txhash[0] >> 4`.
func txhashCFNames() []string {
	const hex = "0123456789abcdef"
	names := make([]string, 16)
	for i := range 16 {
		names[i] = "cf-" + string(hex[i])
	}
	return names
}

// openTestStore is the standard test setup: New + Open against a fresh
// tempdir, with cleanup registered. Lets tests focus on the behavior
// they're checking.
func openTestStore(t *testing.T, cfNames []string) *Store {
	t.Helper()
	s, err := New(Config{Path: t.TempDir(), ColumnFamilies: cfNames, Logger: silentLogger()})
	require.NoError(t, err)
	require.NoError(t, s.Open())
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// TestMain implements a sub-process re-exec hook for the cross-process
// flock test. When ROCKSDB_LOCK_PROBE is set, the test binary acts as
// a lock probe instead of running the suite.
func TestMain(m *testing.M) {
	if os.Getenv("ROCKSDB_LOCK_PROBE") == "1" {
		s, err := New(Config{
			Path:   os.Getenv("ROCKSDB_LOCK_PROBE_PATH"),
			Logger: silentLogger(),
		})
		if err != nil {
			os.Stderr.WriteString(err.Error())
			os.Exit(2)
		}
		if err := s.Open(); err != nil {
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

func TestOpen_HappyPathDefaultCF(t *testing.T) {
	s, err := New(Config{Path: t.TempDir(), Logger: silentLogger()})
	require.NoError(t, err)
	assert.NoError(t, s.Open())
	assert.NoError(t, s.Close())
}

// Open is idempotent: calling it twice on the same Store is a no-op
// the second time. The underlying RocksDB is opened once.
func TestOpen_IdempotentOnSameStore(t *testing.T) {
	s, err := New(Config{Path: t.TempDir(), Logger: silentLogger()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	require.NoError(t, s.Open())
	// Second + third Open are no-ops, return same nil.
	assert.NoError(t, s.Open())
	assert.NoError(t, s.Open())

	// And the Store is fully usable.
	assert.NoError(t, s.Put("default", []byte("k"), []byte("v")))
	val, found, err := s.Get("default", []byte("k"))
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, []byte("v"), val)
}

// Concurrent Open + Close from two goroutines: either ordering is
// fine, but the wrapper must serialize the two via openOnce so the
// just-opened DB isn't leaked when Close races ahead of Open's
// internal grocksdb-open call.
// Run several iterations under -race to flush out any unsafe access.
func TestStore_ConcurrentOpenAndClose(t *testing.T) {
	for range 20 {
		s, err := New(Config{Path: t.TempDir(), Logger: silentLogger()})
		require.NoError(t, err)

		var wg sync.WaitGroup
		wg.Go(func() { _ = s.Open() })
		wg.Go(func() { _ = s.Close() })
		wg.Wait()

		// Whichever ordering won, a follow-up Close is a no-op.
		require.NoError(t, s.Close())
	}
}

// Two separate Stores opened against the same Path collide on
// grocksdb's flock — sharing a directory means sharing a Store, not
// two of them.
func TestOpen_TwoStoresSamePathCollide(t *testing.T) {
	dir := t.TempDir()
	s1, err := New(Config{Path: dir, Logger: silentLogger()})
	require.NoError(t, err)
	require.NoError(t, s1.Open())
	t.Cleanup(func() { _ = s1.Close() })

	s2, err := New(Config{Path: dir, Logger: silentLogger()})
	require.NoError(t, err)
	assert.Error(t, s2.Open())
}

func TestOpen_CreatesMissingDirectoryWithParents(t *testing.T) {
	parent := t.TempDir()
	target := filepath.Join(parent, "active", "ledgers")

	s, err := New(Config{Path: target, Logger: silentLogger()})
	require.NoError(t, err)
	require.NoError(t, s.Open())
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

	// Explicit "default" reads the same key.
	val2, found2, err := s.Get("default", []byte("k1"))
	require.NoError(t, err)
	assert.True(t, found2)
	assert.Equal(t, []byte("v1"), val2)

	// Missing key: (nil, false, nil) — absence is not an error.
	_, found3, err := s.Get("default", []byte("never-written"))
	require.NoError(t, err)
	assert.False(t, found3)
}

// Put / Get / etc. before Open returns ErrStoreNotOpened.
func TestStore_OpsBeforeOpenError(t *testing.T) {
	s, err := New(Config{Path: t.TempDir(), Logger: silentLogger()})
	require.NoError(t, err)

	require.ErrorIs(t, s.Put("", []byte("k"), []byte("v")), ErrStoreNotOpened)
	_, _, err = s.Get("", []byte("k"))
	require.ErrorIs(t, err, ErrStoreNotOpened)
	require.ErrorIs(t, s.Delete("", []byte("k")), ErrStoreNotOpened)
	assert.ErrorIs(t, s.Flush(), ErrStoreNotOpened)
}

// Flush on an open Store with pending writes succeeds.
func TestStore_FlushSucceedsOnOpenStore(t *testing.T) {
	s := openTestStore(t, nil)
	require.NoError(t, s.Put("default", []byte("k"), []byte("v")))
	assert.NoError(t, s.Flush())
}

// 16 CFs, nibble-routed (txhash store flavor). Writes to one CF must
// not appear in another — the property that makes nibble routing safe.
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

// Arbitrary multi-named CFs (events-store flavor). Unknown CF surfaces
// ErrCFNotFound from Put + Get + Delete + Iterate.
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

// Delete is idempotent at the wrapper level. Cleanup_txhash deletes
// per-chunk meta keys; on resume after partial cleanup, some are
// already gone — treating "delete missing key" as success keeps the
// re-run from erroring.
func TestStore_DeleteIsIdempotent(t *testing.T) {
	s := openTestStore(t, nil)

	assert.NoError(t, s.Delete("default", []byte("never-written")))

	assert.NoError(t, s.Put("default", []byte("k"), []byte("v")))
	assert.NoError(t, s.Delete("default", []byte("k")))
	_, found, err := s.Get("default", []byte("k"))
	require.NoError(t, err)
	assert.False(t, found)

	assert.NoError(t, s.Delete("default", []byte("k")))
}

// Iterate returns keys in sorted byte order, scoped to the prefix.
// Big-endian encoding sorts lexicographically the same way it sorts
// numerically — what makes range queries O(window-size).
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
		require.NoError(t, s.Put("default", []byte(k), []byte(v)))
	}

	var got []string
	for e, err := range s.Iterate("default", []byte("chunk:0000000")) {
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

func TestOpen_DataPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()

	first, err := New(Config{Path: dir, Logger: silentLogger()})
	require.NoError(t, err)
	require.NoError(t, first.Open())
	assert.NoError(t, first.Put("default", []byte("persist"), []byte("yes")))
	assert.NoError(t, first.Close())

	second, err := New(Config{Path: dir, Logger: silentLogger()})
	require.NoError(t, err)
	require.NoError(t, second.Open())
	t.Cleanup(func() { _ = second.Close() })

	val, found, err := second.Get("default", []byte("persist"))
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, []byte("yes"), val)
}

// Every Store method run after Close returns ErrStoreClosed —
// protects callers from a Layer-2 facade that loses track of its own
// lifecycle.
func TestStore_OpsAfterCloseFailWithErrStoreClosed(t *testing.T) {
	s := openTestStore(t, nil)
	require.NoError(t, s.Close())

	tests := []struct {
		name string
		run  func() error
	}{
		{"Put", func() error { return s.Put("default", []byte("k"), []byte("v")) }},
		{"Get", func() error { _, _, err := s.Get("default", []byte("k")); return err }},
		{"Delete", func() error { return s.Delete("default", []byte("k")) }},
		{"Iterate", func() error {
			for _, err := range s.Iterate("default", nil) {
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

// Close idempotency:
//   - calling Close twice on an Opened Store is a no-op.
//   - Close on a New'd-but-never-Opened Store is also a no-op (s.db is
//     nil; the impl branches early).
func TestStore_CloseLifecycle(t *testing.T) {
	t.Run("double close after open", func(t *testing.T) {
		s, err := New(Config{Path: t.TempDir(), Logger: silentLogger()})
		require.NoError(t, err)
		require.NoError(t, s.Open())
		assert.NoError(t, s.Close())
		assert.NoError(t, s.Close())
	})

	t.Run("close on never-opened store", func(t *testing.T) {
		s, err := New(Config{Path: t.TempDir(), Logger: silentLogger()})
		require.NoError(t, err)
		assert.NoError(t, s.Close())
		assert.NoError(t, s.Close())
	})
}

// Iterate corner cases: empty prefix scans the whole CF; an empty CF
// returns no keys without error; an unknown CF yields one tuple with
// ErrCFNotFound and no Entry.
func TestStore_IterateCorners(t *testing.T) {
	t.Run("empty prefix scans whole CF", func(t *testing.T) {
		s := openTestStore(t, nil)
		require.NoError(t, s.Put("default", []byte("k1"), []byte("v")))
		require.NoError(t, s.Put("default", []byte("k2"), []byte("v")))
		require.NoError(t, s.Put("default", []byte("k3"), []byte("v")))

		var got []string
		for e, err := range s.Iterate("default", nil) {
			require.NoError(t, err)
			got = append(got, string(e.Key))
		}
		assert.Equal(t, []string{"k1", "k2", "k3"}, got)
	})

	t.Run("empty CF returns no keys, no error", func(t *testing.T) {
		s := openTestStore(t, nil)

		count := 0
		for _, err := range s.Iterate("default", nil) {
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

// Cross-process flock: a second Open from a different process against
// the same directory fails. RocksDB's native LOCK file gives us this.
func TestOpen_FlockBlocksOtherProcess(t *testing.T) {
	dir := t.TempDir()
	primary, err := New(Config{Path: dir, Logger: silentLogger()})
	require.NoError(t, err)
	require.NoError(t, primary.Open())
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

// Concurrent Put / Get / Iterate goroutines hammering the store while
// another goroutine calls Close must not crash, panic, or trigger the
// race detector. Each in-flight operation holds the lifecycle
// read-lock for the duration of its underlying C call; Close waits
// for that lock before tearing down the C-side DB.
//
// Without the read-write mutex on Store, a goroutine that passed
// checkOpen but is still inside its C call would run against memory
// that Close has freed, producing a process-level segfault.
//
// Run this test with `-race` to validate the absence of any
// unsynchronized access to s.db.
func TestStore_ConcurrentOpsAndCloseRaceFree(t *testing.T) {
	s := openTestStore(t, nil)
	// Pre-populate so the Iterate workers have something to scan.
	for i := range 100 {
		require.NoError(t, s.Put("default", fmt.Appendf(nil, "k%03d", i), []byte("v")))
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
				_ = s.Put("default", fmt.Appendf(nil, "w%d-k%05d", w, i), []byte("v"))
			}
		})
		wg.Go(func() {
			for i := 0; !stop.Load(); i++ {
				_, _, _ = s.Get("default", fmt.Appendf(nil, "k%03d", i%100))
			}
		})
		wg.Go(func() {
			for !stop.Load() {
				for _, err := range s.Iterate("default", []byte("k")) {
					if err != nil {
						return
					}
				}
			}
		})
		wg.Go(func() {
			for i := 0; !stop.Load(); i++ {
				_ = s.Batch(func(b *BatchWriter) error {
					b.Put("default", fmt.Appendf(nil, "b%d-k%05d", w, i), []byte("v"))
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
	assert.ErrorIs(t, s.Put("default", []byte("k"), []byte("v")), ErrStoreClosed)
}

// Close must wait for an in-flight operation's read-lock to release
// before tearing down. Verified deterministically by parking an
// Iterate goroutine inside its loop body (so its RLock is held) and
// observing that a concurrent Close blocks until the iteration is
// released.
//
// This is the lock-mechanics test for the design choice spelled out
// in the mu field doc on Store: Close serializes only against
// in-flight ops; it does not serialize against arbitrary Layer-2
// activity.
func TestStore_CloseWaitsForInflightIterate(t *testing.T) {
	s := openTestStore(t, nil)
	for i := range 10 {
		require.NoError(t, s.Put("default", fmt.Appendf(nil, "k%03d", i), []byte("v")))
	}

	iterParked := make(chan struct{})
	releaseIter := make(chan struct{})
	iterDone := make(chan struct{})

	go func() {
		defer close(iterDone)
		first := true
		for _, err := range s.Iterate("default", []byte("k")) {
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
