package rocksdb

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func collectIterateSnap(t *testing.T, s *Store, snap *Snapshot, prefix []byte) []string {
	t.Helper()
	var got []string
	for e, err := range s.IterateAsOf(snap, defaultCFName, prefix) {
		require.NoError(t, err)
		got = append(got, string(e.Key))
	}
	return got
}

// TestSnapshot_RepeatableReadUnderConcurrentWrites pins the core guarantee the
// query catalog relies on: a snapshot taken at admission keeps reading the state
// as of that instant, even as writes land afterward.
func TestSnapshot_RepeatableReadUnderConcurrentWrites(t *testing.T) {
	s := openTestStore(t, nil)
	require.NoError(t, s.Put("", []byte("k"), []byte("v1")))

	snap, err := s.NewSnapshot()
	require.NoError(t, err)
	defer s.ReleaseSnapshot(snap)

	// Mutate after the snapshot: overwrite k, and add a brand-new key.
	require.NoError(t, s.Put("", []byte("k"), []byte("v2")))
	require.NoError(t, s.Put("", []byte("k2"), []byte("new")))

	// The snapshot still sees the pre-mutation value...
	val, found, err := s.GetAsOf(snap, "", []byte("k"))
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, []byte("v1"), val)

	// ...and does not see the key added after it was taken.
	_, found, err = s.GetAsOf(snap, "", []byte("k2"))
	require.NoError(t, err)
	assert.False(t, found)

	// A live read sees the newest committed state.
	val, found, err = s.Get("", []byte("k"))
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, []byte("v2"), val)
}

// TestSnapshot_IteratePinned pins that a prefix scan through a snapshot observes
// the snapshot's view for the whole walk: keys added after are invisible and keys
// deleted after are still yielded.
func TestSnapshot_IteratePinned(t *testing.T) {
	s := openTestStore(t, nil)
	for _, k := range []string{"k1", "k2", "k3"} {
		require.NoError(t, s.Put("", []byte(k), []byte("v")))
	}

	snap, err := s.NewSnapshot()
	require.NoError(t, err)
	defer s.ReleaseSnapshot(snap)

	require.NoError(t, s.Put("", []byte("k4"), []byte("v"))) // added after snapshot
	require.NoError(t, s.Delete("", []byte("k2")))           // deleted after snapshot

	// Snapshot walk: original three keys, including the since-deleted k2.
	assert.Equal(t, []string{"k1", "k2", "k3"}, collectIterateSnap(t, s, snap, []byte("k")))

	// Live walk: k2 gone, k4 present.
	var live []string
	for e, err := range s.Iterate("", []byte("k")) {
		require.NoError(t, err)
		live = append(live, string(e.Key))
	}
	assert.Equal(t, []string{"k1", "k3", "k4"}, live)
}

// TestSnapshot_ReleaseSemantics pins that a released snapshot is inert and that
// release is nil-safe and idempotent.
func TestSnapshot_ReleaseSemantics(t *testing.T) {
	s := openTestStore(t, nil)
	require.NoError(t, s.Put("", []byte("k"), []byte("v")))

	snap, err := s.NewSnapshot()
	require.NoError(t, err)

	s.ReleaseSnapshot(snap)

	// Reads through a released snapshot fail cleanly instead of crashing.
	_, _, err = s.GetAsOf(snap, "", []byte("k"))
	require.ErrorIs(t, err, ErrNilSnapshot)

	yields := 0
	var sawErr error
	for _, err := range s.IterateAsOf(snap, "", nil) {
		yields++
		sawErr = err
	}
	assert.Equal(t, 1, yields)
	require.ErrorIs(t, sawErr, ErrNilSnapshot)

	// Double release and nil release are no-ops.
	s.ReleaseSnapshot(snap)
	s.ReleaseSnapshot(nil)
}

// TestSnapshot_NilSnapshotArgs pins the nil-snapshot guards on both read paths.
func TestSnapshot_NilSnapshotArgs(t *testing.T) {
	s := openTestStore(t, nil)

	_, _, err := s.GetAsOf(nil, "", []byte("k"))
	require.ErrorIs(t, err, ErrNilSnapshot)

	yields := 0
	var sawErr error
	for _, err := range s.IterateAsOf(nil, "", nil) {
		yields++
		sawErr = err
	}
	assert.Equal(t, 1, yields)
	require.ErrorIs(t, sawErr, ErrNilSnapshot)
}

// TestSnapshot_ClosedStore pins that acquisition fails on a closed store, and a
// snapshot taken while open cannot read once the store is closed.
func TestSnapshot_ClosedStore(t *testing.T) {
	t.Run("NewSnapshot on closed store errors", func(t *testing.T) {
		s := openTestStore(t, nil)
		require.NoError(t, s.Close())
		_, err := s.NewSnapshot()
		require.ErrorIs(t, err, ErrStoreClosed)
	})

	t.Run("GetAsOf after close errors, no C access", func(t *testing.T) {
		s, err := New(Config{Path: t.TempDir(), Logger: silentLogger()})
		require.NoError(t, err)
		require.NoError(t, s.Put("", []byte("k"), []byte("v")))

		snap, err := s.NewSnapshot()
		require.NoError(t, err)

		require.NoError(t, s.Close())

		_, _, err = s.GetAsOf(snap, "", []byte("k"))
		require.ErrorIs(t, err, ErrStoreClosed)
	})
}

// TestSnapshot_OutstandingAtCloseLogsAndIsSafe pins that closing with an
// unreleased snapshot is not a crash: it logs the leak and, crucially, a later
// ReleaseSnapshot skips the C release against the freed DB.
func TestSnapshot_OutstandingAtCloseLogsAndIsSafe(t *testing.T) {
	var buf bytes.Buffer
	s, err := New(Config{Path: t.TempDir(), Logger: newTestLogger(&buf)})
	require.NoError(t, err)

	snap, err := s.NewSnapshot()
	require.NoError(t, err)

	require.NoError(t, s.Close())
	assert.Contains(t, buf.String(), "unreleased snapshot")

	// Release after teardown must be a safe no-op.
	s.ReleaseSnapshot(snap)
}

func TestStore_CloseIfIdle_IdleTearsDown(t *testing.T) {
	s, err := New(Config{Path: t.TempDir(), Logger: silentLogger()})
	require.NoError(t, err)
	require.NoError(t, s.Put("", []byte("k"), []byte("v")))

	ok, err := s.CloseIfIdle()
	require.NoError(t, err)
	assert.True(t, ok, "an idle store closes immediately")
	assert.True(t, s.IsClosed())
	assert.ErrorIs(t, s.Put("", []byte("k"), []byte("v")), ErrStoreClosed)

	// Retry after teardown, and a plain Close, are both safe no-ops.
	ok, err = s.CloseIfIdle()
	require.NoError(t, err)
	assert.True(t, ok)
	require.NoError(t, s.Close())
}

// TestStore_CloseIfIdle_BusyReportsFailureWithoutBlocking pins the drain-barrier
// contract: with an in-flight op holding the read-lock, CloseIfIdle returns
// (false, nil) at once, leaves the store poisoned, and a later retry (after the
// op drains) completes the teardown.
func TestStore_CloseIfIdle_BusyReportsFailureWithoutBlocking(t *testing.T) {
	s, err := New(Config{Path: t.TempDir(), Logger: silentLogger()})
	require.NoError(t, err)
	for _, k := range []string{"k1", "k2", "k3"} {
		require.NoError(t, s.Put("", []byte(k), []byte("v")))
	}

	iterParked := make(chan struct{})
	releaseIter := make(chan struct{})
	iterDone := make(chan struct{})

	go func() {
		defer close(iterDone)
		first := true
		for _, err := range s.Iterate("", []byte("k")) {
			// The parked iterator entered before the poison, so it keeps
			// yielding: an in-flight op does not re-check the closed flag.
			assert.NoError(t, err)
			if first {
				close(iterParked)
				<-releaseIter
				first = false
			}
		}
	}()

	<-iterParked

	// The iterator holds RLock, so CloseIfIdle cannot acquire the write lock.
	start := time.Now()
	ok, err := s.CloseIfIdle()
	require.NoError(t, err)
	assert.False(t, ok, "busy store reports failure")
	assert.Less(t, time.Since(start), 50*time.Millisecond, "CloseIfIdle must not block")

	// The store is poisoned even though teardown did not run: new ops fail.
	assert.True(t, s.IsClosed())
	assert.ErrorIs(t, s.Put("", []byte("x"), []byte("v")), ErrStoreClosed)

	// Let the parked iterator drain, then a retry completes the teardown.
	close(releaseIter)
	<-iterDone

	ok, err = s.CloseIfIdle()
	require.NoError(t, err)
	assert.True(t, ok, "once the straggler drains, the retry tears down")
}

// TestStore_CloseIfIdle_ConcurrentWithClose pins that CloseIfIdle and Close race
// cleanly: teardown runs exactly once and the store ends closed regardless of order.
func TestStore_CloseIfIdle_ConcurrentWithClose(t *testing.T) {
	s, err := New(Config{Path: t.TempDir(), Logger: silentLogger()})
	require.NoError(t, err)
	require.NoError(t, s.Put("", []byte("k"), []byte("v")))

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _, _ = s.CloseIfIdle() }()
	go func() { defer wg.Done(); _ = s.Close() }()
	wg.Wait()

	assert.True(t, s.IsClosed())
	_, _, err = s.Get("", []byte("k"))
	assert.ErrorIs(t, err, ErrStoreClosed)
}
