package streaming

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLockRoots_AcquiresAndReleases(t *testing.T) {
	root := t.TempDir()
	locks, err := LockRoots(root)
	require.NoError(t, err)
	require.NotNil(t, locks)

	// The lock file was created.
	_, statErr := os.Stat(filepath.Join(root, lockFileName))
	require.NoError(t, statErr)

	// After release a second holder can take it.
	locks.Release()
	again, err := LockRoots(root)
	require.NoError(t, err)
	again.Release()
}

func TestLockRoots_SecondHolderFailsFast(t *testing.T) {
	root := t.TempDir()
	first, err := LockRoots(root)
	require.NoError(t, err)
	defer first.Release()

	// A second holder on the SAME root is rejected immediately (non-blocking).
	second, err := LockRoots(root)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrRootLocked)
	assert.Contains(t, err.Error(), root)
	assert.Nil(t, second, "no partial RootLocks handed back on the rejected attempt")
}

func TestLockRoots_SharedRootAmongManyFailsFast(t *testing.T) {
	// Two daemons with different meta stores but a SHARED hot/immutable root:
	// the shared root's lock is what stops them.
	shared := t.TempDir()
	meta1 := t.TempDir()
	meta2 := t.TempDir()

	first, err := LockRoots(meta1, shared)
	require.NoError(t, err)
	defer first.Release()

	// Daemon 2: distinct meta store, same shared artifact tree -> rejected, and
	// the meta2 lock it grabbed first must be released on the failure.
	_, err = LockRoots(meta2, shared)
	require.ErrorIs(t, err, ErrRootLocked)

	// Proof meta2 was released on the partial failure: a fresh holder gets it.
	m2, err := LockRoots(meta2)
	require.NoError(t, err)
	m2.Release()
}

func TestLockRoots_DeDuplicatesRepeatedRoot(t *testing.T) {
	root := t.TempDir()
	// The same root twice must not self-deadlock (flock is per-fd, but a second
	// fd on the same file from the same process would still EWOULDBLOCK).
	locks, err := LockRoots(root, root)
	require.NoError(t, err)
	defer locks.Release()
	assert.Len(t, locks.files, 1, "the repeated root is locked once")
}

func TestLockRoots_CreatesMissingRoot(t *testing.T) {
	parent := t.TempDir()
	missing := filepath.Join(parent, "not", "yet", "there")
	locks, err := LockRoots(missing)
	require.NoError(t, err)
	defer locks.Release()
	info, err := os.Stat(missing)
	require.NoError(t, err)
	assert.True(t, info.IsDir())
}

func TestLockRoots_SkipsEmptyRoot(t *testing.T) {
	locks, err := LockRoots("")
	require.NoError(t, err)
	defer locks.Release()
	assert.Empty(t, locks.files)
}

func TestRootLocks_ReleaseNilSafe(t *testing.T) {
	var l *RootLocks
	assert.NotPanics(t, l.Release)
}
