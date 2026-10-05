package rocksdb

import (
	"bytes"
	"fmt"
	"iter"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/linxGnu/grocksdb"
	"github.com/stretchr/testify/require"
)

// pairs yields key/value pairs given as alternating strings.
func pairs(kv ...string) iter.Seq2[[]byte, []byte] {
	return func(yield func(key, value []byte) bool) {
		for i := 0; i < len(kv); i += 2 {
			if !yield([]byte(kv[i]), []byte(kv[i+1])) {
				return
			}
		}
	}
}

// numbered yields n ascending keys, each with value.
func numbered(n int, value []byte) iter.Seq2[[]byte, []byte] {
	return func(yield func(key, value []byte) bool) {
		for i := range n {
			if !yield(fmt.Appendf(nil, "key-%08d", i), value) {
				return
			}
		}
	}
}

func requireValue(t *testing.T, s *Store, cf, key, want string) {
	t.Helper()
	got, found, err := s.Get(cf, []byte(key))
	require.NoError(t, err)
	require.True(t, found, "key %q", key)
	require.Equal(t, want, string(got))
}

func loadFiles(t *testing.T, s *Store) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(s.cfg.Path, loadDirName, "*"))
	require.NoError(t, err)
	return files
}

func uintProperty(t *testing.T, s *Store, cf, name string) uint64 {
	t.Helper()
	n, err := strconv.ParseUint(s.db.GetPropertyCF(name, s.cfHandles[cf]), 10, 64)
	require.NoError(t, err)
	return n
}

func TestStore_LoadSorted_RoundTrip(t *testing.T) {
	const loaded, other = "loaded", "other"
	cfg := Config{Path: t.TempDir(), ColumnFamilies: []string{loaded, other}, Logger: silentLogger()}
	s, err := New(cfg)
	require.NoError(t, err)

	require.NoError(t, s.LoadSorted(loaded, pairs("a1", "one", "a2", "two")))
	// A batch to another CF sits in the memtable while a second, disjoint
	// file is loaded.
	require.NoError(t, s.Batch(func(b *BatchWriter) error {
		b.Put(other, []byte("k"), []byte("batched"))
		return nil
	}))
	require.NoError(t, s.LoadSorted(loaded, pairs("b1", "three")))
	require.Error(t, s.LoadSorted(loaded, pairs("a2", "x")), "a key a loaded file holds")
	require.Empty(t, loadFiles(t, s))

	check := func(s *Store) {
		requireValue(t, s, loaded, "a1", "one")
		requireValue(t, s, loaded, "a2", "two")
		requireValue(t, s, loaded, "b1", "three")
		requireValue(t, s, other, "k", "batched")
		_, found, err := s.Get(other, []byte("a1"))
		require.NoError(t, err)
		require.False(t, found, "a loaded key stays in its column family")
		last, ok, err := s.LastKey(loaded)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, "b1", string(last))
	}
	check(s)

	require.NoError(t, s.Close())
	reopened, err := New(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	check(reopened)
}

func TestStore_LoadSorted_FailedLoadLeavesNothing(t *testing.T) {
	s := openTestStore(t, nil)
	require.NoError(t, s.Put("", []byte("kept"), []byte("v")))

	require.Error(t, s.LoadSorted("", pairs("b", "1", "a", "2")), "descending keys")
	require.Error(t, s.LoadSorted("", pairs()), "no entries")
	require.Error(t, s.LoadSorted("", pairs("kept", "x")), "a key the column family holds")
	require.ErrorIs(t, s.LoadSorted("not-configured", pairs("a", "1")), ErrCFNotFound)

	require.Empty(t, loadFiles(t, s))
	for _, key := range []string{"a", "b"} {
		_, found, err := s.Get("", []byte(key))
		require.NoError(t, err)
		require.False(t, found, "key %q", key)
	}
	requireValue(t, s, "", "kept", "v")
}

func TestStore_LoadSorted_ClosedAndReadOnlyStores(t *testing.T) {
	path := t.TempDir()
	s, err := New(Config{Path: path, Logger: silentLogger()})
	require.NoError(t, err)
	require.NoError(t, s.Close())
	require.ErrorIs(t, s.LoadSorted("", pairs("a", "1")), ErrStoreClosed)

	readOnly, err := New(Config{Path: path, Logger: silentLogger(), ReadOnly: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = readOnly.Close() })
	require.Error(t, readOnly.LoadSorted("", pairs("a", "1")))
	require.NoDirExists(t, filepath.Join(path, loadDirName))
}

// Close waits for a load in flight, so the load is durable or never started.
func TestStore_LoadSorted_CloseWaitsForLoad(t *testing.T) {
	path := t.TempDir()
	s, err := New(Config{Path: path, Logger: silentLogger()})
	require.NoError(t, err)

	loading, release := make(chan struct{}), make(chan struct{})
	loadErr := make(chan error, 1)
	go func() {
		loadErr <- s.LoadSorted("", func(yield func(key, value []byte) bool) {
			close(loading)
			<-release
			yield([]byte("a"), []byte("1"))
		})
	}()
	<-loading
	closed := make(chan struct{})
	go func() {
		_ = s.Close()
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatal("Close returned while a load was in flight")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-loadErr)
	<-closed

	reopened, err := New(Config{Path: path, Logger: silentLogger(), MustExist: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	requireValue(t, reopened, "", "a", "1")
}

// The file is written with the column family's own options: a compressed
// column family gets a compressed file.
func TestStore_LoadSorted_UsesTheColumnFamilyOptions(t *testing.T) {
	const raw, zstd = "raw", "zstd"
	s, err := New(Config{
		Path:           t.TempDir(),
		ColumnFamilies: []string{raw, zstd},
		Logger:         silentLogger(),
		PerCFOptions:   map[string]CFOptions{zstd: {Compression: grocksdb.ZSTDCompression}},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	value := bytes.Repeat([]byte("compressible "), 100)
	for _, cf := range []string{raw, zstd} {
		require.NoError(t, s.LoadSorted(cf, numbered(1000, value)))
	}
	const size = "rocksdb.total-sst-files-size"
	require.Less(t, 4*uintProperty(t, s, zstd, size), uintProperty(t, s, raw, size))
}

// With CacheIndexAndFilterBlocks, an open file's index and filter blocks
// live in the block cache, not on the heap.
func TestStore_CacheIndexAndFilterBlocks_KeepsThemOffTheHeap(t *testing.T) {
	const cached, plain = "cached", "plain"
	s, err := New(Config{
		Path:           t.TempDir(),
		ColumnFamilies: []string{cached, plain},
		Logger:         silentLogger(),
		Tuning:         Tuning{BlockCacheMB: 4},
		PerCFOptions: map[string]CFOptions{
			cached: {BloomFilterBitsPerKey: 10, CacheIndexAndFilterBlocks: true},
			plain:  {BloomFilterBitsPerKey: 10},
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	for _, cf := range []string{cached, plain} {
		require.NoError(t, s.LoadSorted(cf, numbered(20_000, []byte("v"))))
		requireValue(t, s, cf, "key-00000007", "v")
	}
	const heap = "rocksdb.estimate-table-readers-mem"
	require.Less(t, 4*uintProperty(t, s, cached, heap), uintProperty(t, s, plain, heap))
}

// Files a stopped process was still building are removed by the next open
// that takes the DB lock, and by no other open.
func TestNew_RemovesUnfinishedLoadFiles(t *testing.T) {
	path := t.TempDir()
	cfg := Config{Path: path, Logger: silentLogger()}
	s, err := New(cfg)
	require.NoError(t, err)

	// An open that fails on the lock leaves a live store's load alone.
	require.NoError(t, s.LoadSorted("", func(yield func(key, value []byte) bool) {
		_, err := New(cfg)
		require.Error(t, err)
		yield([]byte("a"), []byte("1"))
	}))
	require.NoError(t, s.Close())

	orphan := filepath.Join(path, loadDirName, "orphan.sst")
	require.NoError(t, os.WriteFile(orphan, []byte("half a file"), 0o600))
	readOnly, err := New(Config{Path: path, Logger: silentLogger(), ReadOnly: true})
	require.NoError(t, err)
	require.NoError(t, readOnly.Close())
	require.FileExists(t, orphan)

	reopened, err := New(Config{Path: path, Logger: silentLogger(), MustExist: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	require.NoFileExists(t, orphan)
	requireValue(t, reopened, "", "a", "1")
}
