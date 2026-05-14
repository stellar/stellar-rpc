package rocksdb

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatch_SingleCF_100PutsAtomic(t *testing.T) {
	s := openTestStore(t, nil)

	err := s.Batch(func(b *BatchWriter) error {
		for i := range 100 {
			b.Put("default", fmt.Appendf(nil, "k%03d", i), fmt.Appendf(nil, "v%03d", i))
		}
		return nil
	})
	require.NoError(t, err)

	for i := range 100 {
		val, found, err := s.Get("default", fmt.Appendf(nil, "k%03d", i))
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, fmt.Appendf(nil, "v%03d", i), val)
	}
}

func TestBatch_MultiCF_WritesIsolatedAndAtomic(t *testing.T) {
	cfNames := txhashCFNames()
	s := openTestStore(t, cfNames)

	const perCF = 6
	err := s.Batch(func(b *BatchWriter) error {
		for cfi, cf := range cfNames {
			for j := range perCF {
				b.Put(cf, fmt.Appendf(nil, "k%02d-%02d", cfi, j), fmt.Appendf(nil, "v%02d-%02d", cfi, j))
			}
		}
		return nil
	})
	require.NoError(t, err)

	for cfi, cf := range cfNames {
		for j := range perCF {
			val, found, err := s.Get(cf, fmt.Appendf(nil, "k%02d-%02d", cfi, j))
			require.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, fmt.Appendf(nil, "v%02d-%02d", cfi, j), val)
		}
		// Does NOT contain other CFs' keys.
		for otherCFI, otherCF := range cfNames {
			if otherCFI == cfi {
				continue
			}
			for j := range perCF {
				_, found, err := s.Get(cf, fmt.Appendf(nil, "k%02d-%02d", otherCFI, j))
				require.NoError(t, err)
				assert.False(t, found, "%s leaked %s's key", cf, otherCF)
			}
		}
	}
}

func TestBatch_MidCallbackErrorRollsBack(t *testing.T) {
	s := openTestStore(t, nil)

	sentinel := errors.New("simulated mid-callback failure")
	err := s.Batch(func(b *BatchWriter) error {
		for i := range 10 {
			b.Put("default", fmt.Appendf(nil, "k%d", i), []byte("v"))
		}
		return sentinel
	})
	require.ErrorIs(t, err, sentinel)

	for i := range 10 {
		_, found, err := s.Get("default", fmt.Appendf(nil, "k%d", i))
		require.NoError(t, err)
		assert.False(t, found)
	}
}

func TestBatch_EmptyCallback_NoOp(t *testing.T) {
	s := openTestStore(t, nil)

	called := false
	err := s.Batch(func(*BatchWriter) error {
		called = true
		return nil
	})
	require.NoError(t, err)
	assert.True(t, called)
}

func TestBatch_PutThenDeleteSameKey_DeletionWins(t *testing.T) {
	s := openTestStore(t, nil)

	err := s.Batch(func(b *BatchWriter) error {
		b.Put("default", []byte("k"), []byte("v"))
		b.Delete("default", []byte("k"))
		return nil
	})
	require.NoError(t, err)

	_, found, err := s.Get("default", []byte("k"))
	require.NoError(t, err)
	assert.False(t, found)
}

func TestBatch_ConcurrentBatchesDoNotInterfere(t *testing.T) {
	cfNames := txhashCFNames()
	s := openTestStore(t, cfNames)

	const perGoroutine = 50
	var wg sync.WaitGroup
	commitErr := make([]error, 2)

	wg.Go(func() {
		commitErr[0] = s.Batch(func(b *BatchWriter) error {
			for i := range perGoroutine {
				b.Put("cf-0", fmt.Appendf(nil, "a%03d", i), []byte("a"))
			}
			return nil
		})
	})
	wg.Go(func() {
		commitErr[1] = s.Batch(func(b *BatchWriter) error {
			for i := range perGoroutine {
				b.Put("cf-f", fmt.Appendf(nil, "b%03d", i), []byte("b"))
			}
			return nil
		})
	})
	wg.Wait()

	require.NoError(t, commitErr[0])
	require.NoError(t, commitErr[1])

	for i := range perGoroutine {
		_, foundA, err := s.Get("cf-0", fmt.Appendf(nil, "a%03d", i))
		require.NoError(t, err)
		assert.True(t, foundA)
		_, foundB, err := s.Get("cf-f", fmt.Appendf(nil, "b%03d", i))
		require.NoError(t, err)
		assert.True(t, foundB)
	}
}

func TestBatch_BatchWriterNotRetainedAfterCallback(t *testing.T) {
	s := openTestStore(t, nil)

	var captured *BatchWriter
	err := s.Batch(func(b *BatchWriter) error {
		captured = b
		return nil
	})
	require.NoError(t, err)

	captured.Put("default", []byte("retained"), []byte("v"))
	captured.Delete("default", []byte("retained"))

	_, found, err := s.Get("default", []byte("retained"))
	require.NoError(t, err)
	assert.False(t, found)
}

func TestBatch_ErrorPaths(t *testing.T) {
	t.Run("never-opened store returns ErrStoreNotOpened", func(t *testing.T) {
		s, err := New(Config{Path: t.TempDir(), Logger: silentLogger()})
		require.NoError(t, err)
		err = s.Batch(func(*BatchWriter) error { return nil })
		assert.ErrorIs(t, err, ErrStoreNotOpened)
	})

	t.Run("unknown CF inside callback surfaces ErrCFNotFound", func(t *testing.T) {
		s := openTestStore(t, nil)
		err := s.Batch(func(b *BatchWriter) error {
			b.Put("not-configured", []byte("k"), []byte("v"))
			return nil
		})
		require.ErrorIs(t, err, ErrCFNotFound)

		// And the bad CF write didn't leak into the default CF.
		_, found, err := s.Get("default", []byte("k"))
		require.NoError(t, err)
		assert.False(t, found)
	})

	t.Run("delete-only callback commits cleanly", func(t *testing.T) {
		s := openTestStore(t, nil)
		require.NoError(t, s.Put("default", []byte("k1"), []byte("v")))
		require.NoError(t, s.Put("default", []byte("k2"), []byte("v")))

		err := s.Batch(func(b *BatchWriter) error {
			b.Delete("default", []byte("k1"))
			b.Delete("default", []byte("k2"))
			return nil
		})
		require.NoError(t, err)

		_, found1, _ := s.Get("default", []byte("k1"))
		_, found2, _ := s.Get("default", []byte("k2"))
		assert.False(t, found1)
		assert.False(t, found2)
	})
}

func TestBatch_ConcurrentSnapshotReaderSeesOneGenerationTag(t *testing.T) {
	s := openTestStore(t, nil)

	const keysPerBatch = 32
	const generations = 50

	require.NoError(t, s.Batch(func(b *BatchWriter) error {
		for i := range keysPerBatch {
			b.Put("default", fmt.Appendf(nil, "k%02d", i), []byte("gen-init"))
		}
		return nil
	}))

	var stop atomic.Bool
	var sawTorn atomic.Bool
	var wg sync.WaitGroup

	wg.Go(func() {
		for !stop.Load() {
			tags := map[string]struct{}{}
			for e, err := range s.Iterate("default", []byte("k")) {
				if err != nil {
					assert.NoError(t, err)
					return
				}
				tags[string(e.Value)] = struct{}{}
			}
			if len(tags) > 1 {
				sawTorn.Store(true)
				return
			}
		}
	})

	wg.Go(func() {
		for g := range generations {
			tag := fmt.Appendf(nil, "gen-%03d", g)
			err := s.Batch(func(b *BatchWriter) error {
				for i := range keysPerBatch {
					b.Put("default", fmt.Appendf(nil, "k%02d", i), tag)
				}
				return nil
			})
			require.NoError(t, err)
		}
		stop.Store(true)
	})

	wg.Wait()
	assert.False(t, sawTorn.Load(), "iterator-snapshot reader observed mixed tags — batch atomicity broken")
}
