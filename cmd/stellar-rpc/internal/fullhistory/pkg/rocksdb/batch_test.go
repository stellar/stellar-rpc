package rocksdb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 100 puts in one batch against one CF — all visible after commit.
func TestBatch_SingleCF_100PutsAtomic(t *testing.T) {
	s := openTestStore(t, nil)

	err := s.Batch(context.Background(), func(b BatchWriter) error {
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

// Writes spread across all 16 txhash CFs commit atomically; every CF
// reflects exactly its writes; no cross-CF leakage.
func TestBatch_MultiCF_WritesIsolatedAndAtomic(t *testing.T) {
	cfNames := txhashCFNames()
	s := openTestStore(t, cfNames)

	const perCF = 6
	err := s.Batch(context.Background(), func(b BatchWriter) error {
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

// Mid-callback error → ZERO writes visible (true rollback). Process_chunk's
// three-flag commit depends on this: any writer fsync failing must keep
// all three meta-store flags absent so the chunk re-runs cleanly.
func TestBatch_MidCallbackErrorRollsBack(t *testing.T) {
	s := openTestStore(t, nil)

	sentinel := errors.New("simulated mid-callback failure")
	err := s.Batch(context.Background(), func(b BatchWriter) error {
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

// Empty batch → no-op, no error.
func TestBatch_EmptyCallback_NoOp(t *testing.T) {
	s := openTestStore(t, nil)

	called := false
	err := s.Batch(context.Background(), func(BatchWriter) error {
		called = true
		return nil
	})
	require.NoError(t, err)
	assert.True(t, called)
}

// Put + Delete on the same key in one batch — final state shows the
// deletion (RocksDB applies in queue order; trailing op wins).
func TestBatch_PutThenDeleteSameKey_DeletionWins(t *testing.T) {
	s := openTestStore(t, nil)

	err := s.Batch(context.Background(), func(b BatchWriter) error {
		b.Put("default", []byte("k"), []byte("v"))
		b.Delete("default", []byte("k"))
		return nil
	})
	require.NoError(t, err)

	_, found, err := s.Get("default", []byte("k"))
	require.NoError(t, err)
	assert.False(t, found)
}

// Concurrent batches against different CFs commit independently; no
// interference.
func TestBatch_ConcurrentBatchesDoNotInterfere(t *testing.T) {
	cfNames := txhashCFNames()
	s := openTestStore(t, cfNames)

	const perGoroutine = 50
	var wg sync.WaitGroup
	commitErr := make([]error, 2)

	wg.Go(func() {
		commitErr[0] = s.Batch(context.Background(), func(b BatchWriter) error {
			for i := range perGoroutine {
				b.Put("cf-0", fmt.Appendf(nil, "a%03d", i), []byte("a"))
			}
			return nil
		})
	})
	wg.Go(func() {
		commitErr[1] = s.Batch(context.Background(), func(b BatchWriter) error {
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

// BatchWriter is invalid after the callback returns; using a captured
// reference is silently inert. Protects against a careless caller
// stashing the BatchWriter on a struct field.
func TestBatch_BatchWriterNotRetainedAfterCallback(t *testing.T) {
	s := openTestStore(t, nil)

	var captured BatchWriter
	err := s.Batch(context.Background(), func(b BatchWriter) error {
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

// Concurrent reader using an iterator (point-in-time snapshot) never
// observes a half-committed batch. Per-key Get calls each take their
// own snapshot, so Iterate is the right shape to test atomicity.
func TestBatch_ConcurrentSnapshotReaderSeesOneGenerationTag(t *testing.T) {
	s := openTestStore(t, nil)

	const keysPerBatch = 32
	const generations = 50

	require.NoError(t, s.Batch(context.Background(), func(b BatchWriter) error {
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
			it := s.Iterate("default", []byte("k"))
			tags := map[string]struct{}{}
			for it.Next() {
				tags[string(it.Value())] = struct{}{}
			}
			_ = it.Close()
			if len(tags) > 1 {
				sawTorn.Store(true)
				return
			}
		}
	})

	wg.Go(func() {
		for g := range generations {
			tag := fmt.Appendf(nil, "gen-%03d", g)
			err := s.Batch(context.Background(), func(b BatchWriter) error {
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
