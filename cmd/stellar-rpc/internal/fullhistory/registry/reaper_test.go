package registry

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReaper_DelaysDestructionUntilGrace(t *testing.T) {
	const grace = 400 * time.Millisecond
	p := NewReaper(grace, silentLogger())
	t.Cleanup(p.Close)

	start := time.Now()
	var ranAt atomic.Int64
	p.Schedule(func() error {
		ranAt.Store(int64(time.Since(start)))
		return nil
	})
	require.Zero(t, ranAt.Load(), "destroy must not run inline from Schedule")

	require.Eventually(t, func() bool { return ranAt.Load() != 0 }, 10*time.Second, 5*time.Millisecond)
	require.GreaterOrEqual(t, time.Duration(ranAt.Load()), grace,
		"destroy ran before the grace period elapsed")
}

func TestReaper_RunsInScheduleOrder(t *testing.T) {
	p := NewReaper(20*time.Millisecond, silentLogger())
	t.Cleanup(p.Close)

	var mu sync.Mutex
	var order []int
	for i := range 5 {
		p.Schedule(func() error {
			mu.Lock()
			defer mu.Unlock()
			order = append(order, i)
			return nil
		})
	}
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(order) == 5
	}, 10*time.Second, 5*time.Millisecond)
	require.Equal(t, []int{0, 1, 2, 3, 4}, order)
}

func TestReaper_CloseRunsPendingImmediately(t *testing.T) {
	p := NewReaper(time.Hour, silentLogger())

	var ran atomic.Bool
	p.Schedule(func() error {
		ran.Store(true)
		return nil
	})
	require.False(t, ran.Load())

	p.Close()
	require.True(t, ran.Load(), "Close must run still-queued destroys without waiting out the grace")

	var late atomic.Bool
	p.Schedule(func() error {
		late.Store(true)
		return nil
	})
	require.True(t, late.Load(), "a post-Close Schedule must run its destroy inline")

	p.Close() // idempotent
}

func TestReaper_EveryDestroyRunsExactlyOnceAcrossClose(t *testing.T) {
	p := NewReaper(0, silentLogger())

	const goroutines, perGoroutine = 8, 100
	var count atomic.Int32
	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perGoroutine {
				p.Schedule(func() error {
					count.Add(1)
					return nil
				})
			}
		}()
	}
	wg.Wait()
	p.Close()
	require.Equal(t, int32(goroutines*perGoroutine), count.Load())
}
