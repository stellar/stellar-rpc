package methods

import (
	"sync"
	"sync/atomic"
)

// latestMemo memoizes one value derived from the latest ledger, keyed by its
// sequence; a newly closed ledger invalidates it by moving the key.
type latestMemo[T any] struct {
	entry atomic.Pointer[latestEntry[T]]
	mu    sync.Mutex // serializes misses so a new ledger computes once, not once per waiting request
}

// latestEntry is the memoized value and the ledger it was computed for.
type latestEntry[T any] struct {
	seq uint32
	val T
}

// get returns the value memoized for seq, computing it once on a miss. Errors are not memoized.
func (m *latestMemo[T]) get(seq uint32, compute func() (T, error)) (T, error) {
	if e := m.entry.Load(); e != nil && e.seq == seq {
		return e.val, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if e := m.entry.Load(); e != nil && e.seq == seq { // computed while waiting for the lock
		return e.val, nil
	}
	val, err := compute()
	if err != nil {
		var zero T
		return zero, err
	}
	// A request on an older read view must not evict a newer ledger's value.
	if e := m.entry.Load(); e == nil || seq >= e.seq {
		m.entry.Store(&latestEntry[T]{seq: seq, val: val})
	}
	return val, nil
}
