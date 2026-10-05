package methods

import (
	"sync"
	"sync/atomic"
)

// latestMemo memoizes one value per latest ledger sequence; a new ledger moves the key.
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
	e := m.entry.Load() // stable while mu is held: get is the only writer
	if e != nil && e.seq == seq {
		return e.val, nil // computed while waiting for the lock
	}
	val, err := compute()
	if err != nil {
		var zero T
		return zero, err
	}
	if e == nil || seq > e.seq { // an older read view must not evict a newer ledger's value
		m.entry.Store(&latestEntry[T]{seq: seq, val: val})
	}
	return val, nil
}
