package registry

import (
	"container/list"
	"sync"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
)

// readerCache is a per-kind LRU of open cold readers keyed by chunk. The
// Registry runs one for ledger readers and one for event readers, because
// their resource costs differ and heavy event traffic must not evict ledger
// readers other query paths need.
//
// The cache never closes a reader inline: eviction and retirement hand the
// reader to retire (which schedules Close on the reaper), so a query that
// acquired the reader from an older View can finish using it — the same
// grace-period rule every other serving resource follows.
//
// open runs under the cache lock. Both cold reader constructors are lazy (no
// synchronous I/O), so the lock also deduplicates concurrent misses for one
// chunk without holding up the cache for disk time.
type readerCache[R any] struct {
	mu     sync.Mutex
	cap    int
	order  *list.List // of *cacheSlot[R]; front = most recently used
	byID   map[chunk.ID]*list.Element
	open   func(chunk.ID) (R, error)
	retire func(R)
}

type cacheSlot[R any] struct {
	id     chunk.ID
	reader R
}

func newReaderCache[R any](capacity int, open func(chunk.ID) (R, error), retire func(R)) *readerCache[R] {
	if capacity < 1 {
		capacity = 1
	}
	return &readerCache[R]{
		cap:    capacity,
		order:  list.New(),
		byID:   map[chunk.ID]*list.Element{},
		open:   open,
		retire: retire,
	}
}

// acquire returns chunk id's cached reader, opening and inserting it on miss.
// When the insert overflows the capacity, the least-recently-used reader is
// evicted and retired (closed via the reaper, never inline).
func (c *readerCache[R]) acquire(id chunk.ID) (R, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.byID[id]; ok {
		c.order.MoveToFront(el)
		return el.Value.(*cacheSlot[R]).reader, nil
	}
	reader, err := c.open(id)
	if err != nil {
		var zero R
		return zero, err
	}
	c.byID[id] = c.order.PushFront(&cacheSlot[R]{id: id, reader: reader})
	if c.order.Len() > c.cap {
		oldest := c.order.Back()
		c.evictLocked(oldest.Value.(*cacheSlot[R]).id, oldest)
	}
	return reader, nil
}

// drop retires chunk id's cached reader, if present. Called when the chunk is
// unpublished so an unlinked file is never kept open by a lingering cache
// entry. (A query holding an older View can still re-open the chunk after
// this runs; that re-inserted reader is bounded by the LRU and closes on
// eviction or teardown.)
func (c *readerCache[R]) drop(id chunk.ID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.byID[id]; ok {
		c.evictLocked(id, el)
	}
}

// dropBelow retires every cached reader for chunks below floor.
func (c *readerCache[R]) dropBelow(floor chunk.ID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, el := range c.byID {
		if id < floor {
			c.evictLocked(id, el)
		}
	}
}

// dropAll retires every cached reader. Teardown path.
func (c *readerCache[R]) dropAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, el := range c.byID {
		c.evictLocked(id, el)
	}
}

func (c *readerCache[R]) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}

// evictLocked removes one entry and hands its reader to retire. Callers hold
// c.mu. retire must not block: the registry wires it to reaper.Schedule.
func (c *readerCache[R]) evictLocked(id chunk.ID, el *list.Element) {
	c.order.Remove(el)
	delete(c.byID, id)
	c.retire(el.Value.(*cacheSlot[R]).reader)
}
