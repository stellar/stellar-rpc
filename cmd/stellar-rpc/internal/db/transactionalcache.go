package db

type transactionalCache struct {
	entries map[string]string
}

func newTransactionalCache() transactionalCache {
	return transactionalCache{entries: map[string]string{}}
}

func (c transactionalCache) newReadTx() transactionalCacheReadTx {
	entries := make(map[string]*string, len(c.entries))
	for k, v := range c.entries {
		localV := v
		entries[k] = &localV
	}
	return transactionalCacheReadTx{entries: entries}
}

func (c transactionalCache) newWriteTx(estimatedWriteCount int) transactionalCacheWriteTx {
	return transactionalCacheWriteTx{
		pendingUpdates: make(map[string]*string, estimatedWriteCount),
		parent:         &c,
	}
}

// transactionalCacheReadTx represents a read transaction in the cache.
// nil values in entries indicate the key is not present in the underlying storage.
type transactionalCacheReadTx struct {
	entries map[string]*string
}

// get retrieves a value for the given key.
// It returns (nil, false) if the key is not present in the cache.
// It returns (nil, true) if the key is present but has a nil value (indicating deletion).
func (r transactionalCacheReadTx) get(key string) (*string, bool) {
	val, ok := r.entries[key]
	return val, ok
}

// upsert inserts or updates a key-value pair in the cache.
// If value is nil, it indicates the key should be marked as deleted.
func (r transactionalCacheReadTx) upsert(key string, value *string) {
	r.entries[key] = value
}

type transactionalCacheWriteTx struct {
	// nil indicates deletion
	pendingUpdates map[string]*string
	parent         *transactionalCache
}

func (w transactionalCacheWriteTx) upsert(key, val string) {
	w.pendingUpdates[key] = &val
}

func (w transactionalCacheWriteTx) delete(key string) {
	w.pendingUpdates[key] = nil
}

func (w transactionalCacheWriteTx) commit() {
	for key, newValue := range w.pendingUpdates {
		if newValue == nil {
			delete(w.parent.entries, key)
		} else {
			w.parent.entries[key] = *newValue
		}
	}
}
