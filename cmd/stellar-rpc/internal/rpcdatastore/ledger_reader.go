package rpcdatastore

import (
	"context"
	"fmt"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/support/datastore"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// LedgerBackendFactory creates a new ledger backend.
type LedgerBackendFactory interface {
	NewBufferedBackend(
		config ledgerbackend.BufferedStorageBackendConfig,
		store datastore.DataStore,
		schema datastore.DataStoreSchema,
	) (ledgerbackend.LedgerBackend, error)
}

type bufferedBackendFactory struct{}

// NewBufferedBackend creates a buffered storage backend using the given config and datastore.
func (f *bufferedBackendFactory) NewBufferedBackend(
	config ledgerbackend.BufferedStorageBackendConfig,
	store datastore.DataStore,
	schema datastore.DataStoreSchema,
) (ledgerbackend.LedgerBackend, error) {
	return ledgerbackend.NewBufferedStorageBackend(config, store, schema)
}

const defaultLedgerCacheSize = 1000

// LedgerReader provides access to historical ledger data
// stored in a remote object store (e.g., S3 or GCS) via buffered storage backend.
type LedgerReader interface {
	GetLedgers(ctx context.Context, start, end uint32) ([]xdr.LedgerCloseMeta, error)
	GetAvailableLedgerRange(ctx context.Context) (protocol.LedgerSeqRange, error)
	GetLedgerCached(ctx context.Context, u uint32) (xdr.LedgerCloseMeta, error)
}

// ledgerReader is the default implementation of LedgerReader.
type ledgerReader struct {
	storageBackendConfig ledgerbackend.BufferedStorageBackendConfig
	dataStore            datastore.DataStore
	schema               datastore.DataStoreSchema
	ledgerBackendFactory LedgerBackendFactory
	// ledgerCache caches ledgers to avoid repeated remote fetches
	// when callers logically access one ledger at a time.
	cacheMu     sync.Mutex
	ledgerCache *lru.Cache[uint32, xdr.LedgerCloseMeta]

	// range cache with TTL
	rangeMu       sync.RWMutex
	cachedRange   protocol.LedgerSeqRange
	cachedRangeAt time.Time
	rangeTTL      time.Duration
}

// NewLedgerReader constructs a new LedgerReader using the provided
// buffered storage backend configuration and datastore configuration.
func NewLedgerReader(
	storageBackendConfig ledgerbackend.BufferedStorageBackendConfig,
	dataStore datastore.DataStore,
	schema datastore.DataStoreSchema,
) LedgerReader {
	cache, _ := lru.New[uint32, xdr.LedgerCloseMeta](defaultLedgerCacheSize)
	return &ledgerReader{
		storageBackendConfig: storageBackendConfig,
		dataStore:            dataStore,
		schema:               schema,
		ledgerBackendFactory: &bufferedBackendFactory{},
		ledgerCache:          cache,
		rangeTTL:             time.Minute * 10, // refresh at most once per minute
	}
}

// GetLedgers retrieves a contiguous batch of ledgers in the range [start, end] (inclusive)
// from the configured datastore using a buffered storage backend.
// Returns an error if any ledger in the specified range is unavailable.
func (r *ledgerReader) GetLedgers(ctx context.Context, start, end uint32) ([]xdr.LedgerCloseMeta, error) {
	bufferedBackend, err := r.ledgerBackendFactory.NewBufferedBackend(r.storageBackendConfig, r.dataStore, r.schema)
	if err != nil {
		return nil, fmt.Errorf("failed to create buffered storage backend: %w", err)
	}
	defer func(bufferedBackend ledgerbackend.LedgerBackend) {
		_ = bufferedBackend.Close()
	}(bufferedBackend)

	// Prepare the requested ledger range in the backend
	ledgerRange := ledgerbackend.BoundedRange(start, end)
	if err := bufferedBackend.PrepareRange(ctx, ledgerRange); err != nil {
		return nil, err
	}

	// Fetch each ledger in the range
	ledgers := make([]xdr.LedgerCloseMeta, 0, end-start+1)
	for sequence := ledgerRange.From(); sequence <= ledgerRange.To(); sequence++ {
		ledger, err := bufferedBackend.GetLedger(ctx, sequence)
		if err != nil {
			return nil, err
		}
		ledgers = append(ledgers, ledger)
	}

	return ledgers, nil
}

// GetAvailableLedgerRange returns the assumed available ledger range.
func (r *ledgerReader) GetAvailableLedgerRange(ctx context.Context) (protocol.LedgerSeqRange, error) {
	// Fast path: use cached value if still fresh.
	r.rangeMu.RLock()
	rng := r.cachedRange
	age := time.Since(r.cachedRangeAt)
	ttl := r.rangeTTL
	r.rangeMu.RUnlock()

	if rng.FirstLedger != 0 && age < ttl {
		return rng, nil
	}

	// Slow path: refresh and update cache.
	return r.refreshAvailableRange(ctx)
}

// Cache miss: fetch a batch and populate cache.
const windowSize uint32 = 20

// GetLedgerCached returns a single ledger by sequence using an internal LRU cache.
// On cache miss it fetches a batch via GetLedgers and populates the cache.
func (r *ledgerReader) GetLedgerCached(ctx context.Context, seq uint32) (xdr.LedgerCloseMeta, error) {
	// Fast path: check cache first.
	if lcm, ok := r.getFromCache(seq); ok {
		return lcm, nil
	}

	// Look at current datastore range so we don't request beyond what's available.
	dsRange, err := r.GetAvailableLedgerRange(ctx)
	if err != nil {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("getting available ledger range: %w", err)
	}

	if (dsRange.FirstLedger != 0 && seq < dsRange.FirstLedger) ||
		(dsRange.LastLedger != 0 && seq > dsRange.LastLedger) {
		return xdr.LedgerCloseMeta{}, fmt.Errorf(
			"ledger %d is outside datastore range [%d,%d]",
			seq, dsRange.FirstLedger, dsRange.LastLedger,
		)
	}

	start := seq
	end := min(seq+windowSize-1, dsRange.LastLedger)

	// Clamp end to LastLedger if it's known.
	if dsRange.LastLedger != 0 && end > dsRange.LastLedger {
		end = dsRange.LastLedger
	}

	// If clamping makes the range empty, seq is beyond what's available.
	if end < start {
		return xdr.LedgerCloseMeta{}, fmt.Errorf(
			"ledger %d is beyond datastore last ledger %d",
			seq, dsRange.LastLedger,
		)
	}

	ledgers, err := r.GetLedgers(ctx, start, end)
	if err != nil {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("fetching ledgers [%d,%d]: %w", start, end, err)
	}
	if len(ledgers) == 0 {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("ledger %d not found in datastore", seq)
	}

	r.storeBatchInCache(ledgers)
	return ledgers[0], nil
}

func (r *ledgerReader) refreshAvailableRange(ctx context.Context) (protocol.LedgerSeqRange, error) {
	// Fast path: serve valid cached range
	r.rangeMu.Lock()
	cached := r.cachedRange
	cachedAt := r.cachedRangeAt
	ttl := r.rangeTTL
	r.rangeMu.Unlock()

	if cached.FirstLedger > 0 &&
		cached.LastLedger > 0 &&
		cached.FirstLedger <= cached.LastLedger &&
		time.Since(cachedAt) < ttl {
		return cached, nil
	}

	// Slow path: query datastore
	oldest, err := datastore.FindOldestLedgerSequence(ctx, r.dataStore, r.schema)
	if err != nil {
		return protocol.LedgerSeqRange{}, fmt.Errorf("find oldest ledger: %w", err)
	}

	latest, err := datastore.FindLatestLedgerSequence(ctx, r.dataStore)
	if err != nil {
		return protocol.LedgerSeqRange{}, fmt.Errorf("find latest ledger: %w", err)
	}

	rng := protocol.LedgerSeqRange{
		FirstLedger: oldest,
		LastLedger:  latest,
	}

	// Validate result
	if rng.FirstLedger == 0 || rng.LastLedger == 0 || rng.FirstLedger > rng.LastLedger {
		return protocol.LedgerSeqRange{}, fmt.Errorf("invalid ledger range: %+v", rng)
	}

	// Update cache
	r.rangeMu.Lock()
	r.cachedRange = rng
	r.cachedRangeAt = time.Now()
	r.rangeMu.Unlock()

	return rng, nil
}

// getFromCache returns a cached ledger if present.
func (r *ledgerReader) getFromCache(seq uint32) (xdr.LedgerCloseMeta, bool) {
	if r.ledgerCache == nil {
		return xdr.LedgerCloseMeta{}, false
	}

	r.cacheMu.Lock()
	defer r.cacheMu.Unlock()

	lcm, ok := r.ledgerCache.Get(seq)
	return lcm, ok
}

// storeBatchInCache adds a contiguous batch of ledgers to the cache.
func (r *ledgerReader) storeBatchInCache(ledgers []xdr.LedgerCloseMeta) {
	if r.ledgerCache == nil || len(ledgers) == 0 {
		return
	}

	r.cacheMu.Lock()
	defer r.cacheMu.Unlock()

	for _, lcm := range ledgers {
		r.ledgerCache.Add(lcm.LedgerSequence(), lcm)
	}
}
