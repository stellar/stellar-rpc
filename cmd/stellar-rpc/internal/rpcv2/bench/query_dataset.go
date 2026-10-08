package bench

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"slices"
	"time"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
)

// queryPlan is the parsed and validated flag set of one bench query run.
type queryPlan struct {
	Types     []string
	TargetRPS []float64
	Duration  time.Duration
	Warmup    int

	LedgersSpan      uint32
	TxPageSpan       uint32
	TxPageLimit      int
	EventsLimit      int
	NotFoundFraction float64
	Passphrase       string
	Seed             int64

	// TxHashPoolSize caps the sampled pool, in [1, maxTxHashPoolSize].
	TxHashPoolSize int

	// Evict requests OS page-cache eviction before each cold scenario.
	Evict bool

	// Settings receives the values run.json records under settings.
	Settings map[string]string
}

// cacheScenario names the page-cache state the plan's measured iterations start
// from, for settings.cacheScenario.
func (p queryPlan) cacheScenario() string {
	switch {
	case p.Warmup > 0:
		return "warm-run"
	case p.Evict:
		return "cold-start"
	default:
		return "existing-cache"
	}
}

// queryDataset is what one bench query run reads: the registry over the files
// a bench ingest run left on disk, and the ledger range the requests and pools read.
// Every request takes a read view and resolves its tier through ReadView. The
// cold dataset publishes no hot handle; the hot one freezes no artifact.
type queryDataset struct {
	registry *query.Registry

	// Passphrase is the network passphrase the dataset's transactions were
	// signed under.
	Passphrase string

	// Chunks is the benchmarked chunk range, ascending.
	Chunks []chunk.ID

	// FirstLedger and LastLedger bound the ledgers the requests and pools read.
	FirstLedger, LastLedger uint32

	// EvictPaths are the files a cold scenario requests page-cache eviction for.
	// Empty for a hot dataset.
	EvictPaths []string
}

// view acquires one read view. The caller must Release it.
func (ds *queryDataset) view() (*query.ReadView, error) {
	return ds.registry.NewReadView()
}

// verifyServes checks, one read view per chunk, that every chunk serves its
// ledger store and, when types includes events, its events store.
func (ds *queryDataset) verifyServes(types []string) error {
	events := slices.Contains(types, queryTypeEvents)
	for _, c := range ds.Chunks {
		view, err := ds.view()
		if err != nil {
			return fmt.Errorf("acquire read view: %w", err)
		}
		_, err = view.Ledgers(c)
		if err != nil {
			view.Release()
			return fmt.Errorf("chunk %s has no servable ledger store: %w", c, err)
		}
		if events {
			_, err = view.Events(c)
		}
		view.Release()
		if err != nil {
			return fmt.Errorf("chunk %s has no servable events store: %w", c, err)
		}
	}
	return nil
}

// evictColdArtifacts requests page-cache eviction of EvictPaths and returns how
// many files it advised. It skips a missing file and, off Linux, advises none.
func (ds *queryDataset) evictColdArtifacts() (int, error) {
	if !evictSupported {
		return 0, nil
	}
	evicted := 0
	for _, path := range ds.EvictPaths {
		if err := evictFile(path); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return evicted, fmt.Errorf("evict from the page cache: %w", err)
		}
		evicted++
	}
	return evicted, nil
}

// evictionState is the settings.pageCacheEviction value.
func evictionState(requested bool) string {
	switch {
	case !requested:
		return "off"
	case evictSupported:
		return "requested"
	default:
		return "unsupported-on-this-platform"
	}
}

// chunkRange returns the ascending chunk IDs in [start, start+num). The caller
// validated start+num-1 <= maxChunkID.
func chunkRange(start chunk.ID, num int) []chunk.ID {
	chunks := make([]chunk.ID, 0, num)
	for i := range uint32(num) { //nolint:gosec // num >= 1, bounded by validate()
		chunks = append(chunks, start+chunk.ID(i))
	}
	return chunks
}

// checkInputDir checks that path, the input named by what, is a directory.
func checkInputDir(what, path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("%s: %s is not a directory", what, path)
	}
	return nil
}
