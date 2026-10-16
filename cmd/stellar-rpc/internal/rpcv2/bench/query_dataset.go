package bench

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"slices"
	"time"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
)

// queryPlan is the parsed and validated flag set of one bench query run.
type queryPlan struct {
	Types     []string
	TargetRPS []float64
	Duration  time.Duration
	Warmup    int

	LedgersLimit     uint32
	TxPageLimit      uint32
	EventsLimit      uint32
	NotFoundFraction float64
	Format           string
	Passphrase       string
	Seed             int64

	// TxHashPoolSize caps the sampled pool, in [1, maxTxHashPoolSize].
	TxHashPoolSize int

	// WarmupSet reports whether --warmup was given; forTier sets the tier's
	// default otherwise.
	WarmupSet bool

	// Evict requests OS page-cache eviction before each scenario; forTier
	// clears it for a hot dataset.
	Evict bool
}

// defaultHotWarmup is --warmup's default for a hot dataset; a cold one
// defaults to 0.
const defaultHotWarmup = 20

// forTier returns p with the defaults of a dataset of tier: a hot dataset
// warms up defaultHotWarmup iterations unless --warmup was given, and evicts
// nothing. It checks again that every rate gives a runnable scenario.
func (p queryPlan) forTier(tier string) (queryPlan, error) {
	if tier != tierHot {
		return p, nil
	}
	p.Evict = false
	if !p.WarmupSet {
		p.Warmup = defaultHotWarmup
	}
	for _, rps := range p.TargetRPS {
		if _, _, err := validateScenario(rps, p.Duration, p.Warmup); err != nil {
			return p, fmt.Errorf("--target-rps, --duration and the hot --warmup default: %w", err)
		}
	}
	return p, nil
}

// cacheScenario names the page-cache state the plan's measured iterations start
// from, for settings.cacheScenario.
func (p queryPlan) cacheScenario() string {
	switch {
	case p.Warmup > 0:
		return "warm-run"
	case p.Evict && evictSupported:
		return "cold-start"
	default:
		return "existing-cache"
	}
}

// queryDataset is what one bench query run reads: the dataset catalog, the
// registry over the files a bench ingest run left on disk, the read server over
// it, and the ledger range the requests and pools read.
type queryDataset struct {
	catalog  *catalog.Catalog
	registry *query.Registry
	server   *readServer

	// Passphrase is the network passphrase the dataset's transactions were
	// signed under.
	Passphrase string

	// Tier is the tier of every benchmarked chunk: tierCold or tierHot.
	Tier string

	// Chunks is the benchmarked chunk range, ascending.
	Chunks []chunk.ID

	// FirstLedger and LastLedger bound the ledgers the requests and pools read.
	FirstLedger, LastLedger uint32

	// EvictPaths are the files a scenario requests page-cache eviction for.
	// Empty for a hot dataset.
	EvictPaths []string
}

// serveReads starts ds's read server and returns a release that closes it
// before it calls release, which closes the registry.
func (ds *queryDataset) serveReads(logger *supportlog.Entry, release func()) func() {
	ds.server = newReadServer(ds.registry, ds.Passphrase, logger)
	return func() {
		ds.server.close()
		release()
	}
}

// view acquires one read view. The caller must Release it.
func (ds *queryDataset) view() (*query.ReadView, error) {
	return ds.registry.NewReadView()
}

// verifyServes checks, one read view per chunk, that every chunk serves its
// ledger store and, when types includes events, its events store, so a gap in
// the dataset fails the run before any scenario starts.
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
// A cancel stops it before the next file and returns the context error.
func (ds *queryDataset) evictColdArtifacts(ctx context.Context) (int, error) {
	if !evictSupported {
		return 0, nil
	}
	evicted := 0
	for _, path := range ds.EvictPaths {
		if err := ctx.Err(); err != nil {
			return evicted, err
		}
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

// chunkRange returns the ascending chunk IDs in [start, end]. The caller must
// ensure start <= end.
func chunkRange(start, end chunk.ID) []chunk.ID {
	chunks := make([]chunk.ID, 0, end-start+1)
	for c := start; c <= end; c++ {
		chunks = append(chunks, c)
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
