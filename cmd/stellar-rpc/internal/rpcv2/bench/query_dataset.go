package bench

import (
	"fmt"
	"os"
	"slices"
	"time"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

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
	TxHashPoolSize   int
	Format           string
	Passphrase       string
	Seed             int64
}

// queryDataset is what one bench query run reads: the registry over the files
// a bench ingest run left on disk, the read server over it, and the ledger
// range the requests read.
type queryDataset struct {
	registry *query.Registry
	server   *readServer

	// Passphrase is the network passphrase the dataset's transactions were
	// signed under.
	Passphrase string

	// Tier is the tier of every benchmarked chunk: tierCold or tierHot.
	Tier string

	// Chunks is the benchmarked chunk range, ascending.
	Chunks []chunk.ID

	// FirstLedger and LastLedger bound the ledgers the requests read.
	FirstLedger, LastLedger uint32
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

// verifyServes checks that every chunk serves its ledger store and, when types
// includes events, its events store, so a gap in the dataset fails the run
// before any scenario starts.
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

// chunkRange returns the ascending chunk IDs in [start, end]. The caller must
// ensure start <= end.
func chunkRange(start, end chunk.ID) []chunk.ID {
	chunks := make([]chunk.ID, 0, end-start+1)
	for c := start; c <= end; c++ {
		chunks = append(chunks, c)
	}
	return chunks
}

// checkInputDir checks that dir, the value of --flag, is a directory.
func checkInputDir(flag, dir string) error {
	info, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("read --%s %s: %w", flag, dir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("--%s %s is not a directory", flag, dir)
	}
	return nil
}
