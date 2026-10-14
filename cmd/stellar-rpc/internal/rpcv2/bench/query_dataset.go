package bench

import (
	"fmt"
	"os"
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

	LedgersSpan uint32
	TxPageSpan  uint32
	TxPageLimit int
	Passphrase  string
	Seed        int64
}

// queryDataset is what one bench query run reads: the registry over the files
// a bench ingest run left on disk, and the ledger range the requests read.
// Every request takes a read view and resolves its tier through ReadView.
type queryDataset struct {
	registry *query.Registry

	// Passphrase is the network passphrase the dataset's transactions were
	// signed under.
	Passphrase string

	// Chunks is the benchmarked chunk range, ascending.
	Chunks []chunk.ID

	// FirstLedger and LastLedger bound the ledgers the requests read.
	FirstLedger, LastLedger uint32
}

// view acquires one read view. The caller must Release it.
func (ds *queryDataset) view() (*query.ReadView, error) {
	return ds.registry.NewReadView()
}

// verifyServes checks that the read path resolves a ledger store for every
// chunk, so a gap in the dataset fails the run before any scenario starts.
func (ds *queryDataset) verifyServes() error {
	for _, c := range ds.Chunks {
		view, err := ds.view()
		if err != nil {
			return fmt.Errorf("acquire read view: %w", err)
		}
		_, err = view.Ledgers(c)
		view.Release()
		if err != nil {
			return fmt.Errorf("chunk %s has no servable ledger store: %w", c, err)
		}
	}
	return nil
}

// chunkRange returns the ascending chunk IDs in [start, start+num). The caller
// must ensure num >= 1 and start+num-1 <= maxChunkID.
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
