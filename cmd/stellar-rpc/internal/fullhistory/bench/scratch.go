package bench

import (
	"fmt"
	"os"
	"path/filepath"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
)

// openScratchCatalog opens a fresh, run-scoped catalog in a temp dir bound to
// layout, and returns a release func that closes it and deletes the dir. The
// catalog records completed work and skips it on re-run, so a fresh one per run
// keeps every run a clean backfill from empty.
func openScratchCatalog(layout geometry.Layout, logger *supportlog.Entry) (*catalog.Catalog, func(), error) {
	dir, err := os.MkdirTemp("", "bench-ingest-catalog-")
	if err != nil {
		return nil, nil, fmt.Errorf("create scratch catalog dir: %w", err)
	}
	txLayout, err := geometry.NewTxHashIndexLayout(geometry.ChunksPerTxhashIndex)
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, nil, err
	}
	cat, err := catalog.Open(filepath.Join(dir, "catalog"), layout, txLayout, logger)
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, nil, fmt.Errorf("open scratch catalog: %w", err)
	}
	return cat, func() {
		_ = cat.Close()
		_ = os.RemoveAll(dir)
	}, nil
}
