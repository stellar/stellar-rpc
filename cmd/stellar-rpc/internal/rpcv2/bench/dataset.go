package bench

import (
	"fmt"
	"os"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
)

// dirPerm is the mode of the directories the bench creates: owner rwx,
// group and others rx.
const dirPerm os.FileMode = 0o755

// Dataset tiers: a cold dataset holds frozen chunks, a hot dataset holds hot
// chunk databases.
const (
	tierCold = "cold"
	tierHot  = "hot"
)

// openDatasetCatalog opens the catalog of the dataset under layout, creating it
// when absent. The caller must close it.
func openDatasetCatalog(layout geometry.Layout, logger *supportlog.Entry) (*catalog.Catalog, error) {
	txLayout, err := geometry.NewTxHashIndexLayout(geometry.ChunksPerTxhashIndex)
	if err != nil {
		return nil, err
	}
	cat, err := catalog.Open(layout.CatalogPath(), layout.WithTxHashIndex(txLayout), logger)
	if err != nil {
		return nil, fmt.Errorf("open dataset catalog %s: %w", layout.CatalogPath(), err)
	}
	return cat, nil
}

// unfinishedHint ends the errors for a dataset an earlier ingest left
// unfinished.
const unfinishedHint = "an earlier ingest into it did not finish; delete the dataset directory and ingest again"

// finishIngest completes a successful ingest into a new dataset whose first
// chunk is first. It pins first's first ledger as the earliest ledger, as the
// daemon does on first start, and makes sure the live chunk's hot DB exists,
// as daemon startup does, so a later bench query changes no catalog key.
func finishIngest(cat *catalog.Catalog, first chunk.ID, logger *supportlog.Entry) error {
	if err := cat.PinEarliestLedger(first.FirstLedger()); err != nil {
		return fmt.Errorf("pin the dataset's earliest ledger: %w", err)
	}
	last, err := rpcv2.LastCommittedLedger(cat)
	if err != nil {
		return fmt.Errorf("derive the dataset's last committed ledger: %w", err)
	}
	if err := rpcv2.CreateLiveChunk(cat, last, logger); err != nil {
		return fmt.Errorf("create the dataset's live chunk: %w", err)
	}
	return nil
}
