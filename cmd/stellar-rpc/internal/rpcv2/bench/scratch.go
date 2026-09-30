package bench

import (
	"fmt"
	"os"
	"path/filepath"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
)

const catalogBaseDirPerm os.FileMode = 0o755 // owner rwx, group/others rx

// openScratchCatalog creates a fresh catalog in a temp dir under catalogBase
// and returns a release func that closes it and removes that temp dir.
func openScratchCatalog(
	catalogBase string, layout geometry.Layout, logger *supportlog.Entry,
) (*catalog.Catalog, func(), error) {
	if err := os.MkdirAll(catalogBase, catalogBaseDirPerm); err != nil {
		return nil, nil, fmt.Errorf("create catalog base dir %s: %w", catalogBase, err)
	}
	dir, err := os.MkdirTemp(catalogBase, "bench-ingest-catalog-")
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
