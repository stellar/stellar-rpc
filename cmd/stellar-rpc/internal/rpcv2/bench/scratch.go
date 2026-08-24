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

// Temp-dir prefixes naming which bench owns a scratch catalog, so a leftover
// dir says where it came from.
const (
	scratchPrefixIngest = "bench-ingest-catalog-"
	scratchPrefixQuery  = "bench-query-catalog-"
)

// openScratchCatalog creates a fresh catalog in a temp dir named prefix* under
// catalogBase and returns a release func that closes it and removes that temp
// dir.
func openScratchCatalog(
	catalogBase, prefix string, layout geometry.Layout, logger *supportlog.Entry,
) (*catalog.Catalog, func(), error) {
	if err := os.MkdirAll(catalogBase, catalogBaseDirPerm); err != nil {
		return nil, nil, fmt.Errorf("create catalog base dir %s: %w", catalogBase, err)
	}
	dir, err := os.MkdirTemp(catalogBase, prefix)
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
