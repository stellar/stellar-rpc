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

// benchCatalogSecret pins every scratch catalog's cold-index secret to one
// fixed value. A scratch catalog is created and destroyed per bench run while
// the hot DBs under --work-dir are deliberately reused across runs (freeze
// --reuse-hot), and a hot chunk is bound to the secret its sealed runs were
// blinded with — a per-run mint would make an adopted chunk unfreezable. The
// value is arbitrary: benches measure the cost of keyed routing, and only a
// deployment's minted secret has to be unpredictable.
// The copy-into-a-zero-array form is deliberate: a []byte→[32]byte conversion
// would make the literal's LENGTH load-bearing and panic at process start —
// inside a shipped subcommand — if anyone edited the string by one character.
func benchCatalogSecret() [32]byte {
	var s [32]byte
	copy(s[:], "bench-catalog-secret-fixed-value")
	return s
}

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
	cat, err := catalog.Open(filepath.Join(dir, "catalog"), layout, txLayout, logger,
		catalog.WithSecret(benchCatalogSecret()))
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, nil, fmt.Errorf("open scratch catalog: %w", err)
	}
	return cat, func() {
		_ = cat.Close()
		_ = os.RemoveAll(dir)
	}, nil
}
