package bench

import (
	"errors"
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
	cat, err := catalog.Open(layout.CatalogPath(), layout, txLayout, logger)
	if err != nil {
		return nil, fmt.Errorf("open dataset catalog %s: %w", layout.CatalogPath(), err)
	}
	return cat, nil
}

// datasetTier returns the tier of the chunks cat holds, or "" for an empty
// dataset. A cold dataset's only hot chunk is its empty live chunk.
func datasetTier(cat *catalog.Catalog) (string, error) {
	refs, err := cat.ChunkArtifactKeys()
	if err != nil {
		return "", err
	}
	if len(refs) > 0 {
		return tierCold, nil
	}
	hot, err := cat.HotChunkKeys()
	if err != nil {
		return "", err
	}
	if len(hot) > 0 {
		return tierHot, nil
	}
	return "", nil
}

// prepareIngest readies cat for an ingest of tier over chunks [start, end] and
// returns the dataset's first chunk. A new dataset pins start's first ledger as
// its earliest ledger, as the daemon pins earliest_ledger on first start. An
// existing dataset must be of the same tier and the range must start right
// after its last committed ledger; the dataset's empty live chunk at start is
// then discarded so the ingest can write that chunk.
func prepareIngest(cat *catalog.Catalog, tier string, start, end chunk.ID) (chunk.ID, error) {
	held, err := datasetTier(cat)
	if err != nil {
		return 0, err
	}
	if held != "" && held != tier {
		return 0, fmt.Errorf("the dataset holds %s chunks; a %s ingest needs its own directory", held, tier)
	}
	earliest, pinned, err := cat.EarliestLedger()
	if err != nil {
		return 0, fmt.Errorf("read the dataset's earliest ledger: %w", err)
	}
	if !pinned {
		if held != "" {
			return 0, errors.New("the dataset holds chunks but no earliest ledger; ingest into a new directory")
		}
		if err := cat.PinEarliestLedger(start.FirstLedger()); err != nil {
			return 0, fmt.Errorf("pin the dataset's earliest ledger: %w", err)
		}
		return start, nil
	}
	if err := checkAppend(cat, earliest, start, end); err != nil {
		return 0, err
	}
	if err := discardLiveChunk(cat, start); err != nil {
		return 0, err
	}
	return chunk.IDFromLedger(earliest), nil
}

// checkAppend checks that [start, end] continues the dataset pinned at
// earliest: it starts right after the last committed ledger and holds no chunk
// key except the empty live chunk at start.
func checkAppend(cat *catalog.Catalog, earliest uint32, start, end chunk.ID) error {
	if start.FirstLedger() < earliest {
		return fmt.Errorf("chunk %s starts below the dataset's earliest ledger %d", start, earliest)
	}
	last, err := rpcv2.LastCommittedLedger(cat)
	if err != nil {
		return fmt.Errorf("derive the dataset's last committed ledger: %w", err)
	}
	switch next := last + 1; {
	case start.FirstLedger() < next:
		return fmt.Errorf("the dataset already holds ledgers up to %d, which overlaps chunk %s; "+
			"ingest into a new directory", last, start)
	case start.FirstLedger() > next:
		hint := "it ends mid-chunk, so no chunk can follow it"
		if c := chunk.IDFromLedger(next); c.FirstLedger() == next {
			hint = "ingest from chunk " + c.String()
		}
		return fmt.Errorf("the dataset ends at ledger %d, so chunk %s would leave a gap; %s", last, start, hint)
	}
	refs, err := cat.ChunkArtifactKeys()
	if err != nil {
		return err
	}
	for _, ref := range refs {
		if ref.Chunk >= start && ref.Chunk <= end {
			return fmt.Errorf("the dataset already holds a %s %s artifact for chunk %s", ref.State, ref.Kind, ref.Chunk)
		}
	}
	hot, err := cat.HotChunkKeys()
	if err != nil {
		return err
	}
	for _, c := range hot {
		if c > start && c <= end {
			return fmt.Errorf("the dataset already holds hot chunk %s", c)
		}
	}
	return nil
}

// discardLiveChunk removes the hot chunk at c with the daemon's discard
// transitions. The caller must have checked that it holds no committed ledger.
func discardLiveChunk(cat *catalog.Catalog, c chunk.ID) error {
	state, err := cat.HotState(c)
	if err != nil || state == "" {
		return err
	}
	if err := cat.PutHotTransient(c); err != nil {
		return fmt.Errorf("mark live chunk %s transient: %w", c, err)
	}
	if err := cat.DestroyHotChunk(c); err != nil {
		return fmt.Errorf("discard live chunk %s: %w", c, err)
	}
	return nil
}

// openServingOnce opens and closes the dataset's read registry the way bench
// query and daemon startup open it. It creates the dataset's empty live chunk
// when the dataset has none, so a later bench query writes nothing.
func openServingOnce(cat *catalog.Catalog, logger *supportlog.Entry) error {
	earliest, _, err := cat.EarliestLedger()
	if err != nil {
		return err
	}
	last, err := rpcv2.LastCommittedLedger(cat)
	if err != nil {
		return fmt.Errorf("derive the dataset's last committed ledger: %w", err)
	}
	registry, _, err := rpcv2.OpenServing(cat, geometry.NewRetention(0, chunk.IDFromLedger(earliest)), last, logger)
	if err != nil {
		return fmt.Errorf("open the dataset for reads: %w", err)
	}
	registry.Close()
	return nil
}
