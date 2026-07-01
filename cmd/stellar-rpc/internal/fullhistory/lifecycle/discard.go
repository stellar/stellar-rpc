package lifecycle

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// discardHotDBForChunk retires a chunk's hot DB once its cold artifacts are
// durable (or it fell past retention): transient -> rmdir+fsync parent -> delete
// key. Idempotent — a missing key is a no-op, and a crash mid-discard leaves the
// key "transient" for the next scan to finish. The caller must have closed the
// write handle (the stage runs after executePlan froze the cold artifacts).
func discardHotDBForChunk(cat *catalog.Catalog, chunkID chunk.ID) error {
	state, err := cat.HotState(chunkID)
	if err != nil {
		return fmt.Errorf("read hot key chunk %s: %w", chunkID, err)
	}
	if state == "" {
		return nil
	}
	if putErr := cat.PutHotTransient(chunkID); putErr != nil {
		return fmt.Errorf("mark hot transient chunk %s: %w", chunkID, putErr)
	}

	dir := cat.Layout().HotChunkPath(chunkID)
	if rmErr := os.RemoveAll(dir); rmErr != nil {
		return fmt.Errorf("rmdir hot dir %s: %w", dir, rmErr)
	}
	// rmdir must be durable BEFORE the key delete: the key outlives the dir, so a
	// crash re-runs the discard rather than leaving a key-less dir.
	if syncErr := geometry.FsyncDir(filepath.Dir(dir)); syncErr != nil {
		return fmt.Errorf("fsync hot parent dir %s: %w", filepath.Dir(dir), syncErr)
	}
	if delErr := cat.DeleteHotKey(chunkID); delErr != nil {
		return fmt.Errorf("delete hot key chunk %s: %w", chunkID, delErr)
	}
	return nil
}
