package ledger

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
)

// freezeCtxEvery is how many ledgers the freeze copies between polls of the
// context.
const freezeCtxEvery = 256

// FreezeColdFromStore builds chunkID's cold ledger pack at packPath from
// store, the chunk's complete hot DB, by copying the ledgers CF's compressed
// values: they are the pack's records already. store may be open read-only.
// The scan bypasses the block cache. It returns how many ledgers it wrote.
func FreezeColdFromStore(
	ctx context.Context, chunkID chunk.ID, store *rocksdb.Store, packPath string, opts ColdWriterOptions,
) (int, error) {
	if err := os.MkdirAll(filepath.Dir(packPath), 0o755); err != nil {
		return 0, fmt.Errorf("cold freeze %s: mkdir: %w", chunkID, err)
	}
	opts.PreCompressed = true
	w, err := NewColdWriter(packPath, chunkID.FirstLedger(), opts)
	if err != nil {
		return 0, err
	}
	// Close after Commit is a no-op; before it, Close removes the partial pack.
	defer func() { _ = w.Close() }()

	n := 0
	for e, err := range store.IterateUncached(LedgersCF, nil) {
		if err != nil {
			return n, fmt.Errorf("cold freeze %s: scan %s: %w", chunkID, LedgersCF, err)
		}
		if n%freezeCtxEvery == 0 {
			if err := ctx.Err(); err != nil {
				return n, err
			}
		}
		if len(e.Key) != 4 {
			return n, fmt.Errorf("cold freeze %s: %s key %x", chunkID, LedgersCF, e.Key)
		}
		// The writer requires consecutive sequences, so a missing ledger fails
		// here rather than shifting the ones after it.
		if err := w.AppendCompressedLedger(rocksdb.DecodeUint32(e.Key), e.Value); err != nil {
			return n, err
		}
		n++
	}
	if want := int(chunkID.LastLedger()-chunkID.FirstLedger()) + 1; n != want {
		return n, fmt.Errorf("cold freeze %s: %s holds %d ledgers, want %d", chunkID, LedgersCF, n, want)
	}
	return n, w.Commit()
}
