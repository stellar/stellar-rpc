package ledger

// cold_freeze.go — the ledgers half of the zero-decompression freeze: build a
// completed hot chunk's cold .pack DIRECTLY from its ledgers CF. The CF's
// values ARE the pack's records — one internal/rpcv2/zstd frame per ledger, the
// same level and checksum the raw-mode cold writer would produce — so the
// freeze copies frames verbatim (PreCompressed mode) instead of
// decompressing ~every ledger only to recompress it identically. What used
// to be the freeze's largest CPU stream (per the 2026-07-24 baseline
// profile, ~200s of ZSTD_compress2 per chunk plus the decompress feeding
// it) becomes a sequential checked copy.
//
// Correctness relies on three checks, not on trust: RocksDB block CRCs
// verify the read side of the copy; AppendCompressedLedger validates each
// frame header and enforces seq contiguity against the KEY-derived sequence
// (the pack resolves seqs positionally, so a CF hole must abort the freeze,
// never shift the tail); and the final count check refuses to Commit a scan
// that stopped short of the chunk's last ledger.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
)

// freezeCtxPollEvery is how many ledgers the freeze scan copies between
// context checks — frequent enough that cancellation lands in well under a
// second, rare enough to keep ctx.Err off the per-ledger fast path.
const freezeCtxPollEvery = 256

// FreezeColdFromStore builds the chunk's cold ledger .pack at packPath from
// the chunk's (read-only) hot store, copying the ledgers CF's zstd frames
// verbatim. opts tunes writeback exactly as the walk-driven build does;
// PreCompressed is forced on. Returns the number of ledgers written.
func FreezeColdFromStore(
	ctx context.Context,
	chunkID chunk.ID,
	store *rocksdb.Store,
	packPath string,
	opts ColdWriterOptions,
) (int, error) {
	n := 0
	first, last := chunkID.FirstLedger(), chunkID.LastLedger()
	if err := os.MkdirAll(filepath.Dir(packPath), 0o755); err != nil {
		return 0, fmt.Errorf("cold freeze %s: mkdir: %w", chunkID, err)
	}
	opts.PreCompressed = true
	w, werr := NewColdWriter(packPath, first, opts)
	if werr != nil {
		return 0, werr
	}
	// Close after a failed pass drops the partial pack; after Commit it is
	// a no-op release.
	defer func() { _ = w.Close() }()

	for entry, ierr := range store.IterateRange(LedgersCF, rocksdb.EncodeUint32(first), rocksdb.EncodeUint32(last)) {
		if ierr != nil {
			return n, fmt.Errorf("cold freeze %s: scan %s: %w", chunkID, LedgersCF, ierr)
		}
		if n%freezeCtxPollEvery == 0 {
			if cerr := ctx.Err(); cerr != nil {
				return n, cerr
			}
		}
		if len(entry.Key) != 4 {
			return n, fmt.Errorf("cold freeze %s: %s key length %d (want 4)", chunkID, LedgersCF, len(entry.Key))
		}
		// The seq comes from the KEY, never a local counter: the writer's
		// contiguity check must see a CF hole as a mismatch and abort.
		if aerr := w.AppendCompressedLedger(rocksdb.DecodeUint32(entry.Key), entry.Value); aerr != nil {
			return n, aerr
		}
		n++
	}
	// Contiguity from firstSeq plus this count check pins the exact range
	// [first, last]: a truncated CF (or one that never reached last) must
	// not produce a committable, silently-short pack.
	if want := int(last-first) + 1; n != want {
		return n, fmt.Errorf("cold freeze %s: copied %d ledgers, want %d", chunkID, n, want)
	}
	if cerr := w.Commit(); cerr != nil {
		return n, cerr
	}
	return n, nil
}
