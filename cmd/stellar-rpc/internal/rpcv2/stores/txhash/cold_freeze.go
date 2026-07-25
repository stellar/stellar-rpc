package txhash

// cold_freeze.go — the txhash half of the zero-decompression freeze: build a
// completed hot chunk's cold .bin DIRECTLY from its txhash CF. The CF's keys
// are full 32-byte hashes in RocksDB's bytewise order, and truncating a
// bytewise-sorted sequence to its 16-byte prefix preserves that order — so
// the CF scan yields the .bin's entries ALREADY lex-sorted, deleting both
// the walk path's whole-chunk accumulate-then-sort (the baseline's ~40s,
// ~1.2GB-live finalize at sac-6000 density) and its share of the raw-ledger
// decompress walk. Two encodings meet here and must not be conflated: the
// CF value is a BIG-endian seq (rocksdb.EncodeUint32), the .bin stores
// LITTLE-endian — every value is decoded and re-encoded, never memcpy'd.
//
// Duplicate-prefix semantics are unchanged from the walk path: entries are
// written verbatim and the downstream streamhash build rejects a truncated-
// key collision loudly.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
)

// freezeCtxPollEvery is how many entries the freeze scan copies between
// context checks. Entries are 36 bytes of work apiece, so a coarser cadence
// than the ledgers scan still cancels promptly.
const freezeCtxPollEvery = 4096

// FreezeColdFromStore builds the chunk's cold txhash .bin at binPath from
// the chunk's (read-only) hot store. Entries stream from the CF straight to
// the .bin — no whole-chunk accumulator (hundreds of MB at stress density);
// the header's leading count is patched in once the scan completes, and the
// bytes are identical to WriteColdBin's. Returns the entries written.
func FreezeColdFromStore(
	ctx context.Context,
	chunkID chunk.ID,
	store *rocksdb.Store,
	binPath string,
) (int, error) {
	first, last := chunkID.FirstLedger(), chunkID.LastLedger()
	if err := os.MkdirAll(filepath.Dir(binPath), 0o755); err != nil {
		return 0, fmt.Errorf("txhash freeze %s: mkdir: %w", chunkID, err)
	}
	w, err := newColdBinStream(binPath)
	if err != nil {
		return 0, err
	}
	defer w.abort()
	n := 0
	for entry, ierr := range store.Iterate(txhashCF, nil) {
		if ierr != nil {
			return 0, fmt.Errorf("txhash freeze %s: scan %s: %w", chunkID, txhashCF, ierr)
		}
		if n%freezeCtxPollEvery == 0 {
			if cerr := ctx.Err(); cerr != nil {
				return 0, cerr
			}
		}
		if len(entry.Key) != 32 || len(entry.Value) != 4 {
			return 0, fmt.Errorf("txhash freeze %s: row shape %d/%d (want 32/4)",
				chunkID, len(entry.Key), len(entry.Value))
		}
		seq := rocksdb.DecodeUint32(entry.Value)
		if seq < first || seq > last {
			return 0, fmt.Errorf("txhash freeze %s: entry seq %d outside [%d, %d]", chunkID, seq, first, last)
		}
		var ce ColdEntry
		copy(ce.Key[:], entry.Key[:ColdKeySize])
		ce.Seq = seq
		if werr := w.append(ce); werr != nil {
			return 0, werr
		}
		n++
	}
	if ferr := w.finish(); ferr != nil {
		return 0, ferr
	}
	return n, nil
}
