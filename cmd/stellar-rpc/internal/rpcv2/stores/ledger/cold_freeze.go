package ledger

// cold_freeze.go — the ledgers half of the zero-decompression freeze: build a
// completed hot chunk's cold .pack DIRECTLY from its ledgers CF. The CF's
// values ARE the pack's records — one internal/rpcv2/zstd frame per ledger, the
// same level and checksum the raw-mode cold writer would produce — so the
// freeze copies frames verbatim (PreCompressed mode) instead of
// decompressing ~every ledger only to recompress it identically. A framed
// ledger's span table rides along from the table CF into the record's leading
// skippable frame, so the cold tier answers a transaction lookup the way the
// hot tier does. What used
// to be the freeze's largest CPU stream (per the 2026-07-24 baseline
// profile, ~200s of ZSTD_compress2 per chunk plus the decompress feeding
// it) becomes a checked copy.
//
// Not quite zero decompression, since mainline hashes every cold pack's
// content: the hash is over RAW ledger bytes, so the packfile writer
// decompresses each frame ONCE on its hash goroutines (cold_writer.go's
// ContentHashExtract) to feed the hasher. The freeze pays that decode on as
// many workers as the caller's Concurrency buys; the recompression, and the
// decode that used to feed it on the live path, are what stay deleted.
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
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/txspan"
)

// checkPairedTable proves the span table paired with a ledger is that ledger's
// own. The freeze is the one producer that copies a table without reading it —
// every other writer builds the table from the ledger in front of it, and
// cannot mis-key one — so a hot row stored under the wrong key would otherwise
// ride into a durable pack, where a reader serving from it would answer with
// another ledger's transactions.
//
// It reads the stamp and not the whole table: the copy preserves the table's
// checksum, which the reader that parses it verifies, and a pass over every
// table of a chunk is not what the freeze is for. A mismatch fails the freeze
// loudly — it is corruption of the hot tier, not a table to drop quietly.
func checkPairedTable(seq uint32, table []byte) error {
	if len(table) == 0 {
		return nil
	}
	stamped, err := txspan.StampedSeq(table)
	if err != nil {
		return fmt.Errorf("ledger %d span table: %w", seq, err)
	}
	if stamped != seq {
		return fmt.Errorf("ledger %d is stored with a span table stamped for ledger %d", seq, stamped)
	}
	return nil
}

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

	// The span tables ride alongside the ledgers under the same keys, so the
	// paired scan carries each ledger's table without a second pass or a point
	// read. A chunk whose CF holds none — one ingested without a passphrase,
	// or a DB from before the family existed — pairs every ledger with nil,
	// and its records are written exactly as they were before tables.
	for entry, ierr := range store.IterateRangePaired(
		LedgersCF, TxSpansCF, rocksdb.EncodeUint32(first), rocksdb.EncodeUint32(last)) {
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
		seq := rocksdb.DecodeUint32(entry.Key)
		if terr := checkPairedTable(seq, entry.Paired); terr != nil {
			return n, fmt.Errorf("cold freeze %s: %w", chunkID, terr)
		}
		if aerr := w.AppendCompressedLedger(seq, entry.Value, entry.Paired); aerr != nil {
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
