package ingest

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stellar/streamhash"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/views"
)

const (
	// keySize is the truncated tx-hash key width stored in the cold .bin file.
	// It is pinned to streamhash.MinKeySize: the deferred streamhash index
	// builder routes/hashes on the first MinKeySize bytes of each key, so the
	// .bin producer must truncate to exactly that width for the round-trip to
	// hold. (The .bin-content test asserts this link.)
	keySize = streamhash.MinKeySize
	// entrySize is the per-entry width in the cold .bin file: keySize bytes of
	// truncated hash + a uint32 LE ledger seq.
	entrySize = keySize + 4
)

// ───────────────────────── Hot ingester ─────────────────────────

// txhashHot extracts (txhash, seq) tuples from each ledger via
// views.ExtractTxHashes and writes them in one AddEntries call (one fsync per
// ledger). The store is INJECTED and owned by the caller.
type txhashHot struct {
	store *txhash.HotStore
	sink  MetricSink
}

// NewTxhashHotIngester returns a HotIngester writing (txhash, seq) tuples into
// the injected, caller-owned store.
func NewTxhashHotIngester(store *txhash.HotStore, sink MetricSink) HotIngester {
	return &txhashHot{store: store, sink: orNop(sink)}
}

func (t *txhashHot) Ingest(_ context.Context, lcm xdr.LedgerCloseMetaView) (err error) {
	m := newHotMetrics(t.sink, dataTypeTxhash)
	defer func() { m.emit(err) }()

	seq, serr := ledgerSeqOf(lcm)
	if serr != nil {
		return fmt.Errorf("ledger seq: %w", serr)
	}
	entries, eerr := views.ExtractTxHashes(lcm)
	if eerr != nil {
		return fmt.Errorf("ExtractTxHashes seq %d: %w", seq, eerr)
	}
	if len(entries) > 0 {
		if aerr := t.store.AddEntries(entries); aerr != nil {
			return fmt.Errorf("AddEntries(seq=%d, n=%d): %w", seq, len(entries), aerr)
		}
	}
	m.items = len(entries)
	return nil
}

// ───────────────────────── Cold ingester ─────────────────────────

// txhashEntry is the in-memory tuple buffered by the cold ingester. Stored at
// full keySize key width; the cold .bin reader compares by full key.
type txhashEntry struct {
	key [keySize]byte
	seq uint32
}

// txhashCold accumulates (txhash[:keySize], seq) tuples per ledger; at Finalize
// time it lex-sorts by the truncated key and writes a per-chunk sorted .bin
// file under <out-root>/<chunkID:08d>.bin. A separate index-build step (not in
// this package) turns these .bin files into the queryable cold MPHF index.
type txhashCold struct {
	outRoot string
	chunkID chunk.ID
	entries []txhashEntry
	metrics coldMetrics
}

// NewTxhashColdIngester returns a ColdIngester that accumulates a per-chunk
// sorted .bin under coldDir, written at Finalize.
func NewTxhashColdIngester(coldDir string, chunkID chunk.ID, sink MetricSink) (ColdIngester, error) {
	if err := os.MkdirAll(coldDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", coldDir, err)
	}
	// Drop any stale .bin left by a previous aborted run of this chunk. Unlike
	// the ledger/events cold writers (which truncate their packfiles on
	// construction via Overwrite), the txhash ingester only touches the final
	// path at Finalize — so without this, a failed retry would leave a
	// complete-but-orphaned .bin that a later index build could consume while
	// the sibling ledger/events artifacts were truncated to partials. Removing
	// it here makes the chunk's txhash artifact absent until THIS run's Finalize
	// republishes it, matching the others' truncate-on-construct semantics.
	binPath := filepath.Join(coldDir, chunkID.String()+".bin")
	if rerr := os.Remove(binPath); rerr != nil && !os.IsNotExist(rerr) {
		return nil, fmt.Errorf("remove stale txhash bin %s: %w", binPath, rerr)
	}
	// Initial cap sized for a typical pubnet chunk (~3M tx total). A few
	// growths is fine; we don't want a large allocation for chunks that turn
	// out empty.
	return &txhashCold{
		outRoot: coldDir,
		chunkID: chunkID,
		entries: make([]txhashEntry, 0, 1<<16),
		metrics: newColdMetrics(sink, dataTypeTxhash),
	}, nil
}

func (t *txhashCold) Ingest(_ context.Context, lcm xdr.LedgerCloseMetaView) error {
	start := time.Now()
	seq, err := ledgerSeqOf(lcm)
	if err != nil {
		t.metrics.observe(time.Since(start), 0, err)
		return fmt.Errorf("ledger seq: %w", err)
	}
	// ExtractTxHashes returns full 32-byte hashes (copied, not view-aliased);
	// truncate each to keySize and append into the accumulator. Output is
	// identical to the hot path's full-hash extract truncated at Finalize.
	entries, err := views.ExtractTxHashes(lcm)
	if err != nil {
		t.metrics.observe(time.Since(start), 0, err)
		return fmt.Errorf("ExtractTxHashes seq %d: %w", seq, err)
	}
	for i := range entries {
		var ke txhashEntry
		copy(ke.key[:], entries[i].Hash[:keySize])
		ke.seq = seq
		t.entries = append(t.entries, ke)
	}
	t.metrics.observe(time.Since(start), len(entries), nil)
	return nil
}

// Finalize sorts the in-memory accumulator and writes the per-chunk .bin file.
//
// File layout (consumed by the separate index-build step):
//
//	header  uint64 LE   entry count
//	entry   keySize B   txhash[:keySize] (keySize = streamhash.MinKeySize)
//	        uint32 LE    absolute ledger seq
//
// Each entry is therefore entrySize (= keySize + 4) bytes wide.
func (t *txhashCold) Finalize(_ context.Context) error {
	start := time.Now()
	sort.Slice(t.entries, func(i, j int) bool {
		return bytes.Compare(t.entries[i].key[:], t.entries[j].key[:]) < 0
	})
	path := filepath.Join(t.outRoot, t.chunkID.String()+".bin")
	err := writeTxhashBin(path, t.entries)
	t.metrics.emit(time.Since(start), err)
	return err
}

// Close emits the cold metrics if Finalize never ran (the failure path); emit is
// a no-op after Finalize. There is no open file handle to release (the .bin is
// written in Finalize).
func (t *txhashCold) Close() error {
	t.metrics.emit(0, nil)
	return nil
}

// abortMetric records a synthetic abort error so a subsequent Close emit does
// not look like a clean success. Used by the constructor-rollback path.
func (t *txhashCold) abortMetric(err error) { t.metrics.recordErr(err) }

// writeTxhashBin atomically publishes the .bin file: it writes to a
// "<path>.tmp" sibling and os.Renames it onto the final path only after a
// successful flush + close. On ANY write/flush/close error it removes the
// temp file and returns the error, so a failed write never leaves a stray or
// truncated final .bin behind (the 8-byte header claims the full count, so a
// partial final file would silently feed a wrong index to the build step).
//
// The Close error is explicitly checked: on many filesystems ENOSPC/EIO only
// surface at fd close, and a silently truncated .bin would produce a wrong
// index without any signal.
func writeTxhashBin(path string, entries []txhashEntry) (err error) {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("create %s: %w", tmp, err)
	}
	// On any error past this point, drop the temp file so no partial/stray
	// artifact survives. The nil-ed f guards against double-close after the
	// explicit Close below.
	defer func() {
		if err != nil {
			if f != nil {
				_ = f.Close()
			}
			_ = os.Remove(tmp)
		}
	}()

	bw := bufio.NewWriterSize(f, 1<<20)
	var header [8]byte
	binary.LittleEndian.PutUint64(header[:], uint64(len(entries)))
	if _, werr := bw.Write(header[:]); werr != nil {
		return fmt.Errorf("write header: %w", werr)
	}
	var entryBuf [entrySize]byte
	for _, e := range entries {
		copy(entryBuf[:keySize], e.key[:])
		binary.LittleEndian.PutUint32(entryBuf[keySize:], e.seq)
		if _, werr := bw.Write(entryBuf[:]); werr != nil {
			return fmt.Errorf("write entry: %w", werr)
		}
	}
	if ferr := bw.Flush(); ferr != nil {
		return fmt.Errorf("flush: %w", ferr)
	}
	if cerr := f.Close(); cerr != nil {
		f = nil // already closed; let the deferred cleanup just Remove
		return fmt.Errorf("close %s: %w", tmp, cerr)
	}
	f = nil // closed cleanly; deferred cleanup must not touch it
	if rerr := os.Rename(tmp, path); rerr != nil {
		err = fmt.Errorf("rename %s -> %s: %w", tmp, path, rerr)
		return err
	}
	return nil
}
