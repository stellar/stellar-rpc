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

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash"
)

const (
	// keySize is the truncated tx-hash key width stored in the cold .bin file.
	keySize = 16
	// entrySize is the per-entry width in the cold .bin file: keySize bytes of
	// truncated hash + a uint32 LE ledger seq.
	entrySize = keySize + 4
)

// extractTxHashes reads precomputed transaction hashes off the parsed
// LedgerCloseMeta. Cheap — TransactionHash(i) returns a stored value, no
// walking or hashing required. l.LCM must be non-nil.
func extractTxHashes(l Ledger) ([]txhash.Entry, error) {
	if l.LCM == nil {
		return nil, fmt.Errorf("txhash ingester requires a parsed LCM but ledger %d has none", l.Seq)
	}
	lcm := *l.LCM
	n := lcm.CountTransactions()
	out := make([]txhash.Entry, 0, n)
	for i := range n {
		h := lcm.TransactionHash(i)
		out = append(out, txhash.Entry{Hash: h, LedgerSeq: l.Seq})
	}
	return out, nil
}

// ───────────────────────── Hot ingester ─────────────────────────

// txhashHot extracts (txhash, seq) tuples from each ledger and writes them in
// one AddEntries call. AddEntries fsyncs once per ledger.
type txhashHot struct {
	store *txhash.HotStore
}

func newTxhashHot(dir string, logger *supportlog.Entry) (*txhashHot, error) {
	if err := os.MkdirAll(filepath.Dir(dir), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir parent of %s: %w", dir, err)
	}
	store, err := txhash.OpenHotStore(dir, logger)
	if err != nil {
		return nil, fmt.Errorf("txhash.OpenHotStore %s: %w", dir, err)
	}
	return &txhashHot{store: store}, nil
}

func (t *txhashHot) Ingest(_ context.Context, l Ledger) error {
	entries, err := extractTxHashes(l)
	if err != nil {
		return fmt.Errorf("extract seq %d: %w", l.Seq, err)
	}
	if len(entries) > 0 {
		if err := t.store.AddEntries(entries); err != nil {
			return fmt.Errorf("AddEntries(seq=%d, n=%d): %w", l.Seq, len(entries), err)
		}
	}
	return nil
}

func (t *txhashHot) Close() error { return t.store.Close() }

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
}

func newTxhashCold(outRoot string, chunkID chunk.ID) (*txhashCold, error) {
	if err := os.MkdirAll(outRoot, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", outRoot, err)
	}
	// Initial cap sized for a typical pubnet chunk (~3M tx total). A few
	// growths is fine; we don't want a large allocation for chunks that turn
	// out empty.
	return &txhashCold{
		outRoot: outRoot,
		chunkID: chunkID,
		entries: make([]txhashEntry, 0, 1<<16),
	}, nil
}

func (t *txhashCold) Ingest(_ context.Context, l Ledger) error {
	if l.LCM == nil {
		return fmt.Errorf("txhash ingester requires a parsed LCM but ledger %d has none", l.Seq)
	}
	lcm := *l.LCM
	// Single pass: read each precomputed tx hash off the LCM, truncate to
	// keySize, and append directly into the accumulator — no intermediate
	// []txhash.Entry. Output is identical to the hot path's full-hash extract
	// truncated at Finalize.
	n := lcm.CountTransactions()
	for i := range n {
		h := lcm.TransactionHash(i)
		var ke txhashEntry
		copy(ke.key[:], h[:keySize])
		ke.seq = l.Seq
		t.entries = append(t.entries, ke)
	}
	return nil
}

// Finalize sorts the in-memory accumulator and writes the per-chunk .bin file.
//
// File layout (consumed by the separate index-build step):
//
//	header  uint64 LE  entry count
//	entry   16 bytes    txhash[:keySize]
//	        uint32 LE   absolute ledger seq
func (t *txhashCold) Finalize(_ context.Context) error {
	sort.Slice(t.entries, func(i, j int) bool {
		return bytes.Compare(t.entries[i].key[:], t.entries[j].key[:]) < 0
	})
	path := filepath.Join(t.outRoot, t.chunkID.String()+".bin")
	return writeTxhashBin(path, t.entries)
}

// Close is a no-op for cold txhash: no open file handle (the .bin is written in
// Finalize).
func (t *txhashCold) Close() error { return nil }

// writeTxhashBin writes the .bin file via os.Create + bufio.
//
// The Close error is explicitly checked: on many filesystems ENOSPC/EIO only
// surface at fd close, and a silently truncated .bin would produce a wrong
// index without any signal.
func writeTxhashBin(path string, entries []txhashEntry) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	bw := bufio.NewWriterSize(f, 1<<20)
	var header [8]byte
	binary.LittleEndian.PutUint64(header[:], uint64(len(entries)))
	if _, werr := bw.Write(header[:]); werr != nil {
		_ = f.Close()
		return fmt.Errorf("write header: %w", werr)
	}
	var entryBuf [entrySize]byte
	for _, e := range entries {
		copy(entryBuf[:keySize], e.key[:])
		binary.LittleEndian.PutUint32(entryBuf[keySize:], e.seq)
		if _, werr := bw.Write(entryBuf[:]); werr != nil {
			_ = f.Close()
			return fmt.Errorf("write entry: %w", werr)
		}
	}
	if ferr := bw.Flush(); ferr != nil {
		_ = f.Close()
		return fmt.Errorf("flush: %w", ferr)
	}
	if cerr := f.Close(); cerr != nil {
		return fmt.Errorf("close %s: %w", path, cerr)
	}
	return nil
}
