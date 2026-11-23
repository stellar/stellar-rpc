package txhash

import (
	"bytes"
	"context"
	"fmt"
	"iter"
	"os"
	"path/filepath"

	"github.com/stellar/streamhash"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
)

// freezeCtxEvery is how many entries the freeze writes between polls of the
// context.
const freezeCtxEvery = 1 << 16

// FreezeColdFromStore writes bin.Chunk's .bin at path from its complete hot DB,
// merging the sealed windows with the rows after the last seal, and returns
// the entry count. unsealed returns the rows of the ledgers from a ledger on,
// blinded with bin.Secret. The marker and the scan read one snapshot, so a
// seal that lands meanwhile changes neither.
func FreezeColdFromStore(
	ctx context.Context, store *rocksdb.Store, unsealed func(from uint32) ([][]byte, error),
	path string, bin ColdBin, bytesPerSync int,
) (uint64, error) {
	if err := checkSecret(store, bin.Secret, false); err != nil {
		return 0, err
	}
	snap, err := store.NewSnapshot()
	if err != nil {
		return 0, err
	}
	defer store.ReleaseSnapshot(snap)
	lastSealed, sealedKeys, err := decodeMarker(store.GetAsOf(snap, txhashCF, []byte(markerKey)))
	if err != nil {
		return 0, err
	}
	from := max(lastSealed+1, bin.Chunk.FirstLedger())
	rows, err := unsealed(from)
	if err != nil {
		return 0, fmt.Errorf("txhash: freeze chunk %s: rows from ledger %d: %w", bin.Chunk, from, err)
	}
	tail := make([]row, len(rows))
	for i, keys := range rows {
		tail[i] = row{seq: from + uint32(i), keys: keys} //nolint:gosec // at most a chunk's rows
	}
	if end := from + uint32(len(tail)); end != bin.Chunk.LastLedger()+1 { //nolint:gosec // at most a chunk's rows
		return 0, fmt.Errorf("txhash: freeze chunk %s: rows end at ledger %d, want %d",
			bin.Chunk, end-1, bin.Chunk.LastLedger())
	}
	var tailKeys uint64
	for _, r := range tail {
		tailKeys += uint64(len(r.keys) / ColdKeySize)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return 0, fmt.Errorf("txhash: freeze chunk %s: mkdir: %w", bin.Chunk, err)
	}
	w, err := streamhash.NewSortedFileWriter(path, bin.SortedFileConfig(bytesPerSync))
	if err != nil {
		return 0, fmt.Errorf("txhash: freeze chunk %s: %w", bin.Chunk, err)
	}
	defer w.Close()
	f := &freezeWriter{bin: bin, w: w}
	if err := writeMerged(ctx, store, snap, tail, f); err != nil {
		return 0, fmt.Errorf("txhash: freeze chunk %s: %w", bin.Chunk, err)
	}
	// The scan shows a key two seals hold once, so a duplicate across seals
	// shows up here as a missing entry.
	if want := sealedKeys + tailKeys; f.n != want {
		return 0, fmt.Errorf("txhash: freeze chunk %s: wrote %d entries, the chunk holds %d keys", bin.Chunk, f.n, want)
	}
	return f.n, w.Finish()
}

// freezeWriter writes a chunk's entries to its .bin, refusing an entry that
// names a ledger outside the chunk.
type freezeWriter struct {
	bin ColdBin
	w   *streamhash.SortedFileWriter
	n   uint64
}

func (f *freezeWriter) add(ctx context.Context, key []byte, seq uint32) error {
	payload, err := f.bin.Payload(seq)
	if err != nil {
		return fmt.Errorf("key %x: %w", key, err)
	}
	if f.n%freezeCtxEvery == 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	f.n++
	return f.w.Add(key, payload)
}

// writeMerged writes txhashCF as of snap merged with the tail rows, in key order.
func writeMerged(
	ctx context.Context, store *rocksdb.Store, snap *rocksdb.Snapshot, tail []row, f *freezeWriter,
) error {
	nextTail, stop := iter.Pull2(mergeRows(tail))
	defer stop()
	tailKey, tailSeq, tailOK := nextTail()
	for e, err := range store.IterateUncachedAsOf(snap, txhashCF, nil) {
		if err != nil {
			return fmt.Errorf("scan %s: %w", txhashCF, err)
		}
		if string(e.Key) == markerKey {
			continue
		}
		if len(e.Key) != ColdKeySize || len(e.Value) != 4 {
			return fmt.Errorf("corrupt chunk: %s entry %x", txhashCF, e.Key)
		}
		for ; tailOK && bytes.Compare(tailKey, e.Key) < 0; tailKey, tailSeq, tailOK = nextTail() {
			if err := f.add(ctx, tailKey, tailSeq); err != nil {
				return err
			}
		}
		if err := f.add(ctx, e.Key, rocksdb.DecodeUint32(e.Value)); err != nil {
			return err
		}
	}
	for ; tailOK; tailKey, tailSeq, tailOK = nextTail() {
		if err := f.add(ctx, tailKey, tailSeq); err != nil {
			return err
		}
	}
	return nil
}
