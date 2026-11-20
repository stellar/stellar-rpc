package entrychange

import (
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event"
)

// Reader is one chunk's index, uniform across tiers.
type Reader interface {
	// ChangeLedgers returns, aligned with terms, the ledgers in the chunk that
	// changed each term's key, nil when none did. The bitmaps hold ledger
	// sequences and are the caller's.
	ChangeLedgers(ctx context.Context, terms []event.TermKey) ([]*roaring.Bitmap, error)
}

// ─────────────────────────── Hot ───────────────────────────

// cf is the hot index column family. A row is term ‖ big-endian ledger with
// no value, so one term's ledgers are one prefix scan in ascending order.
const cf = "entrychanges"

// CFNames returns the column family the hot facade owns, for the hotchunk
// opener to register.
func CFNames() []string { return []string{cf} }

// HotStore is the index over a chunk's hot DB. It owns no store: hotchunk.DB
// composes it over the shared one.
type HotStore struct {
	store *rocksdb.Store
}

var _ Reader = (*HotStore)(nil)

// NewWithStore wraps an open store that has CFNames registered.
func NewWithStore(store *rocksdb.Store) *HotStore { return &HotStore{store: store} }

// AddLedgerToBatch queues the rows recording that seq changed each term.
func (h *HotStore) AddLedgerToBatch(b *rocksdb.BatchWriter, seq uint32, terms []event.TermKey) {
	for _, t := range terms {
		key := make([]byte, 0, len(t)+4)
		key = append(key, t[:]...)
		key = binary.BigEndian.AppendUint32(key, seq)
		b.Put(cf, key, nil)
	}
}

// ChangeLedgers scans each term's rows.
func (h *HotStore) ChangeLedgers(ctx context.Context, terms []event.TermKey) ([]*roaring.Bitmap, error) {
	out := make([]*roaring.Bitmap, len(terms))
	for i, t := range terms {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		for e, err := range h.store.Iterate(cf, t[:]) {
			if err != nil {
				return nil, err
			}
			if len(e.Key) != len(t)+4 {
				return nil, fmt.Errorf("%w: entrychanges row of %d bytes", stores.ErrCorrupt, len(e.Key))
			}
			if out[i] == nil {
				out[i] = roaring.New()
			}
			out[i].Add(binary.BigEndian.Uint32(e.Key[len(t):]))
		}
	}
	return out, nil
}

// ─────────────────────────── Cold ───────────────────────────

// coldRoutingDomain is the DeriveIndexSecret domain for this index.
const coldRoutingDomain = "entrychanges"

// ColdIndexSecret derives chunkID's routing secret from the catalog secret.
func ColdIndexSecret(catalogSecret []byte, chunkID chunk.ID) [stores.SecretLen]byte {
	return stores.DeriveIndexSecret(catalogSecret, coldRoutingDomain, uint32(chunkID))
}

// PackName and HashName are a chunk's cold index files, in its bucket dir.
func PackName(c chunk.ID) string { return c.String() + "-entrychanges.pack" }

// HashName is the chunk's index.hash counterpart to PackName.
func HashName(c chunk.ID) string { return c.String() + "-entrychanges.hash" }

// ColdWriter accumulates one chunk's index for freezing.
type ColdWriter struct {
	chunkID chunk.ID
	dir     string
	secret  [stores.SecretLen]byte
	bitmaps event.Bitmaps
}

// NewColdWriter starts chunkID's index, to be written into dir.
func NewColdWriter(chunkID chunk.ID, dir string, secret [stores.SecretLen]byte) *ColdWriter {
	return &ColdWriter{chunkID: chunkID, dir: dir, secret: secret, bitmaps: event.NewBitmaps()}
}

// Add records that seq changed each term.
func (w *ColdWriter) Add(seq uint32, terms []event.TermKey) {
	offset := seq - w.chunkID.FirstLedger()
	for _, t := range terms {
		w.bitmaps.AddTo(t, offset)
	}
}

// Finish writes and fsyncs the index pair.
func (w *ColdWriter) Finish(ctx context.Context) error {
	return event.WriteColdIndexFiles(ctx, w.bitmaps,
		filepath.Join(w.dir, PackName(w.chunkID)), filepath.Join(w.dir, HashName(w.chunkID)), w.secret)
}

// ColdReader is a frozen chunk's index.
type ColdReader struct {
	chunkID chunk.ID
	index   *event.ColdIndexReader
}

var _ Reader = (*ColdReader)(nil)

// OpenColdReader opens chunkID's index in dir.
func OpenColdReader(chunkID chunk.ID, dir string) (*ColdReader, error) {
	r, err := event.OpenColdIndexReader(filepath.Join(dir, PackName(chunkID)), filepath.Join(dir, HashName(chunkID)),
		event.ColdReaderOptions{})
	if err != nil {
		return nil, err
	}
	return &ColdReader{chunkID: chunkID, index: r}, nil
}

// ChangeLedgers looks the terms up and turns ledger offsets into sequences.
func (r *ColdReader) ChangeLedgers(ctx context.Context, terms []event.TermKey) ([]*roaring.Bitmap, error) {
	bitmaps, err := r.index.LookupKeys(ctx, terms)
	if err != nil {
		return nil, err
	}
	first := r.chunkID.FirstLedger()
	for i, bm := range bitmaps {
		if bm == nil {
			continue
		}
		if !bm.IsEmpty() && bm.Maximum() > r.chunkID.LastLedger()-first {
			return nil, fmt.Errorf("%w: entrychanges offset %d past chunk %s", stores.ErrCorrupt, bm.Maximum(), r.chunkID)
		}
		abs := roaring.New()
		for it := bm.Iterator(); it.HasNext(); {
			abs.Add(it.Next() + first)
		}
		bitmaps[i] = abs
	}
	return bitmaps, nil
}

// Close releases the index files.
func (r *ColdReader) Close() error { return r.index.Close() }
