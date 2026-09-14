package verify

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/stellar/go-stellar-sdk/historyarchive"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/ledger"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/txhash"
)

// deps is what every chunk's verification shares.
type deps struct {
	opts    Options
	cat     *catalog.Catalog
	archive *historyarchive.Archive
	indexes *indexCache
}

type chunkRun struct {
	d      *deps
	c      chunk.ID
	frozen catalog.ArtifactSet
	rec    *recorder

	events *eventsChecker
	index  *indexChecker
	bin    *binChecker

	prevHash  *xdr.Hash
	firstHash xdr.Hash
	ledgers   uint32
	txs       uint64
	txHashes  uint64
	// sourceBad is set by the first ledger that fails a source check. The
	// remaining ledgers still get their source checks, but nothing derived
	// from a bad source is compared.
	sourceBad bool
}

func (r *chunkRun) run(ctx context.Context) error {
	lr, err := ledger.OpenColdReader(r.d.cat.Layout().LedgerPackPath(r.c))
	if err != nil {
		return err
	}
	defer func() { _ = lr.Close() }()
	ok, err := r.checkPack(ctx, lr)
	if err != nil || !ok {
		return err
	}
	if r.prevHash, err = r.previousChunkHash(); err != nil {
		return err
	}
	if err := r.openCheckers(ctx); err != nil {
		return err
	}
	defer r.closeCheckers()
	for entry, err := range lr.IterateLedgers(r.c.FirstLedger(), r.c.LastLedger()) {
		if err != nil {
			return err
		}
		if err := r.ledger(entry.Seq, entry.Bytes); err != nil {
			return err
		}
	}
	if r.sourceBad {
		return nil
	}
	return r.finish(ctx)
}

// checkPack verifies the pack's content hash and that it spans the whole
// chunk. A failure is a verdict on the pack unless the run itself was
// canceled.
func (r *chunkRun) checkPack(ctx context.Context, lr *ledger.ColdReader) (bool, error) {
	if err := lr.Verify(ctx); err != nil {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		r.rec.add(Mismatch{
			Artifact: "ledgers", Field: "content_hash", Expected: "the hash the writer stored", Actual: err.Error(),
		})
		return false, nil
	}
	last, err := lr.LastSeq()
	if err != nil {
		r.rec.add(Mismatch{Artifact: "ledgers", Field: "pack", Actual: err.Error()})
		return false, nil //nolint:nilerr // recorded as a verdict on the pack
	}
	if last != r.c.LastLedger() {
		r.rec.add(Mismatch{Artifact: "ledgers", Field: "last_ledger", Expected: u32(r.c.LastLedger()), Actual: u32(last)})
		return false, nil
	}
	return true, nil
}

// previousChunkHash returns the hash of the ledger before this chunk's first,
// read from the previous chunk's frozen pack, or nil when there is none to
// read: chunk 0 follows the genesis ledger, which no pack holds.
func (r *chunkRun) previousChunkHash() (*xdr.Hash, error) {
	if r.c == 0 {
		return nil, nil //nolint:nilnil // no predecessor is a valid answer, not an error
	}
	prev := r.c - 1
	state, err := r.d.cat.State(prev, geometry.KindLedgers)
	if err != nil {
		return nil, err
	}
	if state != geometry.StateFrozen {
		return nil, nil //nolint:nilnil // see above
	}
	lr, err := ledger.OpenColdReader(r.d.cat.Layout().LedgerPackPath(prev))
	if err != nil {
		return nil, err
	}
	defer func() { _ = lr.Close() }()
	var h xdr.Hash
	err = lr.WithLedger(r.c.FirstLedger()-1, func(raw []byte) error {
		b, err := xdr.LedgerCloseMetaView(raw).LedgerHash()
		copy(h[:], b)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("previous chunk %s: %w", prev, err)
	}
	return &h, nil
}

// openCheckers opens one checker per artifact the chunk has: its events
// segment and its .bin when those are frozen, and the tx-hash index when a
// frozen coverage contains the chunk. A chunk whose .bin was demoted after
// its index finalized has only the index; one still waiting on its index
// build has only the .bin.
func (r *chunkRun) openCheckers(ctx context.Context) error {
	layout := r.d.cat.Layout()
	if r.frozen.Has(geometry.KindEvents) {
		ec, err := newEventsChecker(ctx, r.rec, r.c, layout.EventsBucketDir(r.c))
		if err != nil {
			return err
		}
		r.events = ec
	}
	idx, covered, err := r.d.indexes.forChunk(r.c)
	if err != nil {
		r.closeCheckers()
		return err
	}
	if covered {
		r.index = &indexChecker{rec: r.rec, idx: idx}
	}
	if r.frozen.Has(geometry.KindTxHash) {
		bc, err := newBinChecker(r.rec, layout.TxHashBinPath(r.c), r.d.cat.TxHashIndexSecret(r.c))
		if err != nil {
			r.closeCheckers()
			return err
		}
		r.bin = bc
	}
	return nil
}

func (r *chunkRun) closeCheckers() {
	if r.events != nil {
		_ = r.events.close()
	}
}

func (r *chunkRun) ledger(seq uint32, raw []byte) error {
	var lcm xdr.LedgerCloseMeta
	if err := xdr.SafeUnmarshal(raw, &lcm); err != nil {
		r.rec.add(Mismatch{Ledger: seq, Artifact: "ledgers", Field: "decode", Actual: err.Error()})
		r.sourceBad = true
		return nil //nolint:nilerr // recorded as a mismatch: the source is bad, the run is fine
	}
	r.ledgers++
	entry := lcm.LedgerHeaderHistoryEntry()
	if seq == r.c.FirstLedger() {
		r.firstHash = entry.Hash
	}
	if !checkLedger(r.rec, seq, &lcm, r.prevHash) {
		r.sourceBad = true
	}
	h := entry.Hash
	r.prevHash = &h
	if r.sourceBad {
		return nil
	}
	exp, err := expectLedger(r.d.opts.Passphrase, &lcm)
	if err != nil {
		r.rec.add(Mismatch{Ledger: seq, Artifact: "ledgers", Field: "decode_path", Actual: err.Error()})
		r.sourceBad = true
		return nil //nolint:nilerr // see above
	}
	r.txs += exp.txs
	r.txHashes += uint64(len(exp.txHashes))
	if r.events != nil {
		if err := r.events.ledger(seq, exp.events); err != nil {
			return err
		}
	}
	if r.index != nil {
		if err := r.index.ledger(seq, exp.txHashes); err != nil {
			return err
		}
	}
	if r.bin != nil {
		r.bin.ledger(seq, exp.txHashes)
	}
	return nil
}

func (r *chunkRun) finish(ctx context.Context) error {
	if r.events != nil {
		if err := r.events.finish(ctx); err != nil {
			return err
		}
	}
	if r.bin != nil {
		r.bin.finish()
	}
	return r.anchor()
}

// anchor compares the chunk's first stored header hash with the network's
// history archive, so a self-consistent chain is also the right chain.
func (r *chunkRun) anchor() error {
	if r.d.archive == nil {
		return nil
	}
	seq := r.c.FirstLedger()
	entry, err := r.d.archive.GetLedgerHeader(seq)
	if err != nil {
		return fmt.Errorf("history archive header for ledger %d: %w", seq, err)
	}
	if entry.Hash != r.firstHash {
		r.rec.add(Mismatch{
			Ledger: seq, Artifact: "ledgers", Field: "archive_anchor",
			Expected: hexHash(entry.Hash), Actual: hexHash(r.firstHash),
		})
	}
	return nil
}

// indexCache resolves chunks to the frozen tx-hash index coverage containing
// them, one catalog read per index, and holds one open reader per index for
// the run.
type indexCache struct {
	cat *catalog.Catalog

	mu      sync.Mutex
	byIndex map[geometry.TxHashIndexID]*indexEntry
}

type indexEntry struct {
	cov    geometry.TxHashIndexCoverage
	frozen bool
	reader *txhash.ColdReader // opened on first use
}

func newIndexCache(cat *catalog.Catalog) *indexCache {
	return &indexCache{cat: cat, byIndex: make(map[geometry.TxHashIndexID]*indexEntry)}
}

// forChunk returns the open reader of the frozen coverage containing c, or
// covered=false when no frozen coverage holds it.
func (ic *indexCache) forChunk(c chunk.ID) (*txhash.ColdReader, bool, error) {
	ic.mu.Lock()
	defer ic.mu.Unlock()
	id := ic.cat.TxHashIndexLayout().TxHashIndexID(c)
	e, ok := ic.byIndex[id]
	if !ok {
		cov, frozen, err := ic.cat.FrozenTxHashIndex(id)
		if err != nil {
			return nil, false, err
		}
		e = &indexEntry{cov: cov, frozen: frozen}
		ic.byIndex[id] = e
	}
	if !e.frozen || c < e.cov.Lo || c > e.cov.Hi {
		return nil, false, nil
	}
	if e.reader == nil {
		reader, err := txhash.OpenColdReader(ic.cat.Layout().TxHashIndexFilePath(e.cov))
		if err != nil {
			return nil, false, err
		}
		e.reader = reader
	}
	return e.reader, true, nil
}

// opened returns the indexes the run resolved a chunk through, ascending.
func (ic *indexCache) opened() []*indexEntry {
	ic.mu.Lock()
	defer ic.mu.Unlock()
	var out []*indexEntry
	for _, e := range ic.byIndex {
		if e.reader != nil {
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].cov.Index < out[j].cov.Index })
	return out
}

func (ic *indexCache) closeAll() error {
	ic.mu.Lock()
	defer ic.mu.Unlock()
	var err error
	for _, e := range ic.byIndex {
		if e.reader != nil {
			err = errors.Join(err, e.reader.Close())
			e.reader = nil
		}
	}
	return err
}
