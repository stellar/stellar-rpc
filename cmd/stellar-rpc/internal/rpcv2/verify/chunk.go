package verify

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"sync"

	"github.com/stellar/go-stellar-sdk/historyarchive"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/packfile"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
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

	prevHash *xdr.Hash
	ledgers  uint32
	txs      uint64
	txHashes uint64
	invokes  uint64
	// invokesUnchecked counts invocations whose committed events the export
	// does not let the verifier recover.
	invokesUnchecked uint64
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
// chunk. A hash mismatch or a corrupt file is a verdict on the pack; any
// other failure, a missing or unreadable file, is the run's error.
func (r *chunkRun) checkPack(ctx context.Context, lr *ledger.ColdReader) (bool, error) {
	if err := lr.Verify(ctx); err != nil {
		if !errors.Is(err, packfile.ErrContentHashMismatch) && !errors.Is(err, stores.ErrCorrupt) {
			return false, err
		}
		r.rec.add(Mismatch{
			Artifact: "ledgers", Field: "content_hash", Expected: "the hash the writer stored", Actual: err.Error(),
		})
		return false, nil
	}
	last, err := lr.LastSeq()
	if err != nil {
		if !errors.Is(err, stores.ErrCorrupt) {
			return false, err
		}
		r.rec.add(Mismatch{Artifact: "ledgers", Field: "pack", Actual: err.Error()})
		return false, nil
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
		var lcm xdr.LedgerCloseMeta
		if err := xdr.SafeUnmarshal(raw, &lcm); err != nil {
			return err
		}
		entry := lcm.LedgerHeaderHistoryEntry()
		h, err = xdr.HashXdr(&entry.Header)
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
	if r.d.opts.beforeOpen != nil {
		r.d.opts.beforeOpen(r.c)
	}
	if r.frozen.Has(geometry.KindEvents) {
		ec, err := newEventsChecker(ctx, r.rec, r.c, r.d.cat.Layout().EventsBucketDir(r.c))
		if err != nil {
			return err
		}
		r.events = ec
	}
	if err := r.openTxHashCheckers(); err != nil {
		r.closeCheckers()
		return err
	}
	return nil
}

// openTxHashCheckers opens the .bin checker when the chunk's key was frozen
// at listing, and the index checker when a frozen coverage contains the
// chunk. A .bin that has vanished since listing means a live daemon
// finalized the chunk's index and swept the inputs; the catalog is then
// re-read fresh, and the chunk is checked through the index it now has.
func (r *chunkRun) openTxHashCheckers() error {
	cat := r.d.cat
	if r.frozen.Has(geometry.KindTxHash) {
		bc, err := newBinChecker(r.rec, cat.Layout().TxHashBinPath(r.c), cat.TxHashIndexSecret(r.c))
		switch {
		case err == nil:
			r.bin = bc
		case errors.Is(err, fs.ErrNotExist):
			cov, covered, ferr := r.d.freshCoverageAfterSweep(r.c)
			if ferr != nil {
				return ferr
			}
			if !covered {
				return err
			}
			return r.openIndex(cov)
		default:
			return err
		}
	}
	cov, covered, err := r.d.indexes.coverageOf(cat, r.c)
	if err != nil || !covered {
		return err
	}
	return r.openIndex(cov)
}

func (r *chunkRun) openIndex(cov geometry.TxHashIndexCoverage) error {
	idx, err := r.d.indexes.reader(cov)
	if err != nil {
		return err
	}
	r.index = &indexChecker{rec: r.rec, idx: idx}
	return nil
}

// freshCoverageAfterSweep re-reads the catalog through a new read-only open,
// since the run's own handle is a snapshot of the catalog as it was opened,
// and reports whether chunk c's .bin key is gone and a frozen coverage
// contains it. Both are true after a terminal index commit and its sweep;
// a key still frozen means the .bin really is missing.
func (d *deps) freshCoverageAfterSweep(c chunk.ID) (geometry.TxHashIndexCoverage, bool, error) {
	cat := d.cat
	fresh, err := catalog.OpenReadOnly(cat.Layout().CatalogPath(), cat.Layout(), cat.TxHashIndexLayout(), cat.Logger())
	if err != nil {
		return geometry.TxHashIndexCoverage{}, false, fmt.Errorf("re-read catalog: %w", err)
	}
	defer func() { _ = fresh.Close() }()
	state, err := fresh.State(c, geometry.KindTxHash)
	if err != nil || state == geometry.StateFrozen {
		return geometry.TxHashIndexCoverage{}, false, err
	}
	return d.indexes.coverageOf(fresh, c)
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
	if !checkLedger(r.rec, seq, &lcm, r.prevHash) {
		r.sourceBad = true
	}
	h := lcm.LedgerHeaderHistoryEntry().Hash
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
	r.invokes += uint64(len(exp.invokes))
	for _, c := range exp.invokes {
		if c.skipped != "" {
			r.invokesUnchecked++
			continue
		}
		if c.ok() {
			continue
		}
		actual := c.reason
		if actual == "" {
			actual = hexHash(c.got)
		}
		r.rec.add(Mismatch{
			Ledger: seq, TxHash: c.txHash.HexString(), Artifact: "ledgers",
			Field: fmt.Sprintf("invoke_success_hash (op %d)", c.opIdx), Expected: hexHash(c.want), Actual: actual,
		})
	}
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

// anchor compares the chunk's last header hash with the network's history
// archive. The chain authenticates backwards, each header committing to the
// one before it, so an authentic last header makes every header of the
// chunk, and everything they commit to, the network's.
func (r *chunkRun) anchor() error {
	if r.d.archive == nil {
		return nil
	}
	seq := r.c.LastLedger()
	entry, err := r.d.archive.GetLedgerHeader(seq)
	if err != nil {
		return fmt.Errorf("history archive header for ledger %d: %w", seq, err)
	}
	if entry.Hash != *r.prevHash {
		r.rec.add(Mismatch{
			Ledger: seq, Artifact: "ledgers", Field: "archive_anchor",
			Expected: hexHash(entry.Hash), Actual: hexHash(*r.prevHash),
		})
	}
	return nil
}

// indexCache holds one open reader per frozen tx-hash index coverage the
// run resolves a chunk through.
type indexCache struct {
	layout  geometry.Layout
	mu      sync.Mutex
	readers map[string]*indexEntry // by coverage key
}

type indexEntry struct {
	cov    geometry.TxHashIndexCoverage
	reader *txhash.ColdReader
}

func newIndexCache(layout geometry.Layout) *indexCache {
	return &indexCache{layout: layout, readers: make(map[string]*indexEntry)}
}

// coverageOf returns the unique frozen coverage of chunk c's index as cat
// records it, or covered=false when none contains c.
func (ic *indexCache) coverageOf(cat *catalog.Catalog, c chunk.ID) (geometry.TxHashIndexCoverage, bool, error) {
	cov, frozen, err := cat.FrozenTxHashIndex(cat.TxHashIndexLayout().TxHashIndexID(c))
	if err != nil || !frozen || c < cov.Lo || c > cov.Hi {
		return geometry.TxHashIndexCoverage{}, false, err
	}
	return cov, true, nil
}

// reader returns cov's reader, opened on first use.
func (ic *indexCache) reader(cov geometry.TxHashIndexCoverage) (*txhash.ColdReader, error) {
	ic.mu.Lock()
	defer ic.mu.Unlock()
	if e, ok := ic.readers[cov.Key]; ok {
		return e.reader, nil
	}
	reader, err := txhash.OpenColdReader(ic.layout.TxHashIndexFilePath(cov))
	if err != nil {
		return nil, err
	}
	ic.readers[cov.Key] = &indexEntry{cov: cov, reader: reader}
	return reader, nil
}

// entries returns the resolved coverages, ascending by index.
func (ic *indexCache) entries() []*indexEntry {
	ic.mu.Lock()
	defer ic.mu.Unlock()
	out := make([]*indexEntry, 0, len(ic.readers))
	for _, e := range ic.readers {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].cov.Index < out[j].cov.Index })
	return out
}

func (ic *indexCache) closeAll() error {
	ic.mu.Lock()
	defer ic.mu.Unlock()
	var err error
	for _, e := range ic.readers {
		err = errors.Join(err, e.reader.Close())
	}
	clear(ic.readers)
	return err
}
