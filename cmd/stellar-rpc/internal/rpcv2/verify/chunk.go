package verify

import (
	"context"
	"errors"
	"fmt"
	"sort"

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
	layout := r.d.cat.Layout()
	if r.frozen.Has(geometry.KindEvents) {
		ec, err := newEventsChecker(ctx, r.rec, r.c, layout.EventsBucketDir(r.c))
		if err != nil {
			return err
		}
		r.events = ec
	}
	if idx, covered := r.d.indexes.forChunk(r.c); covered {
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

// indexCache holds the frozen tx-hash index coverages the run's chunks fall
// in, each with its reader open for the run.
type indexCache struct {
	byIndex map[geometry.TxHashIndexID]*indexEntry
}

type indexEntry struct {
	cov    geometry.TxHashIndexCoverage
	reader *txhash.ColdReader
}

// openIndexes resolves, for every index a target chunk belongs to, its unique
// frozen coverage through the catalog, and opens the coverage's reader.
func openIndexes(cat *catalog.Catalog, targets []target) (*indexCache, error) {
	ic := &indexCache{byIndex: make(map[geometry.TxHashIndexID]*indexEntry)}
	txl := cat.TxHashIndexLayout()
	for _, t := range targets {
		id := txl.TxHashIndexID(t.chunk)
		if _, seen := ic.byIndex[id]; seen {
			continue
		}
		cov, frozen, err := cat.FrozenTxHashIndex(id)
		if err != nil {
			return nil, errors.Join(err, ic.closeAll())
		}
		if !frozen {
			continue
		}
		reader, err := txhash.OpenColdReader(cat.Layout().TxHashIndexFilePath(cov))
		if err != nil {
			return nil, errors.Join(err, ic.closeAll())
		}
		ic.byIndex[id] = &indexEntry{cov: cov, reader: reader}
	}
	return ic, nil
}

// forChunk returns the reader of the frozen coverage containing c, or
// covered=false when no frozen coverage holds it.
func (ic *indexCache) forChunk(c chunk.ID) (*txhash.ColdReader, bool) {
	for _, e := range ic.byIndex {
		if c >= e.cov.Lo && c <= e.cov.Hi {
			return e.reader, true
		}
	}
	return nil, false
}

// entries returns the resolved coverages, ascending by index.
func (ic *indexCache) entries() []*indexEntry {
	out := make([]*indexEntry, 0, len(ic.byIndex))
	for _, e := range ic.byIndex {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].cov.Index < out[j].cov.Index })
	return out
}

func (ic *indexCache) closeAll() error {
	var err error
	for _, e := range ic.byIndex {
		err = errors.Join(err, e.reader.Close())
	}
	clear(ic.byIndex)
	return err
}
