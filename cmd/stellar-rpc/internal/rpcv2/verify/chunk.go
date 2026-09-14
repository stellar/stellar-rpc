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
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/ledger"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/txhash"
)

// deps is what every chunk's verification shares.
type deps struct {
	opts    Options
	cat     *catalog.Catalog
	archive headerAnchor
	indexes *indexCache
}

// headerAnchor serves the network's header for a ledger; a history archive
// in production.
type headerAnchor interface {
	GetLedgerHeader(ledger uint32) (xdr.LedgerHeaderHistoryEntry, error)
}

var _ headerAnchor = (*historyarchive.Archive)(nil)

type chunkRun struct {
	d      *deps
	c      chunk.ID
	frozen catalog.ArtifactSet
	rec    *recorder

	events *eventsChecker
	index  *indexChecker
	bin    *binChecker

	// prevHash is the hash computed over the previous ledger's header, or
	// nil when that ledger is not at hand or did not decode.
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
	// infra collects the environment failures that kept parts of the chunk
	// from being checked; the chunk is reported as errored with them, and
	// everything else is still checked.
	infra error
}

// run checks the chunk. An artifact that cannot be read because of the
// environment is set aside and reported in the chunk's error; one whose
// bytes are wrong is a verdict. Neither stops the other artifacts from being
// checked.
func (r *chunkRun) run(ctx context.Context) error {
	if r.d.opts.beforeOpen != nil {
		r.d.opts.beforeOpen(r.c)
	}
	lr, err := ledger.OpenColdReader(r.d.cat.Layout().LedgerPackPath(r.c))
	if err != nil {
		return err
	}
	defer func() { _ = lr.Close() }()
	ok, err := r.checkPack(ctx, lr)
	if err != nil || !ok {
		return err
	}
	r.anchor(lr)
	r.prevHash = r.previousChunkHash()
	r.openCheckers(ctx)
	defer r.closeCheckers()
	for entry, err := range lr.IterateLedgers(r.c.FirstLedger(), r.c.LastLedger()) {
		if err != nil {
			// The content hash already covered every record, so a failure
			// here is the environment's, or a walk past the pack's span.
			if isInfrastructure(err) {
				return errors.Join(r.infra, err)
			}
			r.rec.add(Mismatch{Ledger: entry.Seq, Artifact: "ledgers", Field: "pack", Actual: err.Error()})
			r.sourceBad = true
			break
		}
		r.ledger(entry.Seq, entry.Bytes)
	}
	if !r.sourceBad {
		r.finish(ctx)
	}
	return r.infra
}

// verdict records a failure of an artifact's own bytes as a mismatch, or
// sets it aside as an environment failure.
func (r *chunkRun) verdict(artifact, field string, err error) {
	if isInfrastructure(err) {
		r.infra = errors.Join(r.infra, fmt.Errorf("%s: %w", artifact, err))
		return
	}
	r.rec.add(Mismatch{Artifact: artifact, Field: field, Actual: err.Error()})
}

// checkPack verifies the pack's content hash and that it spans exactly the
// chunk. A missing or unreadable pack is the run's error; anything wrong
// with the bytes that are there is a verdict on the pack.
func (r *chunkRun) checkPack(ctx context.Context, lr *ledger.ColdReader) (bool, error) {
	if err := lr.Verify(ctx); err != nil {
		if isInfrastructure(err) {
			return false, err
		}
		r.rec.add(Mismatch{
			Artifact: "ledgers", Field: "content_hash", Expected: "the hash the writer stored", Actual: err.Error(),
		})
		return false, nil
	}
	first, err := lr.FirstSeq()
	if err != nil {
		if isInfrastructure(err) {
			return false, err
		}
		r.rec.add(Mismatch{Artifact: "ledgers", Field: "pack", Actual: err.Error()})
		return false, nil
	}
	last, _ := lr.LastSeq() // same cached header as FirstSeq
	if first != r.c.FirstLedger() || last != r.c.LastLedger() {
		r.rec.add(Mismatch{
			Artifact: "ledgers", Field: "span",
			Expected: fmt.Sprintf("[%d,%d]", r.c.FirstLedger(), r.c.LastLedger()), Actual: fmt.Sprintf("[%d,%d]", first, last),
		})
		return false, nil
	}
	return true, nil
}

// freshCatalog opens the catalog read-only again. The run's own handle is a
// snapshot of the catalog as it was opened, so anything a live daemon has
// committed since is visible only through a new open.
func (d *deps) freshCatalog() (*catalog.Catalog, error) {
	cat := d.cat
	return catalog.OpenReadOnly(cat.Layout().CatalogPath(), cat.Layout(), cat.TxHashIndexLayout(), cat.Logger())
}

// previousChunkHash returns the hash computed over the header of the ledger
// before this chunk's first, read from the previous chunk's frozen pack, or
// nil when there is none to read: chunk 0 follows the genesis ledger, which
// no pack holds, and a previous chunk that is absent, unfrozen, or does not
// decode is that chunk's own finding.
func (r *chunkRun) previousChunkHash() *xdr.Hash {
	if r.c == 0 {
		return nil
	}
	prev := r.c - 1
	state, err := r.d.cat.State(prev, geometry.KindLedgers)
	if err != nil {
		r.infra = errors.Join(r.infra, fmt.Errorf("previous chunk %s: %w", prev, err))
		return nil
	}
	if state != geometry.StateFrozen {
		return nil
	}
	lr, err := ledger.OpenColdReader(r.d.cat.Layout().LedgerPackPath(prev))
	if err != nil {
		r.infra = errors.Join(r.infra, fmt.Errorf("previous chunk %s: %w", prev, err))
		return nil
	}
	defer func() { _ = lr.Close() }()
	h, err := headerHash(lr, r.c.FirstLedger()-1)
	if err != nil {
		if isInfrastructure(err) {
			r.infra = errors.Join(r.infra, fmt.Errorf("previous chunk %s: %w", prev, err))
		}
		return nil
	}
	return &h
}

// openCheckers opens one checker per artifact the chunk has: its events
// segment and its .bin when those are frozen, and the tx-hash index when a
// frozen coverage contains the chunk. A chunk whose .bin was demoted after
// its index finalized has only the index; one still waiting on its index
// build has only the .bin. An artifact that fails to open is set aside or
// recorded, and the others are still checked.
func (r *chunkRun) openCheckers(ctx context.Context) {
	if r.frozen.Has(geometry.KindEvents) {
		ec, err := newEventsChecker(ctx, r.rec, r.c, r.d.cat.Layout().EventsBucketDir(r.c))
		if err != nil {
			r.verdict("events", "open", err)
		} else {
			r.events = ec
		}
	}
	r.openTxHashCheckers()
}

// openTxHashCheckers opens the .bin checker when the chunk's key was frozen
// at listing, and the index checker when a frozen coverage contains the
// chunk. A .bin that has vanished since listing means a live daemon
// finalized the chunk's index and swept the inputs; the catalog is then
// re-read fresh, and the chunk is checked through the index it now has.
func (r *chunkRun) openTxHashCheckers() {
	cat := r.d.cat
	if r.frozen.Has(geometry.KindTxHash) {
		bc, err := newBinChecker(r.rec, cat.Layout().TxHashBinPath(r.c), cat.TxHashIndexSecret(r.c))
		switch {
		case err == nil:
			r.bin = bc
		case errors.Is(err, fs.ErrNotExist):
			cov, covered, ferr := r.d.freshCoverageAfterSweep(r.c)
			switch {
			case ferr != nil:
				r.infra = errors.Join(r.infra, ferr)
			case covered:
				r.openIndex(cov)
			default:
				r.infra = errors.Join(r.infra, fmt.Errorf("txhash: %w", err))
			}
			return
		default:
			r.verdict("txhash", "bin", err)
		}
	}
	cov, covered, err := r.d.indexes.coverageOf(cat, r.c)
	switch {
	case err != nil:
		r.infra = errors.Join(r.infra, fmt.Errorf("txhash index coverage: %w", err))
	case covered:
		r.openIndex(cov)
	}
}

// openIndex opens the index of coverage cov. An index file gone since the
// run listed its coverage means a live daemon rebuilt the window and swept
// the old file; the chunk is then checked through the coverage that
// replaced it.
func (r *chunkRun) openIndex(cov geometry.TxHashIndexCoverage) {
	idx, err := r.d.indexes.reader(cov)
	if errors.Is(err, fs.ErrNotExist) {
		fresh, ferr := r.d.freshCatalog()
		if ferr != nil {
			r.infra = errors.Join(r.infra, ferr)
			return
		}
		defer func() { _ = fresh.Close() }()
		newCov, covered, cerr := r.d.indexes.coverageOf(fresh, r.c)
		switch {
		case cerr != nil:
			r.infra = errors.Join(r.infra, cerr)
			return
		case !covered || newCov.Key == cov.Key:
			r.infra = errors.Join(r.infra, fmt.Errorf("txhash index: %w", err))
			return
		}
		idx, err = r.d.indexes.reader(newCov)
	}
	if err != nil {
		r.verdict("txhash", "index", err)
		return
	}
	r.index = &indexChecker{rec: r.rec, idx: idx}
}

// freshCoverageAfterSweep re-reads the catalog and reports whether chunk c's
// .bin key is gone and a frozen coverage contains it. Both are true after a
// terminal index commit and its sweep; a key still frozen means the .bin
// really is missing.
func (d *deps) freshCoverageAfterSweep(c chunk.ID) (geometry.TxHashIndexCoverage, bool, error) {
	fresh, err := d.freshCatalog()
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

// ledger runs one ledger's source checks and, while the source holds,
// compares what the artifacts hold for it with what the oracle derives.
func (r *chunkRun) ledger(seq uint32, raw []byte) {
	var lcm xdr.LedgerCloseMeta
	if err := xdr.SafeUnmarshal(raw, &lcm); err != nil {
		r.rec.add(Mismatch{Ledger: seq, Artifact: "ledgers", Field: "decode", Actual: err.Error()})
		r.sourceBad = true
		r.prevHash = nil // the next ledger has nothing sound to chain to
		return
	}
	r.ledgers++
	computed, ok := checkLedger(r.rec, seq, &lcm, r.prevHash)
	if !ok {
		r.sourceBad = true
	}
	r.prevHash = &computed
	if r.sourceBad {
		return
	}
	exp, err := expectLedger(r.d.opts.Passphrase, &lcm)
	if err != nil {
		r.rec.add(Mismatch{Ledger: seq, Artifact: "ledgers", Field: "decode_path", Actual: err.Error()})
		r.sourceBad = true
		return
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
			r.events.stopChecking()
			r.verdict("events", "read", err)
		}
	}
	if r.index != nil {
		if err := r.index.ledger(seq, exp.txHashes); err != nil {
			r.index = nil
			r.verdict("txhash", "index", err)
		}
	}
	if r.bin != nil {
		r.bin.ledger(seq, exp.txHashes)
	}
}

func (r *chunkRun) finish(ctx context.Context) {
	if r.events != nil {
		if err := r.events.finish(ctx); err != nil {
			r.verdict("events", "finish", err)
		}
	}
	if r.bin != nil {
		r.bin.finish()
	}
}

// anchor compares the hash of the chunk's last header with the network's
// history archive. The chain authenticates backwards, each header committing
// to the one before it, so an authentic last header makes every header of
// the chunk, and everything they commit to, the network's. It runs before
// the walk so its verdict stands whatever the walk finds: with the last
// header anchored, a broken link lies before it. An archive that cannot be
// reached is set aside as the run's error; the walk still runs.
func (r *chunkRun) anchor(lr *ledger.ColdReader) {
	if r.d.archive == nil {
		return
	}
	seq := r.c.LastLedger()
	entry, err := r.d.archive.GetLedgerHeader(seq)
	if err != nil {
		r.infra = errors.Join(r.infra, fmt.Errorf("history archive header for ledger %d: %w", seq, err))
		return
	}
	stored, err := headerHash(lr, seq)
	if err != nil {
		// A last ledger that does not decode is the walk's finding.
		if isInfrastructure(err) {
			r.infra = errors.Join(r.infra, err)
		}
		return
	}
	if entry.Hash != stored {
		r.rec.add(Mismatch{
			Ledger: seq, Artifact: "ledgers", Field: "archive_anchor",
			Expected: hexHash(entry.Hash), Actual: hexHash(stored),
		})
	}
}

// headerHash decodes one stored ledger and hashes its header.
func headerHash(lr *ledger.ColdReader, seq uint32) (xdr.Hash, error) {
	var h xdr.Hash
	err := lr.WithLedger(seq, func(raw []byte) error {
		var lcm xdr.LedgerCloseMeta
		if err := xdr.SafeUnmarshal(raw, &lcm); err != nil {
			return err
		}
		entry := lcm.LedgerHeaderHistoryEntry()
		var err error
		h, err = xdr.HashXdr(&entry.Header)
		return err
	})
	if err != nil {
		return xdr.Hash{}, fmt.Errorf("ledger %d header: %w", seq, err)
	}
	return h, nil
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
