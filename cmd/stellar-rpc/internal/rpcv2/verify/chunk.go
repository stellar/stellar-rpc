package verify

import (
	"cmp"
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
	// heldIndex is the cached index coverage this chunk acquired, if any, and
	// must release when it finishes.
	heldIndex string

	// prevHash is the hash computed over the previous ledger's header, or
	// nil when that ledger is not at hand or did not decode.
	prevHash *xdr.Hash
	ledgers  uint32
	txs      uint64
	txHashes uint64
	// invokes counts the successful invocations whose committed events were
	// checked; invokesUnchecked those the export does not let the verifier
	// recover.
	invokes          uint64
	invokesUnchecked uint64

	// What became of each comparison, recorded where the cause is known and
	// judged once by outcomes when the run is over. A checker that opened
	// answers for itself; these cover the ones that did not, and the
	// comparisons no checker makes.
	walked     bool   // the walk reached the pack's last ledger
	ledgersWhy string // or why it stopped short
	chained    bool   // the first header was compared with the predecessor's last
	chainWhy   string
	anchored   bool // the last header was compared with the archive's
	archiveWhy string
	eventsWhy  string // why no events checker opened
	binWhy     string // why the .bin could not be used
	indexWhy   string // why the tx-hash index could not be used
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
		r.ledgersWhy = "the ledger pack would not open: " + err.Error()
		return err
	}
	defer func() { _ = lr.Close() }()
	ok, err := r.checkPack(ctx, lr)
	if err != nil || !ok {
		// A pack that failed its own hash or span is a bad source, and its
		// ledgers were never walked. One that could not be read at all is the
		// environment's fault, not a verdict.
		r.sourceBad = !ok
		if err != nil {
			r.ledgersWhy = "the pack could not be read: " + err.Error()
		} else {
			r.ledgersWhy = "the pack failed its own checks, so its ledgers were not walked"
		}
		return err
	}
	r.anchor(lr)
	r.prevHash = r.previousChunkHash()
	r.openCheckers(ctx)
	defer r.closeCheckers()
	for entry, err := range lr.IterateLedgers(r.c.FirstLedger(), r.c.LastLedger()) {
		// IterateLedgers takes no context of its own, so without this a
		// canceled run decodes a whole chunk per in-flight worker before it
		// notices. Checking here also stops the derived-artifact checkers
		// being fed a partial pass.
		if cerr := ctx.Err(); cerr != nil {
			return errors.Join(r.infra, cerr)
		}
		if err != nil {
			// The content hash already covered every record, so a failure
			// here is the environment's, or a walk past the pack's span.
			if isInfrastructure(err) {
				return errors.Join(r.infra, err)
			}
			r.rec.add(Mismatch{Ledger: entry.Seq, Artifact: "ledgers", Field: "pack", Actual: err.Error()})
			r.ledgersWhy = fmt.Sprintf("the pack stopped yielding ledgers at %d: %v", entry.Seq, err)
			r.sourceBad = true
			return r.infra
		}
		r.ledger(entry.Seq, entry.Bytes)
	}
	r.walked = true
	// finish compares chunk-wide totals (event and term counts, the .bin key
	// count). After a canceled walk those totals come from a partial pass, so
	// running it would report differences that describe how far the run got
	// rather than the data.
	if !r.sourceBad && ctx.Err() == nil {
		r.finish(ctx)
	}
	return r.infra
}

// outcomes judges every comparison once the run is over. A checker that
// opened answers for itself, but only over a whole walk of a source the run
// still trusts; otherwise what stopped the walk is the reason. A comparison
// no checker made carries the reason recorded where it was decided.
func (r *chunkRun) outcomes(canceled bool) checkSet {
	var s checkSet
	s[checkLedgers] = judged(r.walked, r.ledgersWhy)
	s[checkChain] = judged(r.chained, r.chainWhy)
	s[checkArchive] = judged(r.anchored, r.archiveWhy)
	derived := r.walked && !r.sourceBad
	switch {
	case r.events == nil:
		s[checkEvents] = outcome{Why: r.eventsWhy}
	case derived:
		s[checkEvents] = r.events.outcome()
	}
	switch {
	case r.index == nil && r.bin == nil:
		s[checkTxHashes] = outcome{Why: cmp.Or(r.binWhy, r.indexWhy,
			"the chunk has neither a frozen .bin nor a frozen index coverage")}
	case derived:
		s[checkTxHashes] = r.txHashOutcome()
	}
	switch {
	case canceled:
		s.unexplained("the run was canceled before it got there")
	case !r.walked:
		s.unexplained("the chunk's ledgers were not walked to the end, so nothing derived from them was compared")
	case r.sourceBad:
		s.unexplained("the chunk's own ledgers did not check out, so nothing derived from them was compared")
	}
	return s
}

func judged(ran bool, why string) outcome {
	if why != "" {
		return outcome{Why: why}
	}
	return outcome{Ran: ran}
}

// txHashOutcome judges the tx-hash comparison when at least one checker
// opened. Either artifact satisfies it on its own: they hold the same hashes,
// so the .bin stopping at the cap costs no coverage when an index resolved
// them all.
func (r *chunkRun) txHashOutcome() outcome {
	switch {
	case r.index != nil:
		return outcome{Ran: true}
	case r.bin.gap() != "":
		return outcome{Why: r.bin.gap()}
	}
	return outcome{Ran: true}
}

// verdict records a failure of an artifact's own bytes as a mismatch, or
// sets it aside as an environment failure. seq is the ledger the failure
// happened on, or 0 for one that belongs to the chunk as a whole.
func (r *chunkRun) verdict(seq uint32, artifact, field string, err error) {
	if isInfrastructure(err) {
		r.infra = errors.Join(r.infra, fmt.Errorf("%s: %w", artifact, err))
		return
	}
	r.rec.add(Mismatch{Ledger: seq, Artifact: artifact, Field: field, Actual: err.Error()})
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
	first, last, err := lr.Span()
	if err != nil {
		if isInfrastructure(err) {
			return false, err
		}
		r.rec.add(Mismatch{Artifact: "ledgers", Field: "pack", Actual: err.Error()})
		return false, nil
	}
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
// before this chunk's first, read from the previous chunk's frozen pack. That
// link is what carries a chunk's authenticity across the boundary.
//
// nil with no finding when there is nothing to read: chunk 0 follows the
// genesis ledger, and an unfrozen predecessor has no pack. A predecessor that
// IS frozen and will not yield its last header is a verdict on that pack —
// without it a bounded run has nothing tying its chunks to the rest of
// history.
func (r *chunkRun) previousChunkHash() *xdr.Hash {
	if r.c == 0 {
		r.chainWhy = "the first chunk of the history follows the genesis ledger, which no pack holds"
		return nil
	}
	prev := r.c - 1
	state, err := r.d.cat.State(prev, geometry.KindLedgers)
	if err != nil {
		r.infra = errors.Join(r.infra, fmt.Errorf("previous chunk %s: %w", prev, err))
		r.chainWhy = fmt.Sprintf("the catalog would not answer for chunk %s: %v", prev, err)
		return nil
	}
	if state != geometry.StateFrozen {
		r.chainWhy = fmt.Sprintf("chunk %s is not frozen, so it has no pack to read", prev)
		return nil
	}
	lr, err := ledger.OpenColdReader(r.d.cat.Layout().LedgerPackPath(prev))
	if err != nil {
		r.infra = errors.Join(r.infra, fmt.Errorf("previous chunk %s: %w", prev, err))
		r.chainWhy = fmt.Sprintf("chunk %s's pack would not open: %v", prev, err)
		return nil
	}
	defer func() { _ = lr.Close() }()
	seq := r.c.FirstLedger() - 1
	h, err := headerHash(lr, seq)
	if err != nil {
		r.chainWhy = fmt.Sprintf("chunk %s's last header would not come back out of it: %v", prev, err)
		if isInfrastructure(err) {
			r.infra = errors.Join(r.infra, fmt.Errorf("previous chunk %s: %w", prev, err))
			return nil
		}
		r.rec.add(Mismatch{
			Ledger: seq, Artifact: "ledgers", Field: "previous_chunk_hash",
			Expected: fmt.Sprintf("the last header of chunk %s, which is frozen", prev),
			Actual:   err.Error(),
		})
		return nil
	}
	return &h
}

// openCheckers opens one checker per artifact the chunk has: its events
// segment when that is frozen, and its tx-hash artifacts. An artifact that
// fails to open is set aside or recorded, and the others are still checked.
func (r *chunkRun) openCheckers(ctx context.Context) {
	if !r.frozen.Has(geometry.KindEvents) {
		r.eventsWhy = "the catalog names no frozen events artifact for it"
	} else if ec, err := newEventsChecker(ctx, r.rec, r.c, r.d.cat.Layout().EventsColdDirs(r.c)); err != nil {
		r.verdict(0, "events", "open", err)
		r.eventsWhy = "the events segment would not open: " + err.Error()
	} else {
		r.events = ec
	}
	r.openTxHashCheckers()
}

// openTxHashCheckers opens what the catalog names for the chunk's tx hashes:
// its .bin while that key is frozen, and the frozen index coverage containing
// it. A chunk whose .bin was demoted after its index finalized has only the
// index; one still waiting on its index build has only the .bin.
//
// The run's catalog handle is a snapshot, so a live daemon may have finalized
// the chunk's index and swept its .bin, or rebuilt the window, since the
// listing. A named file that is gone is resolved once more against a fresh
// view, and the chunk is checked through what replaced it.
func (r *chunkRun) openTxHashCheckers() {
	err := r.openTxHashSources(r.d.cat)
	if errors.Is(err, fs.ErrNotExist) {
		fresh, ferr := r.d.freshCatalog()
		if ferr != nil {
			r.infra = errors.Join(r.infra, ferr)
			return
		}
		defer func() { _ = fresh.Close() }()
		err = r.openTxHashSources(fresh)
	}
	if err != nil {
		r.infra = errors.Join(r.infra, err)
	}
}

// openTxHashSources opens the tx-hash artifacts cat names for the chunk that
// are not open yet. A named file that is missing is returned, wrapping
// fs.ErrNotExist; a file that is there but wrong is a verdict, recorded here.
func (r *chunkRun) openTxHashSources(cat *catalog.Catalog) error {
	state, err := cat.State(r.c, geometry.KindTxHash)
	if err != nil {
		return fmt.Errorf("txhash state: %w", err)
	}
	var missing error
	if state == geometry.StateFrozen && r.bin == nil && r.binWhy == "" {
		bc, err := newBinChecker(r.rec, cat.Layout().TxHashBinPath(r.c), cat.TxHashIndexSecret(r.c))
		switch {
		case err == nil:
			r.bin = bc
		case errors.Is(err, fs.ErrNotExist):
			missing = fmt.Errorf("txhash: %w", err)
		default:
			r.verdict(0, "txhash", "bin", err)
			r.binWhy = "the .bin would not parse: " + err.Error()
		}
	}
	if r.index != nil || r.indexWhy != "" {
		return missing
	}
	cov, covered, err := r.d.indexes.coverageOf(cat, r.c)
	switch {
	case err != nil:
		return errors.Join(missing, fmt.Errorf("txhash index coverage: %w", err))
	case covered:
		if err := r.openIndex(cov); err != nil {
			missing = errors.Join(missing, fmt.Errorf("txhash index: %w", err))
		}
	}
	return missing
}

// openIndex opens the index of coverage cov and checks that the file is the
// one the catalog means. A missing file is returned; anything wrong with
// the file that is there is a verdict.
func (r *chunkRun) openIndex(cov geometry.TxHashIndexCoverage) error {
	idx, err := r.d.indexes.acquire(cov)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return err
		}
		r.verdict(0, "txhash", "index", err)
		r.indexWhy = "the tx-hash index would not open: " + err.Error()
		return nil
	}
	r.heldIndex = cov.Key
	// The index names its own routing secret and keys every lookup with it,
	// so a file built under a different master secret — or copied in from
	// another data tree — resolves perfectly and answers about the wrong
	// chunks. Comparing it with the secret this catalog derives is the only
	// way to notice, and a difference is a verdict on the file.
	if want := r.d.cat.TxHashIndexSecret(r.c); idx.Secret() != want {
		r.rec.add(Mismatch{
			Artifact: "txhash", Field: "index_secret",
			Expected: fmt.Sprintf("the secret this catalog derives for %s", cov.Index),
			Actual:   "the index file declares a different one",
		})
		r.indexWhy = "the tx-hash index declares a secret this catalog did not derive"
		return nil
	}
	// The file is chosen by the coverage in its name; its own bounds must
	// agree, or an empty index under the wrong name would pass every lookup
	// it is never asked.
	if lo, hi := cov.Lo.FirstLedger(), cov.Hi.LastLedger(); idx.MinLedger() != lo || idx.MaxLedger() != hi {
		r.rec.add(Mismatch{
			Artifact: "txhash", Field: "index_span",
			Expected: fmt.Sprintf("[%d,%d]", lo, hi), Actual: fmt.Sprintf("[%d,%d]", idx.MinLedger(), idx.MaxLedger()),
		})
		r.indexWhy = "the tx-hash index spans different ledgers than its coverage names"
		return nil
	}
	r.index = &indexChecker{rec: r.rec, idx: idx}
	return nil
}

// releaseIndex drops this chunk's hold on the index it acquired, if any.
func (r *chunkRun) releaseIndex() error {
	if r.heldIndex == "" {
		return nil
	}
	key := r.heldIndex
	r.heldIndex = ""
	return r.d.indexes.release(key)
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
		if seq == r.c.FirstLedger() && r.prevHash != nil {
			r.chainWhy = "the chunk's first ledger would not decode, " +
				"so the previous chunk's last header was compared with nothing"
		}
		r.sourceBad = true
		r.prevHash = nil // the next ledger has nothing sound to chain to
		return
	}
	r.ledgers++
	if seq == r.c.FirstLedger() && r.prevHash != nil {
		// Recorded here rather than where the hash was fetched: a run
		// canceled in between never reaches the comparison.
		r.chained = true
	}
	computed, ok := checkLedger(r.rec, seq, &lcm, r.prevHash)
	if !ok {
		r.sourceBad = true
	}
	r.prevHash = computed
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
	r.invocations(seq, exp.invokes)
	if r.events != nil {
		if err := r.events.ledger(seq, exp.events); err != nil {
			r.events.stopChecking()
			r.verdict(seq, "events", "read", err)
		}
	}
	if r.index != nil {
		if err := r.index.ledger(seq, exp.txHashes); err != nil {
			r.index = nil
			r.verdict(seq, "txhash", "index", err)
			r.indexWhy = fmt.Sprintf("the tx-hash index stopped answering at ledger %d: %v", seq, err)
		}
	}
	if r.bin != nil {
		r.bin.ledger(seq, exp.txHashes)
	}
}

// invocations records the successful Soroban invocations of one ledger whose
// events do not hash to what their result committed to. An invocation the
// export gave no way to check is counted apart: that is a limit of the
// export, not a verdict on the data.
func (r *chunkRun) invocations(seq uint32, checks []invokeCheck) {
	for i := range checks {
		c := &checks[i]
		if c.skipped != "" {
			r.invokesUnchecked++
			continue
		}
		r.invokes++
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
}

func (r *chunkRun) finish(ctx context.Context) {
	if r.events != nil {
		if err := r.events.finish(ctx); err != nil {
			r.verdict(0, "events", "finish", err)
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
		r.archiveWhy = "no history archive was given"
		return
	}
	seq := r.c.LastLedger()
	entry, err := r.d.archive.GetLedgerHeader(seq)
	if err != nil {
		r.infra = errors.Join(r.infra, fmt.Errorf("history archive header for ledger %d: %w", seq, err))
		r.archiveWhy = "the archive would not serve the chunk's last header: " + err.Error()
		return
	}
	stored, err := headerHash(lr, seq)
	if err != nil {
		// A last ledger that does not decode is the walk's finding.
		if isInfrastructure(err) {
			r.infra = errors.Join(r.infra, err)
		}
		r.archiveWhy = "the chunk's own last header would not decode: " + err.Error()
		return
	}
	r.anchored = true
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

// indexEntry is one coverage's cached index. The reader is mmap-backed, so it
// is released as soon as the last chunk using it finishes rather than held
// for the whole run: a full-history run resolves every coverage, and holding
// them all open maps the entire index tree at once. keyCount is snapshotted
// at open because the report needs it after the reader is gone.
type indexEntry struct {
	cov    geometry.TxHashIndexCoverage
	reader *txhash.ColdReader // nil once every user has released it
	// keyCount is snapshotted at open so the report can read it after the
	// cache closed the reader.
	keyCount uint64
	refs     int
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

// acquire returns cov's reader, opening it on first use and counting the
// caller as a user of it. Every acquire must be matched by a release.
func (ic *indexCache) acquire(cov geometry.TxHashIndexCoverage) (*txhash.ColdReader, error) {
	ic.mu.Lock()
	defer ic.mu.Unlock()
	if e, ok := ic.readers[cov.Key]; ok && e.reader != nil {
		e.refs++
		return e.reader, nil
	}
	reader, err := txhash.OpenColdReader(ic.layout.TxHashIndexFilePath(cov))
	if err != nil {
		return nil, err
	}
	// Reuse an existing entry: a coverage can be reopened if the schedule ever
	// interleaves.
	e, ok := ic.readers[cov.Key]
	if !ok {
		e = &indexEntry{cov: cov}
		ic.readers[cov.Key] = e
	}
	e.reader, e.keyCount, e.refs = reader, reader.KeyCount(), e.refs+1
	return reader, nil
}

// release drops one user of cov and closes the mapping when the last one
// goes. The entry itself stays: the run's report reads keyCount from it after
// every chunk has finished.
func (ic *indexCache) release(key string) error {
	ic.mu.Lock()
	defer ic.mu.Unlock()
	e, ok := ic.readers[key]
	if !ok || e.reader == nil {
		return nil
	}
	if e.refs--; e.refs > 0 {
		return nil
	}
	reader := e.reader
	e.reader = nil
	return reader.Close()
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

// closeAll releases any mapping still open; every chunk releases its own
// hold, so normally there is none.
func (ic *indexCache) closeAll() error {
	ic.mu.Lock()
	defer ic.mu.Unlock()
	var err error
	for _, e := range ic.readers {
		if e.reader == nil {
			continue
		}
		err = errors.Join(err, e.reader.Close())
		e.reader, e.refs = nil, 0
	}
	return err
}
