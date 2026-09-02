package txhash

// cold_freeze.go — the txhash half of the zero-decompression freeze: build a
// completed hot chunk's cold .bin DIRECTLY from its durable hot state, as a
// pure STREAM. The packed-row tier seals its runs already blinded and
// key-sorted (blind-at-seal, hotindex_seal.go), i.e. holding the .bin's own
// records, so the .bin's order is recovered by a k-way merge over
// already-sorted sources: the manifest-listed sealed runs (20-byte blinded
// records, CRC64-framed) copied verbatim, plus each un-sealed tail row —
// raw in the CF — blinded and sorted into its own small merge source. No
// whole-chunk accumulator and no whole-chunk sort: freeze RAM here is the
// merge's cursors plus one tail row's keys, and the merge output streams
// straight into the .bin writer. Orphan run files (a crash between run write
// and manifest Put) are ignored by construction: only manifest-named runs
// feed the merge, and the CF rows past the sealed frontier cover everything
// an orphan holds — freeze output is identical from every crash state.
//
// Two byte-discipline rules, unchanged from the CF-scan freeze this
// replaces:
//   - truncation to ColdKeySize happens exactly once, inside RoutingKey
//     (cold_format.go) — the one place any producer blinds;
//   - sequences are decoded and re-encoded (BE row key / LE run record →
//     LE .bin), never memcpy'd.
//
// Duplicate emission is VERBATIM — equal blinded keys merge in ledger order
// (run-then-tail source order) with no dedup and no error: byte parity with
// the walk path is the freeze's gate, and the downstream streamhash build
// already rejects a duplicate-key collision loudly (cold_bin.go). An
// intra-ledger duplicate was rejected at write time and cannot reach here.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

// freezeCtxPollEvery is how many entries the merge emits between context
// checks. Entries are ~20 bytes of work apiece, so a coarse cadence still
// cancels promptly.
const freezeCtxPollEvery = 4096

// FreezeColdFromStore builds the chunk's cold txhash .bin at binPath from
// the chunk's hot store (read-only opens included — nothing here needs a
// warmed facade, and nothing is mutated). The merge streams straight into
// the walk path's own writer (coldBinStream, which WriteColdBin loops a
// whole slice over), so the byte-parity gate's only remaining subject is
// entry ORDER — and both paths sort through SortColdEntries' comparator.
// Returns the entries written.
//
// secret is the chunk's per-index routing secret: every stored key is
// RoutingKey(secret, hash) — the walk writer's rule. The
// sealed runs were keyed with the secret the chunk DB adopted at its first
// read-write open, so a caller's secret that disagrees with the persisted
// one would silently mix two keyspaces into one .bin; requireStoreSecret
// rejects that before a byte is written.
func FreezeColdFromStore(
	ctx context.Context,
	chunkID chunk.ID,
	store *rocksdb.Store,
	binPath string,
	secret [stores.SecretLen]byte,
) (int, error) {
	if err := os.MkdirAll(filepath.Dir(binPath), 0o755); err != nil {
		return 0, fmt.Errorf("txhash freeze %s: mkdir: %w", chunkID, err)
	}
	if err := requireStoreSecret(store, secret); err != nil {
		return 0, fmt.Errorf("txhash freeze %s: %w", chunkID, err)
	}
	sources, err := openFreezeSources(ctx, store, chunkID, secret)
	if err != nil {
		return 0, err
	}
	defer closeFreezeSources(sources)
	w, err := newColdBinStream(binPath, secret)
	if err != nil {
		return 0, err
	}
	defer w.close()
	n, err := mergeFreezeSources(ctx, chunkID, sources, w)
	if err != nil {
		return 0, err
	}
	if ferr := w.commit(); ferr != nil {
		return 0, ferr
	}
	return n, nil
}

// openFreezeSources assembles the merge inputs in tie-break order: the
// manifest-listed sealed runs (oldest first), then the un-sealed tail rows
// (ledger order) — so equal keys emit in global ledger order.
//
// It re-validates the manifest's run CHAIN while opening, because that chain
// is what the tie-break order MEANS. mergeFreezeSources emits equal keys by
// (key, source index) and calls the result (key, ledger); the two are the
// same order only while source index rises with ledger — i.e. while each run
// starts where the previous ended and the newest ends at the sealed frontier.
// OpenHotIndex enforces exactly that, but a production freeze never runs it:
// it opens through OpenReadyView → OpenReadOnly, which warms nothing. Without
// the check here, a manifest whose runs were listed out of order would
// silently produce a .bin whose duplicate-key order diverges from the walk
// path's — surfacing much later as a streamhash duplicate-key build failure
// or a byte-parity gate miss, with nothing pointing at the manifest.
func openFreezeSources(
	ctx context.Context, store *rocksdb.Store, chunkID chunk.ID, secret [stores.SecretLen]byte,
) ([]freezeSource, error) {
	names, lastSealed, err := rocksdbManifest{store: store}.GetRuns()
	if err != nil {
		return nil, fmt.Errorf("txhash freeze %s: manifest: %w", chunkID, err)
	}
	runDir := filepath.Join(store.Path(), txhashRunDir)
	sources := make([]freezeSource, 0, len(names))
	var prev runHeader
	for i, name := range names {
		rs, oerr := openRunSource(filepath.Join(runDir, name))
		if oerr != nil {
			closeFreezeSources(sources)
			return nil, fmt.Errorf("txhash freeze %s: %w", chunkID, oerr)
		}
		// Append BEFORE the chain check so the cleanup below covers this
		// handle too, however the check exits.
		sources = append(sources, rs)
		if i > 0 && rs.hdr.first != prev.last+1 {
			closeFreezeSources(sources)
			return nil, fmt.Errorf(
				"txhash freeze %s: manifest run %s starts at %d, previous run ends at %d "+
					"(the merge's source order IS its ledger order — see openFreezeSources)",
				chunkID, name, rs.hdr.first, prev.last)
		}
		prev = rs.hdr
	}
	if len(names) > 0 && prev.last != lastSealed {
		closeFreezeSources(sources)
		return nil, fmt.Errorf(
			"txhash freeze %s: newest manifest run ends at %d, sealed frontier is %d "+
				"(the tail scan would re-cover or skip rows)", chunkID, prev.last, lastSealed)
	}
	tails, terr := collectTailSources(ctx, store, chunkID, lastSealed, secret)
	if terr != nil {
		closeFreezeSources(sources)
		return nil, terr
	}
	return append(sources, tails...), nil
}

// collectTailSources copies every un-sealed packed row (ledgers past the
// sealed frontier) out of the CF as its own merge source, enforcing the
// same properties warmup does: 4-byte keys (a longer key is the stale
// pre-release format), a dense chain from the frontier, and EncodeRow
// shape. Values are copied — the iterator buffer is borrowed.
func collectTailSources(
	ctx context.Context, store *rocksdb.Store, chunkID chunk.ID, lastSealed uint32,
	secret [stores.SecretLen]byte,
) ([]freezeSource, error) {
	expected := max(lastSealed+1, chunkID.FirstLedger())
	var sources []freezeSource
	for entry, ierr := range store.IterateRange(txhashCF, rocksdb.EncodeUint32(expected), nil) {
		if ierr != nil {
			return nil, fmt.Errorf("txhash freeze %s: scan %s: %w", chunkID, txhashCF, ierr)
		}
		// Per row, like the events twin's tail scan: len(sources) counts
		// APPENDED sources, not iterations, so it cannot drive pollCtx's
		// cadence (tx-less ledgers never bump it).
		if cerr := ctx.Err(); cerr != nil {
			return nil, cerr
		}
		if len(entry.Key) != rowKeyLen {
			return nil, fmt.Errorf("txhash freeze %s: %d-byte key in %s: stale pre-release txhash format",
				chunkID, len(entry.Key), txhashCF)
		}
		seq := rocksdb.DecodeUint32(entry.Key)
		if seq != expected {
			return nil, fmt.Errorf("txhash freeze %s: tail row %d breaks the dense chain (want %d)",
				chunkID, seq, expected)
		}
		expected++
		if _, verr := validateRow(entry.Value); verr != nil {
			return nil, fmt.Errorf("txhash freeze %s: tail row %d: %w", chunkID, seq, verr)
		}
		if len(entry.Value) == 0 {
			continue // tx-less ledger: nothing to merge
		}
		sources = append(sources, newTailRowSource(entry.Value, seq, secret))
	}
	return sources, nil
}

// mergeFreezeSources runs the k-way merge, appending each record to w as a
// cold entry — key already blinded and truncated by its source — with the
// seq range-checked against the chunk. Returns the entry count written.
func mergeFreezeSources(
	ctx context.Context, chunkID chunk.ID, sources []freezeSource, w *coldBinStream,
) (int, error) {
	first, last := chunkID.FirstLedger(), chunkID.LastLedger()
	h, err := newFreezeHeap(sources)
	if err != nil {
		return 0, fmt.Errorf("txhash freeze %s: %w", chunkID, err)
	}
	n := 0
	for h.len() > 0 {
		if cerr := pollCtx(ctx, n); cerr != nil {
			return 0, cerr
		}
		key, seq := h.min()
		if seq < first || seq > last {
			return 0, fmt.Errorf("txhash freeze %s: entry seq %d outside [%d, %d]", chunkID, seq, first, last)
		}
		// Copy the key out of the source's buffer BEFORE the step
		// invalidates it.
		if aerr := w.append(ColdEntry{Key: [ColdKeySize]byte(key), Seq: seq}); aerr != nil {
			return 0, aerr
		}
		n++
		if serr := h.step(); serr != nil {
			return 0, fmt.Errorf("txhash freeze %s: %w", chunkID, serr)
		}
	}
	return n, nil
}

// pollCtx checks ctx once every freezeCtxPollEvery iterations.
func pollCtx(ctx context.Context, i int) error {
	if i%freezeCtxPollEvery == 0 {
		return ctx.Err()
	}
	return nil
}

// ─────────────────────────── merge sources ───────────────────────────

// freezeSource is one key-sorted stream of (blinded key, seq) records
// feeding the freeze merge: a sealed run file (runSource, run_reader.go) or
// a single un-sealed tail row. Sources start positioned BEFORE their first
// record; advance steps to the next one.
type freezeSource interface {
	// key returns the current record's blinded routing key — valid only
	// until the next advance.
	key() []byte
	// seq returns the current record's ledger sequence.
	seq() uint32
	// advance steps to the next record, reporting whether one is now
	// current. (false, nil) is clean end-of-stream — for a run source that
	// includes the whole-payload CRC64 verification passing.
	advance() (bool, error)
	// close releases resources; safe in any state.
	close()
}

// closeFreezeSources releases every source (error path and normal exit).
func closeFreezeSources(sources []freezeSource) {
	for _, s := range sources {
		s.close()
	}
}

// tailRowSource walks ONE un-sealed packed row's records with a fixed ledger.
// The row is raw and hash-sorted; blinding reorders it, so the source blinds
// the whole row up front and sorts — a row is one ledger's transactions
// (thousands at the extreme, 20 bytes each), so this is a bounded per-row
// buffer, not a chunk-scale accumulator. It runs the seal's own two steps,
// blindRow then SortColdEntries, which is why a tail row's records and a
// sealed run's are the same bytes in the same order. off starts at -1 (before
// the first record) per the freezeSource contract.
type tailRowSource struct {
	entries []ColdEntry
	rowSeq  uint32
	off     int
}

// newTailRowSource blinds and sorts one CF row's hashes. row is borrowed
// (the iterator's buffer); nothing of it is retained.
func newTailRowSource(row []byte, seq uint32, secret [stores.SecretLen]byte) *tailRowSource {
	entries := blindRow(make([]ColdEntry, 0, len(row)/rowHashLen), row, seq, secret)
	SortColdEntries(entries)
	return &tailRowSource{entries: entries, rowSeq: seq, off: -1}
}

func (t *tailRowSource) key() []byte { return t.entries[t.off].Key[:] }

func (t *tailRowSource) seq() uint32 { return t.rowSeq }

func (t *tailRowSource) advance() (bool, error) {
	t.off++
	return t.off < len(t.entries), nil
}

func (t *tailRowSource) close() {}

// ─────────────────────────── merge heap ───────────────────────────

// freezeHeap is the package's key-ordered heap (merge_heap.go) over
// freezeSources: entries carry each live source's CURRENT key beside its
// source index, so a compare never calls back through the freezeSource
// interface. The index tie-break emits equal cross-source duplicate keys in
// source order (runs oldest first, then tail rows in ledger order), and equal
// keys WITHIN a run are already in ledger order from the seal's sort — so
// global duplicate order is ascending ledger, deterministically: the .bin's
// stored order (SortColdEntries), reproduced by streaming.
//
// A cached key is the source's own buffer, valid only until that source
// advances: runSource.key() aliases the record buffer it re-reads into, and
// tailRowSource.key() re-slices as its offset moves. Only the root's source
// ever advances, and step refreshes the root's entry from it before sifting,
// so no entry outlives its bytes.
type freezeHeap struct {
	sources []freezeSource
	heap    keyHeap
}

// newFreezeHeap primes every source (first advance) and heapifies the
// non-empty ones. An empty source that fails its end-of-stream verification
// (torn empty run) fails here.
func newFreezeHeap(sources []freezeSource) (*freezeHeap, error) {
	h := &freezeHeap{sources: sources, heap: make(keyHeap, 0, len(sources))}
	for i, s := range sources {
		ok, err := s.advance()
		if err != nil {
			return nil, err
		}
		if ok {
			h.heap = append(h.heap, keyEntry{key: s.key(), idx: i})
		}
	}
	h.heap.heapify()
	return h, nil
}

func (h *freezeHeap) len() int { return len(h.heap) }

// min returns the smallest current record — its cached key (valid until the
// next step) and its ledger.
func (h *freezeHeap) min() ([]byte, uint32) {
	e := h.heap[0]
	return e.key, h.sources[e.idx].seq()
}

// step advances the minimum source past its emitted record, refilling the root
// with its next key — or dropping the source once drained (its end-of-stream
// verification ran inside advance).
func (h *freezeHeap) step() error {
	src := h.sources[h.heap[0].idx]
	ok, err := src.advance()
	if err != nil {
		return err
	}
	if !ok {
		h.heap = h.heap.dropRoot()
		return nil
	}
	h.heap[0].key = src.key()
	h.heap.siftDown(0)
	return nil
}
