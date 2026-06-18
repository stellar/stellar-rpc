package streaming

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"os"
	"path/filepath"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/ingest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/eventstore"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash"
)

// rocksHotProbe is the production HotProbe: it opens the chunk's three
// independent per-chunk RocksDB hot stores (ledger, txhash, events) at the
// paths the daemon's hot-storage layout dictates, and answers catchupSource's
// completeness question over them.
//
// The three stores are independent (DECISION (b): no cross-store atomic batch),
// so "complete" is the MIN of their last-committed ledger seq — see
// minCommittedSeq for how each store's contribution is derived and why the
// derivation is conservative (it can only UNDER-report completeness, which is
// the safe direction: an under-report falls through to re-derivation, never a
// false "complete").
type rocksHotProbe struct {
	hotRoot func(chunkID chunk.ID) string
	logger  *supportlog.Entry
}

// NewRocksHotProbe returns the production HotProbe. hotChunkPath maps a chunk to
// its hot-DB directory (the daemon passes Layout.HotChunkPath); logger is
// forwarded to the store openers.
//
// Caller contract: the chunk passed to OpenHotChunk must NOT be the one captive
// core is actively ingesting — that chunk holds its hot RocksDB open read-write,
// and a second open of the same path fails on RocksDB's LOCK. The catch-up loop
// excludes the live chunk by design (the partial resume chunk is finished by
// ingestion, not by a freeze pass), so the probe only ever opens chunks
// ingestion has already released.
func NewRocksHotProbe(hotChunkPath func(chunk.ID) string, logger *supportlog.Entry) HotProbe {
	return &rocksHotProbe{hotRoot: hotChunkPath, logger: logger}
}

func (p *rocksHotProbe) OpenHotChunk(chunkID chunk.ID) (HotChunk, bool, error) {
	dir := p.hotRoot(chunkID)
	if _, err := os.Stat(dir); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, false, nil // dir absent — caller treats as loss under "ready"
		}
		return nil, false, fmt.Errorf("stat hot dir %s: %w", dir, err)
	}

	// The three hot stores live as siblings under the chunk's hot dir. The
	// subdirectory names match the daemon's hot-store openers; opening any of
	// the three read paths uses the same constructors the ingester uses.
	lstore, err := ledger.OpenHotStore(ledgerHotPath(dir), chunkID, p.logger)
	if err != nil {
		return nil, false, fmt.Errorf("open ledger hot store: %w", err)
	}
	tstore, err := txhash.NewHotStore(txhashHotPath(dir), chunkID, p.logger)
	if err != nil {
		_ = lstore.Close()
		return nil, false, fmt.Errorf("open txhash hot store: %w", err)
	}
	estore, err := eventstore.OpenHotStore(eventsHotPath(dir), chunkID, p.logger)
	if err != nil {
		_ = lstore.Close()
		_ = tstore.Close()
		return nil, false, fmt.Errorf("open events hot store: %w", err)
	}
	return &rocksHotChunk{chunkID: chunkID, ledger: lstore, txhash: tstore, events: estore}, true, nil
}

// Hot-store subdirectory names under a chunk's hot dir. They are the streaming
// daemon's hot-storage layout convention, kept here so the probe and the
// ingestion-side hot-store openers agree on one set of paths.
func ledgerHotPath(chunkDir string) string { return filepath.Join(chunkDir, "ledgers") }
func txhashHotPath(chunkDir string) string { return filepath.Join(chunkDir, "txhash") }
func eventsHotPath(chunkDir string) string { return filepath.Join(chunkDir, "events") }

// rocksHotChunk is one chunk's opened hot tier.
type rocksHotChunk struct {
	chunkID chunk.ID
	ledger  *ledger.HotStore
	txhash  *txhash.HotStore
	events  *eventstore.HotStore
}

// MinCommittedSeq returns the MIN across the three stores' last-committed ledger
// seq (DECISION (b)). Each store's contribution:
//
//   - ledger: LastSeq() — the highest ledger seq durably written. The true
//     per-ledger watermark; every ingested ledger writes one row.
//   - events: Offsets().EndLedger()-1 — the events store advances its
//     LedgerOffsets once per ledger (including zero-event ledgers), so EndLedger
//     is the exclusive committed end; EndLedger-1 is the last committed seq.
//   - txhash: the ledger store's LastSeq is used as txhash's upper bound rather
//     than scanning ~3M random-keyed rows for a max seq. The hot fan-out
//     (HotService) writes all three stores for a ledger before pulling the next,
//     so txhash never trails the ledger store by more than a single in-flight
//     ledger; and a zero-tx ledger writes NOTHING to txhash, so a contents-
//     derived max would spuriously under-report a complete chunk ending in
//     empty ledgers. Binding txhash to the ledger watermark is therefore both
//     cheaper and more accurate. (The HotChunk contract is genuinely min-of-
//     three; a future txhash committed-watermark slots straight in here.)
//
// ok is false if any contributing store is empty — an empty store has no
// committed seq to take a min with, so the chunk cannot be complete.
func (h *rocksHotChunk) MinCommittedSeq() (uint32, bool, error) {
	lseq, lok, err := h.ledger.LastSeq()
	if err != nil {
		return 0, false, fmt.Errorf("ledger LastSeq: %w", err)
	}
	if !lok {
		return 0, false, nil
	}

	offsets, err := h.events.Offsets()
	if err != nil {
		return 0, false, fmt.Errorf("events Offsets: %w", err)
	}
	if offsets.LedgerCount() == 0 {
		return 0, false, nil
	}
	eseq := offsets.EndLedger() - 1

	// txhash's contribution is bounded by the ledger watermark (see doc): it is
	// already <= lseq, so it never raises the min and we need not query it.
	return min(lseq, eseq), true, nil
}

// Source streams the chunk's LCMs from the ledger hot store as a ChunkSource the
// cold pipeline drains.
func (h *rocksHotChunk) Source() ingest.ChunkSource {
	return &hotLedgerSource{store: h.ledger}
}

// Close releases the three opened stores, joining any errors.
func (h *rocksHotChunk) Close() error {
	var err error
	if h.ledger != nil {
		err = errors.Join(err, h.ledger.Close())
	}
	if h.txhash != nil {
		err = errors.Join(err, h.txhash.Close())
	}
	if h.events != nil {
		err = errors.Join(err, h.events.Close())
	}
	return err
}

// ---------------------------------------------------------------------------
// hotLedgerSource — an ingest.ChunkSource backed by a ledger.HotStore, so the
// merged cold pipeline (RunColdChunk) can freeze a just-closed chunk straight
// from its hot DB without a refetch.
// ---------------------------------------------------------------------------

type hotLedgerSource struct {
	store *ledger.HotStore
}

// OpenStream returns a stream over the hot store's ledgers for the requested
// chunk. The store is already chunk-bound; the stream honors the driver's
// requested [from,to] range via IterateLedgers.
func (s *hotLedgerSource) OpenStream(chunkID chunk.ID) (ledgerbackend.LedgerStream, error) {
	if s.store == nil {
		return nil, errors.New("streaming: hotLedgerSource has no store")
	}
	if s.store.ChunkID() != chunkID {
		return nil, fmt.Errorf("streaming: hotLedgerSource bound to chunk %s, asked for %s",
			s.store.ChunkID(), chunkID)
	}
	return &hotLedgerStream{store: s.store}, nil
}

type hotLedgerStream struct {
	store *ledger.HotStore
}

var _ ledgerbackend.LedgerStream = (*hotLedgerStream)(nil)

// RawLedgers yields each ledger's wire bytes for the requested range from the
// hot store. The store's IterateLedgers yields BORROWED buffers (valid only to
// the next step); the cold ingesters copy what they retain (HotIngester
// contract), and the drain loop consumes each ledger fully before the next
// yield, so the borrow is safe. ctx cancellation is observed between ledgers,
// upholding the ChunkSource contract the drain loop relies on.
func (st *hotLedgerStream) RawLedgers(
	ctx context.Context, r ledgerbackend.Range, _ ...ledgerbackend.StreamOption,
) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		to := r.To()
		if !r.Bounded() {
			last, ok, err := st.store.LastSeq()
			if err != nil {
				yield(nil, err)
				return
			}
			if !ok {
				return
			}
			to = last
		}
		for e, ierr := range st.store.IterateLedgers(r.From(), to) {
			if cerr := ctx.Err(); cerr != nil {
				yield(nil, cerr)
				return
			}
			if ierr != nil {
				yield(nil, ierr)
				return
			}
			if !yield(e.Bytes, nil) {
				return
			}
		}
	}
}
