package streaming

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"os"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/ingest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/hotchunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
)

// rocksHotProbe is the production HotProbe: it opens the chunk's single shared
// multi-CF hot DB at the path the daemon's hot-storage layout dictates and
// answers backfillSource's completeness question over it (decision (a): the
// single authoritative maxCommittedSeq, no min-of-three).
type rocksHotProbe struct {
	hotRoot func(chunkID chunk.ID) string
	logger  *supportlog.Entry
}

// NewRocksHotProbe returns the production HotProbe. hotChunkPath maps a chunk to
// its hot-DB directory (the daemon passes Layout.HotChunkPath); logger is
// forwarded to the shared-DB opener.
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

	// One shared multi-CF DB at the chunk's hot dir — the same instance, opened
	// with the same union of CFs, that the ingestion side writes.
	db, err := hotchunk.Open(dir, chunkID, p.logger)
	if err != nil {
		return nil, false, fmt.Errorf("open hot chunk DB: %w", err)
	}
	return &rocksHotChunk{chunkID: chunkID, db: db}, true, nil
}

// rocksHotChunk is one chunk's opened hot tier — the single shared DB.
type rocksHotChunk struct {
	chunkID chunk.ID
	db      *hotchunk.DB
}

// MaxCommittedSeq returns the single authoritative watermark (decision (a)): the
// highest ledger seq the shared DB has durably committed. ok=false on an empty DB.
func (h *rocksHotChunk) MaxCommittedSeq() (uint32, bool, error) {
	seq, ok, err := h.db.MaxCommittedSeq()
	if err != nil {
		return 0, false, fmt.Errorf("hot DB max committed seq: %w", err)
	}
	return seq, ok, nil
}

// Source streams the chunk's LCMs from the ledgers CF as a ChunkSource the cold
// pipeline drains.
func (h *rocksHotChunk) Source() ingest.ChunkSource {
	return &hotLedgerSource{store: h.db.Ledgers()}
}

// Close releases the shared hot DB.
func (h *rocksHotChunk) Close() error {
	if h.db == nil {
		return nil
	}
	return h.db.Close()
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
