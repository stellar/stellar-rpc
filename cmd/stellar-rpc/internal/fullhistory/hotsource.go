package fullhistory

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"os"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/backfill"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/hotchunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
)

// rocksHotProbe is the production backfill.HotProbe: it opens the chunk's shared
// multi-CF hot DB and answers backfillSource's completeness question (decision
// (a): the single maxCommittedSeq).
type rocksHotProbe struct {
	hotRoot func(chunkID chunk.ID) string
	logger  *supportlog.Entry
}

// NewRocksHotProbe returns the production backfill.HotProbe (hotChunkPath maps a
// chunk to its hot-DB dir — the daemon passes Layout.HotChunkPath).
//
// Caller contract: OpenHotChunk must NOT be passed the LIVE chunk — ingestion
// holds its hot DB open read-write and a second open fails on RocksDB's LOCK. The
// freeze only ever targets chunks ingestion has already released.
func NewRocksHotProbe(hotChunkPath func(chunk.ID) string, logger *supportlog.Entry) backfill.HotProbe {
	return &rocksHotProbe{hotRoot: hotChunkPath, logger: logger}
}

func (p *rocksHotProbe) OpenHotChunk(chunkID chunk.ID) (backfill.HotChunk, bool, error) {
	dir := p.hotRoot(chunkID)
	if _, err := os.Stat(dir); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, false, nil // dir absent — caller treats as loss under "ready"
		}
		return nil, false, fmt.Errorf("stat hot dir %s: %w", dir, err)
	}

	// Open the chunk's shared multi-CF DB READ-ONLY: the freeze reads its ledgers
	// to re-derive the cold artifacts and must never mutate it (the design's
	// openRocksDBReadOnly). The probe only ever opens a chunk ingestion already
	// released, so its data is fully in SST — no concurrent writer, no WAL replay.
	db, err := hotchunk.OpenReadOnly(dir, chunkID, p.logger)
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

// MaxCommittedSeq returns the single authoritative last-committed ledger (decision (a)): the
// highest ledger seq the shared DB has durably committed. ok=false on an empty DB.
func (h *rocksHotChunk) MaxCommittedSeq() (uint32, bool, error) {
	seq, ok, err := h.db.MaxCommittedSeq()
	if err != nil {
		return 0, false, fmt.Errorf("hot DB max committed seq: %w", err)
	}
	return seq, ok, nil
}

// Source streams the chunk's LCMs from the ledgers CF as a LedgerStream the cold
// writer (WriteColdChunk) drains, so a just-closed chunk freezes straight from its
// hot DB without a refetch.
func (h *rocksHotChunk) Source() ledgerbackend.LedgerStream {
	return &hotLedgerStream{store: h.db.Ledgers()}
}

// Close releases the shared hot DB.
func (h *rocksHotChunk) Close() error {
	if h.db == nil {
		return nil
	}
	return h.db.Close()
}

// hotLedgerStream is a ledgerbackend.LedgerStream over a ledger.HotStore, so the
// source-blind cold pipeline freezes a just-closed chunk from its hot DB.
type hotLedgerStream struct {
	store *ledger.HotStore
}

var _ ledgerbackend.LedgerStream = (*hotLedgerStream)(nil)

// RawLedgers yields the range's wire bytes from the hot store. IterateLedgers
// yields BORROWED buffers (valid only to the next step); the drain loop consumes
// each fully before the next yield, so the borrow is safe. ctx cancellation is
// observed between ledgers (the LedgerStream contract drain relies on).
func (st *hotLedgerStream) RawLedgers(
	ctx context.Context, r ledgerbackend.Range, _ ...ledgerbackend.StreamOption,
) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		if st.store == nil {
			yield(nil, errors.New("hotLedgerStream has no store"))
			return
		}
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
