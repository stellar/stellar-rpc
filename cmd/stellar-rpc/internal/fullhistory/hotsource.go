package fullhistory

import (
	"errors"
	"fmt"
	"os"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/backfill"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/hotchunk"
)

// rocksHotProbe is the production backfill.HotProbe: it opens the chunk's shared
// multi-CF hot DB and answers backfillSource's completeness question (decision
// (a): the single maxCommittedSeq).
type rocksHotProbe struct {
	hotRoot func(chunkID chunk.ID) string
	logger  *supportlog.Entry
	recover bool
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

// NewRocksHotRecoveryProbe returns the startup progress probe. Unlike the freeze
// probe, it opens the highest ready hot DB read-write: a read-only open would
// also recover a crash-left synced WAL into memtables (so MaxCommittedSeq is
// correct either way), but only a writable handle persists that recovery — its
// Close flushes to SST. Startup uses it before ingestion opens a writer, then
// closes it immediately.
func NewRocksHotRecoveryProbe(hotChunkPath func(chunk.ID) string, logger *supportlog.Entry) backfill.HotProbe {
	return &rocksHotProbe{hotRoot: hotChunkPath, logger: logger, recover: true}
}

func (p *rocksHotProbe) OpenHotChunk(chunkID chunk.ID) (backfill.HotChunk, bool, error) {
	dir := p.hotRoot(chunkID)
	if _, err := os.Stat(dir); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, false, nil // dir absent — caller treats as loss under "ready"
		}
		return nil, false, fmt.Errorf("stat hot dir %s: %w", dir, err)
	}

	var (
		db  *hotchunk.DB
		err error
	)
	if p.recover {
		// Recovery opens read-WRITE so a synced-WAL replay is persisted on Close,
		// but must-exist (create-if-missing OFF): a "ready" chunk's DB is never
		// auto-created here.
		db, err = hotchunk.OpenExisting(dir, chunkID, p.logger)
	} else {
		// Open the chunk's shared multi-CF DB READ-ONLY: the freeze reads its
		// ledgers to re-derive the cold artifacts and must never mutate it. The
		// freeze only targets chunks ingestion already released, so its data is in
		// SST (no concurrent writer, no WAL replay needed).
		db, err = hotchunk.OpenReadOnly(dir, chunkID, p.logger)
	}
	if err != nil {
		return nil, false, fmt.Errorf("open hot chunk DB: %w", err)
	}
	return &rocksHotChunk{db: db}, true, nil
}

// rocksHotChunk is one chunk's opened hot tier — the single shared DB.
type rocksHotChunk struct {
	db *hotchunk.DB
}

// MaxCommittedSeq returns the single authoritative last-committed ledger (decision (a)): the
// highest ledger seq the shared DB has durably committed. ok=false on an empty DB.
func (h *rocksHotChunk) MaxCommittedSeq() (uint32, bool, error) {
	return h.db.MaxCommittedSeq()
}

// Source streams the chunk's LCMs from the ledgers CF as a LedgerStream the cold
// writer (WriteColdChunk) drains, so a just-closed chunk freezes straight from its
// hot DB without a refetch. The adapter lives on hotchunk.DB.
func (h *rocksHotChunk) Source() ledgerbackend.LedgerStream {
	return h.db.Source()
}

// Close releases the shared hot DB.
func (h *rocksHotChunk) Close() error {
	return h.db.Close()
}
