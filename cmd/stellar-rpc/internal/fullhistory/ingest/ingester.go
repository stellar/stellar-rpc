package ingest

import (
	"context"

	sdkingest "github.com/stellar/go-stellar-sdk/ingest"
)

// ledgerData is one ledger's shared, already-extracted input to every cold
// ingester. ColdService walks the borrowed view ONCE (ExtractLedgerEvents) and
// hands the result to each per-type writer, so txhash and events share the
// single TxProcessing walk instead of each running their own — halving cold
// per-ledger extraction, matching the hot path's IngestLedger (issue #836).
//
//   - seq is the ledger sequence on drain's contiguous counter (the in-order
//     contract is enforced at the source).
//   - raw is the wire-format LedgerCloseMeta bytes for the ledgers writer; it
//     ALIASES the source stream's borrowed buffer, valid only for the current
//     Ingest call, so an implementation must copy what it retains.
//   - txEvents is the per-transaction hash + contract events (apply order); each
//     element's Hash feeds txhash and the slice feeds events via
//     events.PayloadsFromLedgerEvents. Its byte slices alias the same buffer.
//   - closedAt is the view's LedgerCloseTime, needed for event shaping.
//
// txEvents/closedAt are zero when neither txhash nor events is enabled
// (ledgers-only: ColdService skips the walk entirely).
type ledgerData struct {
	seq      uint32
	raw      []byte
	closedAt int64
	txEvents []sdkingest.LedgerTransactionEvents
}

// ColdIngester ingests one data type for one chunk into a per-chunk cold writer.
//
// Ownership: the ingester OPENS its own per-chunk writer in its constructor and
// owns its lifecycle. Finalize commits the chunk's artifact (explicit,
// error-checked, never deferred). Close is always deferred and idempotent; on
// the failure path (Finalize never ran) it drops any partial file.
//
// Contract: Finalize must NOT be called after a failed Ingest — once any
// Ingest errors, the chunk is abandoned via Close and retried from scratch.
// Implementations may have committed partial per-ledger state before the
// error (e.g. the events ingester's mirror/pack run ahead of its offsets
// commit point), so a post-failure Finalize could publish an inconsistent
// artifact; implementations are encouraged to latch the failure and refuse
// (eventsCold does).
//
// Input: l carries one ledger's shared pre-extracted data (see ledgerData). Its
// borrowed byte slices are valid only for the current call — an implementation
// must copy any bytes it retains. ColdService drives the per-ledger Ingest calls
// sequentially, so each ledger is fully consumed before the next.
type ColdIngester interface {
	Ingest(ctx context.Context, l ledgerData) error
	Finalize(ctx context.Context) error
	Close() error
}

// Cold writers run ONLY on the batch freeze/backfill path — WriteColdChunk is
// their sole production caller — so they opt into the packfile writer's batch
// tuning rather than the serial zero-value defaults (issue #836): parallel zstd
// encoding plus background dirty-page writeback that smooths the final fsync.
const (
	// coldEncoderConcurrency parallelizes zstd record encoding per writer. The
	// backfill pool already runs DefaultWorkers (GOMAXPROCS) chunks concurrently,
	// so this stays modest — enough to overlap a chunk's compression with its
	// download without heavily oversubscribing that pool.
	// ponytail: fixed at 4 (the writer docs' low end); drop toward 1 if the
	// GOMAXPROCS-wide pool ever shows CPU contention under profiling.
	coldEncoderConcurrency = 4
	// coldBytesPerSync triggers background writeback every 1 MiB so Commit/Finish
	// doesn't flush a whole pack's dirty pages at once (a large win on networked
	// storage, per the writer docs).
	coldBytesPerSync = 1 << 20
)
