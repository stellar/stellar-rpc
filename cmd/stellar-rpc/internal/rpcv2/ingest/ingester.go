package ingest

import (
	"context"
	"errors"
	"fmt"
	"time"

	sdkingest "github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/packfile"
)

// coldChunk is one chunk's set of cold writers — one concrete field per data
// type, nil when the type is not enabled. The set is fixed at three, so there
// is no ingester interface or fan-out: ingest mirrors what the hot path's
// hotchunk.DB.IngestLedger does inline (extract once, then concrete writes),
// and each writer takes exactly the inputs it uses.
//
// Ownership: openColdChunk opens each enabled writer's per-chunk file; finalize
// commits the chunk's artifacts (explicit, error-checked, never deferred);
// close is always deferred and idempotent — on the failure path (finalize never
// ran) each writer drops its partial file.
//
// Contract: finalize must NOT be called after a failed ingest — once any ingest
// errors, the chunk is abandoned via close and retried from scratch. A writer
// may have committed partial per-ledger state before the error (the events
// writer's mirror/pack run ahead of its offsets commit point), so a
// post-failure finalize could publish an inconsistent artifact; eventsCold
// latches the failure and refuses.
type coldChunk struct {
	ledgers *ledgerCold
	txhash  *txhashCold
	events  *eventsCold
	sink    MetricSink
}

// openColdChunk opens one cold writer per enabled type at its resolved path,
// in the canonical ledgers→txhash→events order, with build rollback. (The
// freeze path re-encodes the same order and per-kind dispatch in
// FreezeColdChunk — an order or kind change must touch both entry points.)
// An enabled type with an empty ColdDirs path is a config
// error. On any failure the already-opened writers are closed; they never
// ingested or finalized, and close emits no per-writer ColdIngest sample, so a
// rolled-back build produces no phantom-success sample.
func openColdChunk(dirs ColdDirs, chunkID chunk.ID, sink MetricSink, cfg Config) (*coldChunk, error) {
	cc := &coldChunk{sink: sink}
	fail := func(err error) (*coldChunk, error) { return nil, errors.Join(err, cc.close()) }
	if cfg.Ledgers {
		if dirs.LedgerPack == "" {
			return fail(errors.New("ingest: ledgers enabled but its ColdDirs path is empty"))
		}
		w, err := newLedgerCold(dirs.LedgerPack, chunkID, sink, cfg.ZstdEncodeWorkers)
		if err != nil {
			return fail(fmt.Errorf("open ledgers cold writer: %w", err))
		}
		cc.ledgers = w
	}
	if cfg.Txhash {
		if dirs.TxhashBin == "" {
			return fail(errors.New("ingest: txhash enabled but its ColdDirs path is empty"))
		}
		w, err := newTxhashCold(dirs.TxhashBin, chunkID, sink)
		if err != nil {
			return fail(fmt.Errorf("open txhash cold writer: %w", err))
		}
		cc.txhash = w
	}
	if cfg.Events {
		if dirs.EventsDir == "" {
			return fail(errors.New("ingest: events enabled but its ColdDirs path is empty"))
		}
		w, err := newEventsCold(dirs.EventsDir, chunkID, sink)
		if err != nil {
			return fail(fmt.Errorf("open events cold writer: %w", err))
		}
		cc.events = w
	}
	return cc, nil
}

// ingest writes one ledger into every enabled writer, in canonical order. raw
// is the ledger's borrowed wire bytes over the source stream's buffer, valid
// only for this call
// — each writer copies what it retains. seq is the ledger sequence on drain's
// contiguous counter (the in-order contract is enforced at the source).
//
// When txhash or events is enabled, the bytes are walked ONCE (the shared
// ExtractLedgerEvents, mirroring the hot path's IngestLedger): one pass over
// its output accumulates the indexable hashes for txhash and shapes the
// event payloads
// (events.PayloadShaper) for events together — halving cold
// per-ledger extraction (issue #836). The walk-or-not
// decision is structural: no consumer of the walk → no walk, so a ledgers-only
// chunk never pays it and an enabled events writer can never miss it. The walk
// (accumulation and shaping included) is metered as the ledger-scoped
// ColdExtract signal. The first error aborts the ledger.
func (c *coldChunk) ingest(seq uint32, raw []byte) error {
	if c.ledgers != nil {
		if err := c.ledgers.write(seq, raw); err != nil {
			return err
		}
	}
	if c.txhash == nil && c.events == nil {
		return nil
	}
	// A walk failure aborts the ledger before any writer runs, so no per-writer
	// signal fires for it — the error-carrying ColdExtract (with the partial
	// walk's duration) is its one metric, mirroring hot's PhaseExtract failure.
	start := time.Now()
	lcm := xdr.LedgerCloseMetaView(raw)
	closedAt, err := lcm.LedgerCloseTime()
	if err != nil {
		c.sink.ColdExtract(time.Since(start), 0, err)
		return fmt.Errorf("ledger close time seq %d: %w", seq, err)
	}
	txEvents, err := sdkingest.ExtractLedgerEvents(lcm)
	if err != nil {
		c.sink.ColdExtract(time.Since(start), 0, err)
		return fmt.Errorf("extract ledger events seq %d: %w", seq, err)
	}
	shaper := events.NewPayloadShaper(seq, closedAt)
	hashes, err := c.walk(txEvents, &shaper)
	if err != nil {
		c.sink.ColdExtract(time.Since(start), 0, err)
		return fmt.Errorf("shape events seq %d: %w", seq, err)
	}
	c.sink.ColdExtract(time.Since(start), len(txEvents), nil)
	if c.txhash != nil {
		if err := c.txhash.write(seq, hashes); err != nil {
			return err
		}
	}
	if c.events != nil {
		if err := c.events.write(seq, shaper.Finish()); err != nil {
			return err
		}
	}
	return nil
}

// walk runs ONE pass over the extracted slice, accumulating the
// indexable tx hashes (outer, plus inner for a fee-bump — #862) when txhash is
// enabled and feeding shaper when events is enabled. It returns the
// accumulated hashes (nil when txhash is
// disabled); hashes are value copies, the shaper's payload bytes alias the
// walked buffer.
func (c *coldChunk) walk(
	txEvents []sdkingest.LedgerTransactionEvents, shaper *events.PayloadShaper,
) ([][32]byte, error) {
	var hashes [][32]byte
	collectHashes, shapeEvents := c.txhash != nil, c.events != nil
	if collectHashes {
		// Fee-bump slack: each fee-bump contributes its inner hash too
		// (#862), so a flat cap would regrow on the first one.
		hashes = make([][32]byte, 0, len(txEvents)+len(txEvents)/8+1)
	}
	if shapeEvents {
		shaper.Begin(len(txEvents))
	}
	for i := range txEvents {
		if collectHashes {
			hashes = append(hashes, txEvents[i].Hash)
			if txEvents[i].FeeBump {
				hashes = append(hashes, txEvents[i].InnerHash)
			}
		}
		if shapeEvents {
			if err := shaper.Add(i, txEvents[i]); err != nil {
				return nil, err
			}
		}
	}
	return hashes, nil
}

// finalize commits each enabled writer's chunk artifact, in canonical order.
// The first error STOPS: the remaining (unfinalized) writers are released by
// the caller's deferred close, and the failed chunk attempt is reported to the
// orchestrator, which never records completion for it. Artifacts the earlier
// writers already committed are left in place — without the orchestrator's
// completion record they are inert scratch (see the package doc's artifact
// model), and the retry's overwrite is the cleanup.
func (c *coldChunk) finalize(ctx context.Context) error {
	if c.ledgers != nil {
		if err := c.ledgers.finalize(ctx); err != nil {
			return fmt.Errorf("finalize: %w", err)
		}
	}
	if c.txhash != nil {
		if err := c.txhash.finalize(ctx); err != nil {
			return fmt.Errorf("finalize: %w", err)
		}
	}
	if c.events != nil {
		if err := c.events.finalize(ctx); err != nil {
			return fmt.Errorf("finalize: %w", err)
		}
	}
	return nil
}

// close closes every opened writer, joining each close error. Idempotent: on
// the failure path a writer's close drops its partial file; after a successful
// finalize it is a no-op. A per-writer ColdIngest is emitted only from a
// TERMINAL step (a failed write, via coldMetrics.observe, or finalize) — never
// from close, so a writer rolled back before any work produces no sample.
func (c *coldChunk) close() error {
	var err error
	if c.ledgers != nil {
		if cerr := c.ledgers.close(); cerr != nil {
			err = errors.Join(err, fmt.Errorf("close: %w", cerr))
		}
	}
	if c.txhash != nil {
		if cerr := c.txhash.close(); cerr != nil {
			err = errors.Join(err, fmt.Errorf("close: %w", cerr))
		}
	}
	if c.events != nil {
		if cerr := c.events.close(); cerr != nil {
			err = errors.Join(err, fmt.Errorf("close: %w", cerr))
		}
	}
	return err
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
	coldBytesPerSync = packfile.DefaultBytesPerSync
)
