package bench

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/ingest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/hotchunk"
)

// hotOptions configures one hot ingest benchmark run.
type hotOptions struct {
	Source sourceConfig
	// Chunk is the single chunk whose ledgers are driven through the hot
	// service. Hot ingestion always writes all three data types — one atomic
	// WriteBatch across every hot column family — so there is no Types knob.
	Chunk chunk.ID
	// NumLedgers caps how many of the chunk's ledgers are ingested (0 = the
	// whole chunk). fsync-per-ledger makes full-chunk hot runs slow; a cap
	// gives cheap smoke runs without changing what is measured per ledger.
	NumLedgers uint32
	// HotRoot is the scratch root the fresh hot RocksDB is created under (at
	// geometry.NewLayout(HotRoot).HotChunkPath(Chunk)). The chunk's DB dir
	// must not already exist: hot timings are only comparable from a fixed
	// (empty) starting state — wipe it between runs.
	HotRoot string
	// OutDir receives the CSV report.
	OutDir string
}

func (o hotOptions) validate() error {
	if o.HotRoot == "" {
		return errors.New("--hot-dir is required")
	}
	if o.Chunk > maxChunkID {
		return fmt.Errorf("--chunk=%d is past the last valid chunk ID %d", uint32(o.Chunk), uint32(maxChunkID))
	}
	return nil
}

// runHot benchmarks the production hot ingest path: it opens a fresh
// per-chunk hot DB, builds ingest.NewHotService over it with the CSV sink,
// and drives the chunk's ledgers through Ingest one at a time — the same
// shape as the daemon's live loop. Per-phase percentiles come from the
// HotPhase signals; the read_blocked driver row captures time spent waiting
// on the source between ledgers.
func runHot(ctx context.Context, logger *supportlog.Entry, opts hotOptions) error {
	if err := opts.validate(); err != nil {
		return err
	}
	// Surface an unwritable --out before the expensive run, not after it.
	if err := os.MkdirAll(opts.OutDir, 0o755); err != nil {
		return fmt.Errorf("create --out dir %s: %w", opts.OutDir, err)
	}
	layout := geometry.NewLayout(opts.HotRoot)
	// A live daemon creates AND deletes hot/{chunk} DBs under its flocked hot
	// root (ingestion/discard), and RocksDB's own LOCK file only guards
	// double-opening one DB dir — so take the daemon's root flock: pointing
	// --hot-dir at a live deployment fails fast with ErrRootLocked instead of
	// racing its lifecycle, and the lock closes the stat-then-open window on
	// the exists check below.
	locks, err := config.LockRoots(layout.HotRoot())
	if err != nil {
		return fmt.Errorf("lock --hot-dir hot root: %w", err)
	}
	defer locks.Release()
	dbPath := layout.HotChunkPath(opts.Chunk)
	if _, err := os.Stat(dbPath); err == nil {
		return fmt.Errorf("hot DB dir %s already exists; delete it for a fixed starting state", dbPath)
	}
	streamFor, release, err := openSource(ctx, opts.Source)
	if err != nil {
		return err
	}
	defer release()
	stream, err := streamFor(opts.Chunk)
	if err != nil {
		return err
	}

	db, err := hotchunk.Open(dbPath, opts.Chunk, logger)
	if err != nil {
		return fmt.Errorf("open hot DB %s: %w", dbPath, err)
	}
	defer func() { _ = db.Close() }()

	sink := newCSVSink()
	if err := driveHot(ctx, ingest.NewHotService(db, sink), stream, sink, opts); err != nil {
		writePartialCSVs(logger, sink, opts.OutDir)
		return err
	}

	sink.logSummary(logger)
	written, err := sink.writeCSVs(opts.OutDir)
	if err != nil {
		return err
	}
	logger.Infof("wrote %d CSVs to %s", len(written), opts.OutDir)
	return nil
}

// driveHot feeds the benchmarked range through svc.Ingest sequentially,
// mirroring the daemon's hot loop, and records per-ledger source wait
// (read_blocked, first pull's setup excluded) plus the run's wall-clock
// (chunk_wall) on the sink.
func driveHot(
	ctx context.Context,
	svc *ingest.HotService,
	stream ledgerbackend.LedgerStream,
	sink *csvSink,
	opts hotOptions,
) error {
	first, last := opts.Chunk.FirstLedger(), opts.Chunk.LastLedger()
	// Overflow-safe cap: compare against the chunk's span rather than adding
	// a flag-supplied count to a ledger sequence.
	if span := last - first + 1; opts.NumLedgers > 0 && opts.NumLedgers < span {
		last = first + opts.NumLedgers - 1
	}

	start := time.Now()
	seq := first
	tRead := time.Now()
	for raw, serr := range stream.RawLedgers(ctx, ledgerbackend.BoundedRange(first, last)) {
		if serr != nil {
			return fmt.Errorf("stream at seq %d: %w", seq, serr)
		}
		// The first pull pays source setup (datastore dial, PrepareRange,
		// prefetch spin-up), not a between-ledgers wait — one setup-sized
		// outlier in a row whose max/total mean steady-state waits — so it
		// is excluded from read_blocked. It still counts in chunk_wall.
		if seq != first {
			sink.observeDriver(driverReadBlocked, time.Since(tRead), 0)
		}
		if err := svc.Ingest(ctx, seq, xdr.LedgerCloseMetaView(raw)); err != nil {
			return fmt.Errorf("hot ingest seq %d: %w", seq, err)
		}
		seq++
		tRead = time.Now()
	}
	if seq != last+1 {
		return fmt.Errorf("stream ended at seq %d, expected through %d", seq-1, last)
	}
	sink.observeDriver(driverChunkWall, time.Since(start), int(seq-first))
	return nil
}
