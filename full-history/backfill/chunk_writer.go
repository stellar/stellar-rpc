package backfill

import (
	"context"
	"fmt"
	"time"

	"github.com/stellar/stellar-rpc/full-history/pkg/format"
	"github.com/stellar/stellar-rpc/full-history/pkg/geometry"
	"github.com/stellar/stellar-rpc/full-history/pkg/logging"
	"github.com/stellar/stellar-rpc/full-history/pkg/memory"
)

// =============================================================================
// Chunk Writer — Per-Output Idempotency
// =============================================================================
//
// process_chunk checks each output flag independently and only produces missing
// outputs. A partially-completed chunk resumes from where it left off:
//
//   1. Check need_lfs, need_txhash, need_events from meta store
//   2. If all present → return (no-op)
//   3. Choose data source: if lfs exists → local packfile (no BSB download)
//   4. Open writers only for missing outputs
//   5. Loop ledgers, dispatch to each active writer
//   6. Fsync + flag each independently (no WriteBatch)

// ChunkWriterConfig holds the configuration for a ChunkWriter.
type ChunkWriterConfig struct {
	ChunkID       uint32
	LedgersPath   string // from cfg.ImmutableStorage.Ledgers.Path
	TxHashRawPath string // from cfg.ImmutableStorage.TxHashRaw.Path
	EventsPath    string // from cfg.ImmutableStorage.Events.Path

	Meta     BackfillMetaStore
	Memory   memory.Monitor
	Logger   logging.Logger
	Progress *IndexProgress
	Geo      geometry.Geometry
}

type chunkWriter struct {
	cfg ChunkWriterConfig
	log logging.Logger
}

func NewChunkWriter(cfg ChunkWriterConfig) *chunkWriter {
	if cfg.Meta == nil {
		panic("ChunkWriter: Meta required")
	}
	if cfg.Logger == nil {
		panic("ChunkWriter: Logger required")
	}
	return &chunkWriter{
		cfg: cfg,
		log: cfg.Logger.WithScope(fmt.Sprintf("CHUNK:%08d", cfg.ChunkID)),
	}
}

// WriteChunk processes a single chunk with per-output idempotency.
// Only missing outputs are produced. Each flag is set independently after fsync.
func (cw *chunkWriter) WriteChunk(ctx context.Context, source LedgerSource) (*ChunkWriteStats, error) {
	chunkStart := time.Now()
	chunkID := cw.cfg.ChunkID

	// 1. Check which outputs are missing.
	needLFS, err := cw.needsOutput(func(id uint32) (bool, error) { return cw.cfg.Meta.IsChunkLFSDone(id) })
	if err != nil {
		return nil, fmt.Errorf("check lfs flag for chunk %d: %w", chunkID, err)
	}
	needTxHash, err := cw.needsOutput(func(id uint32) (bool, error) { return cw.cfg.Meta.IsChunkTxHashDone(id) })
	if err != nil {
		return nil, fmt.Errorf("check txhash flag for chunk %d: %w", chunkID, err)
	}
	needEvents, err := cw.needsOutput(func(id uint32) (bool, error) { return cw.cfg.Meta.IsChunkEventsDone(id) })
	if err != nil {
		return nil, fmt.Errorf("check events flag for chunk %d: %w", chunkID, err)
	}

	if !needLFS && !needTxHash && !needEvents {
		cw.log.Info("All outputs present — skipping")
		return &ChunkWriteStats{ChunkID: chunkID, Skipped: true}, nil
	}

	cw.log.Info("Processing: need_lfs=%v need_txhash=%v need_events=%v", needLFS, needTxHash, needEvents)

	// 2. Open writers only for missing outputs.
	var lfsW LFSWriter
	if needLFS {
		lfsW, err = NewLFSWriter(LFSWriterConfig{
			LedgersPath: cw.cfg.LedgersPath,
			ChunkID:     chunkID,
		})
		if err != nil {
			return nil, fmt.Errorf("create LFS writer for chunk %d: %w", chunkID, err)
		}
	}

	var txW *txHashWriter
	if needTxHash {
		txW, err = NewTxHashWriter(TxHashWriterConfig{
			TxHashRawPath: cw.cfg.TxHashRawPath,
			ChunkID:       chunkID,
		})
		if err != nil {
			if lfsW != nil {
				lfsW.Abort()
			}
			return nil, fmt.Errorf("create TxHash writer for chunk %d: %w", chunkID, err)
		}
	}

	var eventsW EventsWriter
	if needEvents {
		eventsW = NewStubEventsWriter()
	}

	// 3. Process each ledger.
	firstLedger := cw.cfg.Geo.ChunkFirstLedger(chunkID)
	lastLedger := cw.cfg.Geo.ChunkLastLedger(chunkID)
	var totalLFSTime, totalTxTime time.Duration
	var txCount int64

	for seq := firstLedger; seq <= lastLedger; seq++ {
		select {
		case <-ctx.Done():
			cw.abortAll(lfsW, txW, eventsW)
			return nil, ctx.Err()
		default:
		}

		getLedgerStart := time.Now()
		lcm, err := source.GetLedger(ctx, seq)
		if err != nil {
			cw.abortAll(lfsW, txW, eventsW)
			return nil, fmt.Errorf("get ledger %d for chunk %d: %w", seq, chunkID, err)
		}
		if cw.cfg.Progress != nil {
			cw.cfg.Progress.RecordBSBGetLedger(time.Since(getLedgerStart))
		}

		if needLFS {
			lfsTime, err := lfsW.AppendLedger(lcm)
			if err != nil {
				cw.abortAll(lfsW, txW, eventsW)
				return nil, fmt.Errorf("write LFS for ledger %d chunk %d: %w", seq, chunkID, err)
			}
			totalLFSTime += lfsTime
		}

		if needTxHash {
			entries, err := ExtractTxHashes(lcm, seq)
			if err != nil {
				cw.abortAll(lfsW, txW, eventsW)
				return nil, fmt.Errorf("extract txhashes for ledger %d chunk %d: %w", seq, chunkID, err)
			}
			if len(entries) > 0 {
				txTime, err := txW.AppendEntries(entries)
				if err != nil {
					cw.abortAll(lfsW, txW, eventsW)
					return nil, fmt.Errorf("write txhashes for ledger %d chunk %d: %w", seq, chunkID, err)
				}
				totalTxTime += txTime
			}
			txCount += int64(len(entries))
		}

		// Events: stub — no actual extraction yet.
	}

	// 4. Fsync + flag each output independently.
	var totalFsyncTime time.Duration

	if needLFS {
		lfsFsyncTime, err := lfsW.FsyncAndClose()
		if err != nil {
			cw.abortAll(nil, txW, eventsW)
			return nil, fmt.Errorf("fsync LFS for chunk %d: %w", chunkID, err)
		}
		totalFsyncTime += lfsFsyncTime
		if err := cw.cfg.Meta.SetChunkLFS(chunkID); err != nil {
			return nil, fmt.Errorf("set lfs flag for chunk %d: %w", chunkID, err)
		}
	}

	if needTxHash {
		txFsyncTime, err := txW.FsyncAndClose()
		if err != nil {
			cw.abortAll(nil, nil, eventsW)
			return nil, fmt.Errorf("fsync txhash for chunk %d: %w", chunkID, err)
		}
		totalFsyncTime += txFsyncTime
		if err := cw.cfg.Meta.SetChunkTxHash(chunkID); err != nil {
			return nil, fmt.Errorf("set txhash flag for chunk %d: %w", chunkID, err)
		}
	}

	if needEvents {
		evFsyncTime, err := eventsW.FsyncAndClose()
		if err != nil {
			return nil, fmt.Errorf("fsync events for chunk %d: %w", chunkID, err)
		}
		totalFsyncTime += evFsyncTime
		if err := cw.cfg.Meta.SetChunkEvents(chunkID); err != nil {
			return nil, fmt.Errorf("set events flag for chunk %d: %w", chunkID, err)
		}
	}

	if cw.cfg.Memory != nil {
		cw.cfg.Memory.Check()
	}

	ledgersProcessed := int(lastLedger - firstLedger + 1)
	var lfsBytesWritten int64
	if lfsW != nil {
		lfsBytesWritten = lfsW.DataBytesWritten()
	}
	var txBytesWritten int64
	if txW != nil {
		txBytesWritten = txW.BytesWritten()
	}

	stats := &ChunkWriteStats{
		ChunkID:            chunkID,
		LedgersProcessed:   ledgersProcessed,
		TxCount:            txCount,
		LFSWriteTime:       totalLFSTime,
		TxHashWriteTime:    totalTxTime,
		FsyncTime:          totalFsyncTime,
		TotalTime:          time.Since(chunkStart),
		LFSBytesWritten:    lfsBytesWritten,
		TxHashBytesWritten: txBytesWritten,
	}

	cw.log.Info("Done: %s ledgers, %s tx in %s (LFS %v, TxH %v, fsync %v)",
		format.FormatNumber(int64(ledgersProcessed)),
		format.FormatNumber(txCount),
		format.FormatDuration(stats.TotalTime),
		stats.LFSWriteTime, stats.TxHashWriteTime, stats.FsyncTime)

	if cw.cfg.Progress != nil {
		cw.cfg.Progress.RecordChunkComplete(*stats)
	}

	return stats, nil
}

// needsOutput returns true if the flag is NOT set (i.e., output is missing).
func (cw *chunkWriter) needsOutput(isDone func(uint32) (bool, error)) (bool, error) {
	done, err := isDone(cw.cfg.ChunkID)
	if err != nil {
		return false, err
	}
	return !done, nil
}

func (cw *chunkWriter) abortAll(lfsW LFSWriter, txW *txHashWriter, eventsW EventsWriter) {
	if lfsW != nil {
		lfsW.Abort()
	}
	if txW != nil {
		txW.Abort()
	}
	if eventsW != nil {
		eventsW.Abort()
	}
}
