package ingest

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/hotchunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/ledger"
)

// FreezeColdChunk is WriteColdChunk for a chunk whose source is its own
// complete hot DB. It takes no ledger stream: the hot DB's column families
// hold each artifact's content in its final form, so every artifact is a
// copy or a merge of them, and nothing is decompressed or extracted again.
// The artifacts are WriteColdChunk's bytes, and so are the metrics and the
// failure semantics: the first error stops the freeze, and a retry
// overwrites what it left.
func FreezeColdChunk(
	ctx context.Context,
	logger *supportlog.Entry,
	chunkID chunk.ID,
	db *hotchunk.DB,
	dirs ColdDirs,
	sink MetricSink,
	cfg Config,
) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	sink = orNop(sink)
	start := time.Now()
	defer func() { sink.ColdChunkTotal(time.Since(start)) }()

	if err := ctx.Err(); err != nil {
		return err
	}
	logger.Debugf("freeze chunk %d [%d, %d] from its hot DB", uint32(chunkID), chunkID.FirstLedger(), chunkID.LastLedger())
	if cfg.Ledgers {
		if dirs.LedgerPack == "" {
			return errors.New("ingest: ledgers enabled but its ColdDirs path is empty")
		}
		if err := freezeArm(sink, dataTypeLedgers, func() (int, error) {
			return db.FreezeLedgersCold(ctx, dirs.LedgerPack, ledger.ColdWriterOptions{
				Concurrency:  coldEncoderConcurrency,
				BytesPerSync: coldBytesPerSync,
			})
		}); err != nil {
			return err
		}
	}
	if cfg.Txhash {
		if dirs.TxhashBin == "" {
			return errors.New("ingest: txhash enabled but its ColdDirs path is empty")
		}
		if err := freezeArm(sink, dataTypeTxhash, func() (int, error) {
			n, err := db.FreezeTxhashCold(ctx, dirs.TxhashBin, [stores.SecretLen]byte(cfg.TxhashSecret),
				cfg.TxhashPayloadBase, coldBytesPerSync)
			// A walk build of the chunk that crashed may have left runs.
			err = errors.Join(err, os.RemoveAll(dirs.TxhashRuns))
			return int(n), err //nolint:gosec // a chunk's entry count
		}); err != nil {
			return err
		}
	}
	if cfg.Events {
		if dirs.Events.Data == "" || dirs.Events.Index == "" {
			return errors.New("ingest: events enabled but its ColdDirs path is empty")
		}
		if err := freezeArm(sink, dataTypeEvents, func() (int, error) {
			n, err := db.FreezeEventsCold(ctx, dirs.Events, [stores.SecretLen]byte(cfg.EventsSecret),
				event.ColdWriterOptions{Concurrency: coldEncoderConcurrency, BytesPerSync: coldBytesPerSync})
			return int(n), err
		}); err != nil {
			return err
		}
	}
	return nil
}

// freezeArm runs one artifact's freeze and reports it as the walk reports
// that artifact: one ColdIngest, and a finalize stage on success.
func freezeArm(sink MetricSink, dataType string, build func() (int, error)) error {
	start := time.Now()
	n, err := build()
	d := time.Since(start)
	if err != nil {
		err = fmt.Errorf("freeze %s from the hot DB: %w", dataType, err)
	} else {
		sink.IngestStage(dataType, stageFinalize, d, n)
	}
	sink.ColdIngest(dataType, d, n, err)
	return err
}
