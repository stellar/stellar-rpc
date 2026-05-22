// One-shot migration: existing ledger pack files (written by
// scripts/full-history-backfill with itemsPerRecord=128, ContentHash=on,
// no AppData) → cold-store-compatible pack files (itemsPerRecord=1,
// ContentHash=off, 4-byte AppData firstSeq) as expected by
// fullhistory/pkg/stores/ledger.ColdStoreReader.
//
// Operates entirely on the local filesystem. For each old .pack the tool
// reads every item via packfile.Reader+zstd.Decompressor and writes it to a
// new .pack at --dst-dir via ledger.ColdStoreWriter. Resume is per-chunk: a
// destination .pack that already opens cleanly via ColdStoreReader is
// skipped. Source files are left untouched.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/packfile"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/zstd"
)

const (
	ledgersPerChunk uint32 = 10_000
	chunksPerBucket uint32 = 1_000
)

func chunkFirstLedger(chunkID uint32) uint32 { return chunkID*ledgersPerChunk + 2 }

func packPath(root string, chunkID uint32) string {
	bucketID := chunkID / chunksPerBucket
	return filepath.Join(
		root,
		fmt.Sprintf("%05d", bucketID),
		fmt.Sprintf("%08d.pack", chunkID),
	)
}

func main() {
	var (
		srcDir     string
		dstDir     string
		firstChunk uint
		lastChunk  uint
		workers    int
	)
	flag.StringVar(&srcDir, "src-dir", "/mnt/nvme/disk2/ledgers", "root of existing (old-format) pack files")
	flag.StringVar(&dstDir, "dst-dir", "/mnt/nvme/disk2/ledgers-cold", "root for new cold-store-format pack files")
	flag.UintVar(&firstChunk, "first-chunk", 4999, "first chunkID (inclusive)")
	flag.UintVar(&lastChunk, "last-chunk", 5999, "last chunkID (inclusive)")
	flag.IntVar(&workers, "workers", 8, "concurrent chunk migrations")
	flag.Parse()

	if firstChunk > lastChunk {
		fmt.Fprintln(os.Stderr, "--first-chunk must be <= --last-chunk")
		os.Exit(2)
	}
	if workers < 1 {
		fmt.Fprintln(os.Stderr, "--workers must be >= 1")
		os.Exit(2)
	}

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		logger.WithError(err).Errorf("could not create dst-dir %s", dstDir)
		os.Exit(1)
	}

	// Shared decoder across all workers; concurrent-safe per zstd package doc.
	decoder := zstd.NewDecompressor()

	totalChunks := uint32(lastChunk - firstChunk + 1)
	chunkCh := make(chan uint32, workers*2)

	var (
		wg         sync.WaitGroup
		errOnce    sync.Once
		firstErr   error
		doneChunks atomic.Uint32
		migrated   atomic.Uint32
		skipped    atomic.Uint32
		bytesIn    atomic.Int64
		bytesOut   atomic.Int64
	)

	runStart := time.Now()

	for w := range workers {
		wg.Add(1)
		workerID := w
		go func() {
			defer wg.Done()
			wLog := logger.WithField("worker", workerID)
			for chunkID := range chunkCh {
				if ctx.Err() != nil {
					return
				}
				bIn, bOut, didMigrate, err := migrateChunk(ctx, wLog, decoder, srcDir, dstDir, chunkID)
				if err != nil {
					errOnce.Do(func() {
						firstErr = fmt.Errorf("chunk %d: %w", chunkID, err)
						cancel()
					})
					return
				}
				bytesIn.Add(bIn)
				bytesOut.Add(bOut)
				if didMigrate {
					migrated.Add(1)
				} else {
					skipped.Add(1)
				}
				done := doneChunks.Add(1)
				if done%10 == 0 || done == totalChunks {
					wLog.Infof(
						"progress: %d/%d (migrated=%d skipped=%d) bytesIn=%.1f GiB bytesOut=%.1f GiB elapsed=%s",
						done, totalChunks, migrated.Load(), skipped.Load(),
						float64(bytesIn.Load())/(1<<30),
						float64(bytesOut.Load())/(1<<30),
						time.Since(runStart).Round(time.Second),
					)
				}
			}
		}()
	}

	for chunkID := uint32(firstChunk); chunkID <= uint32(lastChunk); chunkID++ {
		select {
		case <-ctx.Done():
		case chunkCh <- chunkID:
		}
		if ctx.Err() != nil {
			break
		}
	}
	close(chunkCh)
	wg.Wait()

	if firstErr != nil {
		logger.WithError(firstErr).Error("migration failed")
		os.Exit(1)
	}
	if ctx.Err() != nil && !errors.Is(ctx.Err(), context.Canceled) {
		logger.WithError(ctx.Err()).Error("migration aborted")
		os.Exit(1)
	}
	logger.Infof(
		"migration complete: %d migrated, %d skipped, in=%.1f GiB, out=%.1f GiB, ratio=%.3f, wall=%s",
		migrated.Load(), skipped.Load(),
		float64(bytesIn.Load())/(1<<30),
		float64(bytesOut.Load())/(1<<30),
		float64(bytesOut.Load())/float64(bytesIn.Load()),
		time.Since(runStart).Round(time.Second),
	)
}

func migrateChunk(
	ctx context.Context,
	logger *supportlog.Entry,
	decoder *zstd.Decompressor,
	srcDir, dstDir string,
	chunkID uint32,
) (bytesIn, bytesOut int64, didMigrate bool, err error) {
	srcPath := packPath(srcDir, chunkID)
	dstPath := packPath(dstDir, chunkID)
	firstSeq := chunkFirstLedger(chunkID)

	// Resume: if a destination .pack already opens cleanly via ColdStoreReader
	// and has the expected first/last seqs, skip.
	if _, err := os.Stat(dstPath); err == nil {
		r, openErr := ledger.NewColdStoreReader(dstPath)
		if openErr == nil {
			first, firstErr := r.FirstSeq()
			last, lastErr := r.LastSeq()
			ok := firstErr == nil && lastErr == nil &&
				first == firstSeq && last == firstSeq+ledgersPerChunk-1
			_ = r.Close()
			if ok {
				return 0, 0, false, nil
			}
		}
		// Corrupt/incompatible destination — fall through and overwrite.
	}

	if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
		return 0, 0, false, fmt.Errorf("mkdir %s: %w", filepath.Dir(dstPath), err)
	}

	// Source size for reporting bytesIn.
	if fi, err := os.Stat(srcPath); err == nil {
		bytesIn = fi.Size()
	}

	src := packfile.Open(srcPath, packfile.ReaderOptions{RecordDecoder: decoder})
	defer src.Close()

	tr, err := src.Trailer()
	if err != nil {
		return 0, 0, false, fmt.Errorf("source Trailer: %w", err)
	}
	if tr.TotalItems != ledgersPerChunk {
		return 0, 0, false, fmt.Errorf("source has %d items, expected %d", tr.TotalItems, ledgersPerChunk)
	}

	w, err := ledger.NewColdStoreWriter(dstPath, firstSeq, ledger.ColdWriterOptions{})
	if err != nil {
		return 0, 0, false, fmt.Errorf("NewColdStoreWriter %q: %w", dstPath, err)
	}
	// On any error path, Close removes the partial file; on success Commit
	// finalizes and Close becomes a no-op.
	defer w.Close()

	chunkStart := time.Now()
	seq := firstSeq
	for item, iterErr := range src.ReadRange(0, int(ledgersPerChunk)) {
		if iterErr != nil {
			return bytesIn, 0, false, fmt.Errorf("source ReadRange at seq %d: %w", seq, iterErr)
		}
		if ctx.Err() != nil {
			return bytesIn, 0, false, ctx.Err()
		}
		if err := w.AppendLedger(seq, item); err != nil {
			return bytesIn, 0, false, fmt.Errorf("AppendLedger %d: %w", seq, err)
		}
		seq++
	}
	if seq != firstSeq+ledgersPerChunk {
		return bytesIn, 0, false, fmt.Errorf("only consumed %d ledgers, expected %d",
			seq-firstSeq, ledgersPerChunk)
	}
	if err := w.Commit(); err != nil {
		return bytesIn, 0, false, fmt.Errorf("commit: %w", err)
	}

	if fi, err := os.Stat(dstPath); err == nil {
		bytesOut = fi.Size()
	}

	logger.Debugf("migrated chunk %d: %s -> %s, %.1f MiB -> %.1f MiB in %s",
		chunkID, srcPath, dstPath,
		float64(bytesIn)/(1<<20), float64(bytesOut)/(1<<20),
		time.Since(chunkStart).Round(time.Millisecond),
	)
	return bytesIn, bytesOut, true, nil
}
