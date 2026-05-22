// Experimental backfill driver for the full-history ledger cold store.
//
// Reads ledgers from a BSB-backed GCS bucket and writes one cold-store
// pack file per 10K-ledger chunk under
// {output-dir}/{bucketID:05d}/{chunkID:08d}.pack. Each ledger is appended
// via fullhistory/pkg/stores/ledger.ColdStoreWriter — raw
// LedgerCloseMeta XDR bytes returned by BufferedStorageBackend.GetLedgerRaw,
// one ledger per record, zstd-compressed at the packfile record level.
// Resume is per-chunk: an existing destination .pack that opens via
// ColdStoreReader with the expected firstSeq is skipped; everything else
// is rebuilt.
//
// Scope: this is an exploratory driver used to seed cold-store data for
// the full-history RPC work. It does not touch the MetaStore, txhash, or
// events stores, and is not the eventual `stellar-rpc full-history-backfill`
// cobra subcommand specced in the backfill design doc — this is a thin
// CLI on top of the lower-level primitives so they can be exercised end
// to end against pubnet.
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

	"cloud.google.com/go/storage"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/option"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/datastore"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/zstd"
)

// chunksPerBucket — directory grouping. Hardcoded by design doc;
// not operator-configurable. Mirrors what fullhistory/backfill will
// eventually own.
const chunksPerBucket uint32 = 1_000

type cliOptions struct {
	startLedger   uint32
	endLedger     uint32
	outputDir     string
	bucketPath    string
	bsbBufferSize uint32
	bsbNumWorkers uint32
	chunkWorkers  int
	encodeWorkers int
	bytesPerSync  int
	anonymous     bool
}

func parseFlags() (cliOptions, error) {
	var (
		opts          cliOptions
		startLedger   uint64
		endLedger     uint64
		bsbBufferSize uint64
		bsbNumWorkers uint64
	)
	flag.Uint64Var(&startLedger, "start-ledger", 0,
		"first ledger to ingest (inclusive); expands outward to chunk boundary")
	flag.Uint64Var(&endLedger, "end-ledger", 0,
		"last ledger to ingest (inclusive); expands outward to chunk boundary")
	flag.StringVar(&opts.outputDir, "output-dir", "./ledgers",
		"output directory for cold-store ledger pack files")
	flag.StringVar(&opts.bucketPath, "bucket-path", "sdf-ledger-close-meta/v1/ledgers/pubnet",
		"GCS destination_bucket_path (no gs:// prefix)")
	flag.Uint64Var(&bsbBufferSize, "bsb-buffer-size", 1000,
		"BSB prefetch buffer depth per chunk worker")
	flag.Uint64Var(&bsbNumWorkers, "bsb-num-workers", 20,
		"BSB download workers per chunk worker")
	flag.IntVar(&opts.chunkWorkers, "chunk-workers", 4,
		"number of chunks processed in parallel (each owns its own BSB connection)")
	flag.IntVar(&opts.encodeWorkers, "encode-workers", 4,
		"per-chunk zstd encode workers (ColdWriterOptions.Concurrency)")
	flag.IntVar(&opts.bytesPerSync, "bytes-per-sync", 0,
		"non-blocking writeback granularity in bytes (ColdWriterOptions.BytesPerSync); 0 disables")
	flag.BoolVar(&opts.anonymous, "anonymous", false,
		"use unauthenticated GCS client (for public buckets like sdf-ledger-close-meta)")
	flag.Parse()

	if startLedger > uint64(^uint32(0)) || endLedger > uint64(^uint32(0)) {
		return opts, errors.New("--start-ledger / --end-ledger must fit in uint32")
	}
	if bsbBufferSize > uint64(^uint32(0)) || bsbNumWorkers > uint64(^uint32(0)) {
		return opts, errors.New("--bsb-buffer-size / --bsb-num-workers must fit in uint32")
	}
	opts.startLedger = uint32(startLedger)
	opts.endLedger = uint32(endLedger)
	opts.bsbBufferSize = uint32(bsbBufferSize)
	opts.bsbNumWorkers = uint32(bsbNumWorkers)

	if opts.startLedger < 2 {
		return opts, errors.New("--start-ledger must be >= 2")
	}
	if opts.endLedger <= opts.startLedger {
		return opts, errors.New("--end-ledger must be > --start-ledger")
	}
	if opts.outputDir == "" {
		return opts, errors.New("--output-dir is required")
	}
	if opts.bucketPath == "" {
		return opts, errors.New("--bucket-path is required")
	}
	if opts.chunkWorkers < 1 {
		return opts, errors.New("--chunk-workers must be >= 1")
	}
	if opts.encodeWorkers < 1 {
		return opts, errors.New("--encode-workers must be >= 1")
	}
	if opts.bytesPerSync < 0 {
		return opts, errors.New("--bytes-per-sync must be >= 0")
	}
	return opts, nil
}

// chunkIDForLedger maps ledger sequence → chunk id. Genesis is ledger 2,
// so chunk 0 covers [2, 10_001].
func chunkIDForLedger(seq uint32) uint32 {
	return (seq - 2) / chunk.LedgersPerChunk
}

func chunkFirstLedger(chunkID uint32) uint32 { return chunkID*chunk.LedgersPerChunk + 2 }
func chunkLastLedger(chunkID uint32) uint32  { return (chunkID+1)*chunk.LedgersPerChunk + 1 }
func bucketIDForChunk(chunkID uint32) uint32 { return chunkID / chunksPerBucket }

func packPath(outputDir string, chunkID uint32) string {
	return filepath.Join(
		outputDir,
		fmt.Sprintf("%05d", bucketIDForChunk(chunkID)),
		fmt.Sprintf("%08d.pack", chunkID),
	)
}

// chunkAlreadyDone reports whether the destination .pack exists, opens
// via ColdStoreReader, and carries the expected firstSeq. A missing file
// or one that fails any of the validations is treated as "needs rebuild";
// ColdStoreWriter.Overwrite handles the partial-rebuild path.
func chunkAlreadyDone(path string, expectedFirstSeq uint32, _ *zstd.Decompressor) bool {
	if _, err := os.Stat(path); err != nil {
		return false
	}
	r, err := ledger.NewColdStoreReader(path)
	if err != nil {
		return false
	}
	defer r.Close()
	first, err := r.FirstSeq()
	if err != nil {
		return false
	}
	last, err := r.LastSeq()
	if err != nil {
		return false
	}
	return first == expectedFirstSeq && last == expectedFirstSeq+chunk.LedgersPerChunk-1
}

func main() {
	opts, err := parseFlags()
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		flag.Usage()
		os.Exit(2)
	}

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	firstChunk := chunkIDForLedger(opts.startLedger)
	lastChunk := chunkIDForLedger(opts.endLedger)
	effectiveStart := chunkFirstLedger(firstChunk)
	effectiveEnd := chunkLastLedger(lastChunk)
	totalChunks := lastChunk - firstChunk + 1

	logger.Infof(
		"backfill range: requested [%d, %d] -> chunk-aligned [%d, %d] (chunks %d..%d, %d chunks total)",
		opts.startLedger, opts.endLedger,
		effectiveStart, effectiveEnd,
		firstChunk, lastChunk, totalChunks,
	)
	logger.Infof("output dir: %s", opts.outputDir)
	logger.Infof("source bucket: %s", opts.bucketPath)

	ds, schema, err := openDataStore(ctx, opts.bucketPath, opts.anonymous)
	if err != nil {
		logger.WithError(err).Error("datastore setup failed")
		os.Exit(1)
	}
	defer ds.Close()
	logger.Infof("datastore schema: LedgersPerFile=%d FilesPerPartition=%d FileExtension=%q",
		schema.LedgersPerFile, schema.FilesPerPartition, schema.FileExtension)

	if err := os.MkdirAll(opts.outputDir, 0o755); err != nil {
		logger.WithError(err).Errorf("could not create output dir %s", opts.outputDir)
		os.Exit(1)
	}

	runStart := time.Now()
	if err := runBackfill(ctx, logger, opts, ds, schema, firstChunk, lastChunk); err != nil {
		logger.WithError(err).Error("backfill failed")
		os.Exit(1)
	}
	logger.Infof("backfill complete: %d chunks in %s", totalChunks, time.Since(runStart).Round(time.Second))
}

func openDataStore(ctx context.Context, bucketPath string, anonymous bool) (datastore.DataStore, datastore.DataStoreSchema, error) {
	cfg := datastore.DataStoreConfig{
		Type: "GCS",
		Params: map[string]string{
			"destination_bucket_path": bucketPath,
		},
	}

	var ds datastore.DataStore
	var err error
	if anonymous {
		var client *storage.Client
		client, err = storage.NewClient(ctx, option.WithoutAuthentication())
		if err != nil {
			return nil, datastore.DataStoreSchema{}, fmt.Errorf("storage.NewClient(anonymous): %w", err)
		}
		ds, err = datastore.FromGCSClient(ctx, client, bucketPath)
	} else {
		ds, err = datastore.NewDataStore(ctx, cfg)
	}
	if err != nil {
		return nil, datastore.DataStoreSchema{}, fmt.Errorf("open datastore %q: %w", bucketPath, err)
	}

	schema, err := datastore.LoadSchema(ctx, ds, cfg)
	if err != nil {
		ds.Close()
		return nil, datastore.DataStoreSchema{}, fmt.Errorf("LoadSchema: %w", err)
	}
	return ds, schema, nil
}

// runBackfill walks chunks [firstChunk, lastChunk] inclusive through a
// bounded worker pool. First fatal error cancels the run; subsequent
// errors are dropped (first-wins).
func runBackfill(
	ctx context.Context,
	logger *supportlog.Entry,
	opts cliOptions,
	ds datastore.DataStore,
	schema datastore.DataStoreSchema,
	firstChunk, lastChunk uint32,
) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	totalChunks := lastChunk - firstChunk + 1

	// Shared decompressor for resume-check reads across all workers.
	// Concurrent-safe per the zstd package contract.
	resumeDec := zstd.NewDecompressor()

	chunkCh := make(chan uint32, opts.chunkWorkers*2)
	var (
		wg          sync.WaitGroup
		errOnce     sync.Once
		firstErr    error
		doneChunks  atomic.Uint32
		skipChunks  atomic.Uint32
		writeChunks atomic.Uint32
	)

	bsbCfg := ledgerbackend.BufferedStorageBackendConfig{
		BufferSize: opts.bsbBufferSize,
		NumWorkers: opts.bsbNumWorkers,
		RetryLimit: 3,
		RetryWait:  5 * time.Second,
	}

	for w := range opts.chunkWorkers {
		wg.Add(1)
		workerID := w
		go func() {
			defer wg.Done()
			workerLog := logger.WithField("worker", workerID)
			for chunkID := range chunkCh {
				if ctx.Err() != nil {
					return
				}
				wrote, err := processChunk(ctx, workerLog, opts, ds, schema, bsbCfg, resumeDec, chunkID)
				if err != nil {
					errOnce.Do(func() {
						firstErr = fmt.Errorf("chunk %d: %w", chunkID, err)
						cancel()
					})
					return
				}
				if wrote {
					writeChunks.Add(1)
				} else {
					skipChunks.Add(1)
				}
				done := doneChunks.Add(1)
				if done%10 == 0 || done == totalChunks {
					workerLog.Infof("progress: %d/%d chunks (%d written, %d skipped)",
						done, totalChunks, writeChunks.Load(), skipChunks.Load())
				}
			}
		}()
	}

	for chunkID := firstChunk; chunkID <= lastChunk; chunkID++ {
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
		return firstErr
	}
	if err := ctx.Err(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

// processChunk writes one cold-store .pack file. Returns (wrote, err):
// wrote=false means the chunk was already complete and the file was left
// alone.
func processChunk(
	ctx context.Context,
	logger *supportlog.Entry,
	opts cliOptions,
	ds datastore.DataStore,
	schema datastore.DataStoreSchema,
	bsbCfg ledgerbackend.BufferedStorageBackendConfig,
	resumeDec *zstd.Decompressor,
	chunkID uint32,
) (bool, error) {
	path := packPath(opts.outputDir, chunkID)
	first := chunkFirstLedger(chunkID)
	last := chunkLastLedger(chunkID)
	chunkLog := logger.WithField("chunk", chunkID)

	if chunkAlreadyDone(path, first, resumeDec) {
		chunkLog.Debugf("chunk %d already present at %s; skipping", chunkID, path)
		return false, nil
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return false, fmt.Errorf("mkdir %s: %w", filepath.Dir(path), err)
	}

	backend, err := ledgerbackend.NewBufferedStorageBackend(bsbCfg, ds, schema)
	if err != nil {
		return false, fmt.Errorf("NewBufferedStorageBackend: %w", err)
	}
	defer backend.Close()

	if err := backend.PrepareRange(ctx, ledgerbackend.BoundedRange(first, last)); err != nil {
		return false, fmt.Errorf("PrepareRange[%d,%d]: %w", first, last, err)
	}

	writer, err := ledger.NewColdStoreWriter(path, first, ledger.ColdWriterOptions{
		Concurrency:  opts.encodeWorkers,
		BytesPerSync: opts.bytesPerSync,
	})
	if err != nil {
		return false, fmt.Errorf("NewColdStoreWriter %s: %w", path, err)
	}
	// Close removes the partial file if Commit isn't called — fine for the
	// error path; on the happy path Commit runs and Close is a no-op.
	defer writer.Close()

	start := time.Now()
	var bytesIn int64
	for seq := first; seq <= last; seq++ {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		raw, err := backend.GetLedgerRaw(ctx, seq)
		if err != nil {
			return false, fmt.Errorf("GetLedgerRaw(%d): %w", seq, err)
		}
		bytesIn += int64(len(raw))
		if err := writer.AppendLedger(seq, raw); err != nil {
			return false, fmt.Errorf("AppendLedger(%d): %w", seq, err)
		}
	}

	if err := writer.Commit(); err != nil {
		return false, fmt.Errorf("commit %s: %w", path, err)
	}

	elapsed := time.Since(start)
	mbIn := float64(bytesIn) / (1 << 20)
	chunkLog.Infof("wrote %s: %d ledgers, %.1f MiB input, %s (%.1f MiB/s in)",
		path, last-first+1, mbIn, elapsed.Round(time.Millisecond),
		mbIn/elapsed.Seconds(),
	)
	return true, nil
}
