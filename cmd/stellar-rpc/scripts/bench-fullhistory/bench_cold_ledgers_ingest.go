package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/datastore"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
)

// cmdColdLedgersIngest benches end-to-end cold-tier packfile
// production. Each iteration produces ONE packfile (10k ledgers) by
// streaming from BSB into a ColdStoreWriter. Records two times per
// packfile:
//
//	total       wall-clock from BSB-open through writer.Commit (with BSB)
//	writer_only total minus time blocked in backend.GetLedgerRaw (no BSB)
//
// At --num-packfiles=1 just prints the two values. At >1, also prints
// p50/p90/p99/max across packfiles.
func cmdColdLedgersIngest() {
	fs := flag.NewFlagSet("cold-ledgers-ingest", flag.ExitOnError)
	targetDir := fs.String("target-dir", "", "output dir for new packfiles (required)")
	startChunk := fs.Uint("start-chunk", 0, "first chunk ID to produce (required)")
	numPackfiles := fs.Int("num-packfiles", 1, "how many packfiles to produce sequentially")
	bucketPath := fs.String("bucket-path", "sdf-ledger-close-meta/v1/ledgers/pubnet",
		"GCS destination_bucket_path (authenticated; relies on GOOGLE_APPLICATION_CREDENTIALS / ADC)")
	bsbBufferSize := fs.Uint("bsb-buffer-size", 5000, "BSB prefetch buffer depth")
	bsbNumWorkers := fs.Uint("bsb-num-workers", 50, "BSB download workers")
	retryLimit := fs.Uint("retry-limit", 3, "BSB retry attempts on transient backend failure")
	retryWait := fs.Duration("retry-wait", 5*time.Second, "BSB delay between retry attempts")
	packfileConcurrency := fs.Int("packfile-concurrency", 4, "ColdStoreWriter parallel zstd encoder workers")
	bytesPerSync := fs.Int("bytes-per-sync", 0, "non-blocking writeback granularity in bytes (0 disables)")
	outDir := fs.String("out", "bench-out", "CSV output dir")
	_ = fs.Parse(os.Args[1:])

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)

	if *targetDir == "" {
		fatal(logger, "--target-dir is required")
	}
	if *startChunk == 0 {
		fatal(logger, "--start-chunk is required")
	}
	if *numPackfiles < 1 {
		fatal(logger, "--num-packfiles must be >= 1, got %d", *numPackfiles)
	}
	if *bucketPath == "" {
		fatal(logger, "--bucket-path is required")
	}
	if *bsbBufferSize == 0 {
		fatal(logger, "--bsb-buffer-size must be >= 1")
	}
	if *bsbNumWorkers == 0 {
		fatal(logger, "--bsb-num-workers must be >= 1")
	}
	if *packfileConcurrency < 1 {
		fatal(logger, "--packfile-concurrency must be >= 1")
	}
	if *bytesPerSync < 0 {
		fatal(logger, "--bytes-per-sync must be >= 0")
	}
	if *retryWait < 0 {
		fatal(logger, "--retry-wait must be >= 0")
	}
	firstChunk := uint32(*startChunk)

	if err := os.MkdirAll(*targetDir, 0o755); err != nil {
		fatal(logger, "mkdir %s: %v", *targetDir, err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	ds, schema, err := openColdIngestDataStore(ctx, *bucketPath)
	if err != nil {
		fatal(logger, "datastore setup: %v", err)
	}
	defer ds.Close()

	logger.Infof("cold-ledgers-ingest target=%s start-chunk=%d num-packfiles=%d bucket=%s",
		*targetDir, firstChunk, *numPackfiles, *bucketPath)
	logger.Infof("datastore schema: LedgersPerFile=%d FilesPerPartition=%d FileExtension=%q",
		schema.LedgersPerFile, schema.FilesPerPartition, schema.FileExtension)

	bsbCfg := ledgerbackend.BufferedStorageBackendConfig{
		BufferSize: uint32(*bsbBufferSize),
		NumWorkers: uint32(*bsbNumWorkers),
		RetryLimit: uint32(*retryLimit),
		RetryWait:  *retryWait,
	}
	writerOpts := ledger.ColdWriterOptions{
		Concurrency:  *packfileConcurrency,
		BytesPerSync: *bytesPerSync,
	}

	csvF, csvPath, err := createCSV(*outDir, "cold-ledgers-ingest",
		"chunk,total_ms,writer_only_ms,blocked_ms,ledgers")
	if err != nil {
		fatal(logger, "%v", err)
	}
	defer csvF.Close()

	fmt.Printf("\n%-8s %-12s %-14s %-12s %-9s\n",
		"chunk", "total_ms", "writer_only_ms", "blocked_ms", "ledgers")
	fmt.Println(strings.Repeat("-", 70))

	totals := make([]time.Duration, 0, *numPackfiles)
	writerOnlys := make([]time.Duration, 0, *numPackfiles)
	for i := range *numPackfiles {
		chunkID := firstChunk + uint32(i)
		total, blocked, ledgers, perr := produceOnePackfile(
			ctx, *targetDir, chunkID, ds, schema, bsbCfg, writerOpts)
		if perr != nil {
			fatal(logger, "packfile chunk=%d: %v", chunkID, perr)
		}
		writerOnly := total - blocked
		totals = append(totals, total)
		writerOnlys = append(writerOnlys, writerOnly)

		totalMs := float64(total.Microseconds()) / 1000.0
		writerMs := float64(writerOnly.Microseconds()) / 1000.0
		blockedMs := float64(blocked.Microseconds()) / 1000.0
		fmt.Printf("%-8d %-12.1f %-14.1f %-12.1f %-9d\n",
			chunkID, totalMs, writerMs, blockedMs, ledgers)
		fmt.Fprintf(csvF, "%d,%.3f,%.3f,%.3f,%d\n",
			chunkID, totalMs, writerMs, blockedMs, ledgers)
	}

	if *numPackfiles > 1 {
		fmt.Println()
		fmt.Println("Summary (across packfiles):")
		printPackfileStats("total          ", totals)
		printPackfileStats("writer_only    ", writerOnlys)
	}
	logger.Infof("wrote %s", csvPath)
}

func openColdIngestDataStore(ctx context.Context, bucketPath string) (datastore.DataStore, datastore.DataStoreSchema, error) {
	cfg := datastore.DataStoreConfig{
		Type:   "GCS",
		Params: map[string]string{"destination_bucket_path": bucketPath},
	}
	ds, err := datastore.NewDataStore(ctx, cfg)
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

// produceOnePackfile builds one chunk's packfile from BSB. Returns
// (total wall time from BSB-open through Commit, time blocked in
// GetLedgerRaw, ledgers written).
func produceOnePackfile(
	ctx context.Context,
	targetDir string,
	chunkID uint32,
	ds datastore.DataStore,
	schema datastore.DataStoreSchema,
	bsbCfg ledgerbackend.BufferedStorageBackendConfig,
	writerOpts ledger.ColdWriterOptions,
) (total, blocked time.Duration, ledgers uint32, err error) {
	first := chunkFirstLedger(chunkID)
	last := chunkLastLedger(chunkID)
	path := packPath(targetDir, chunkID)

	if err = os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return 0, 0, 0, fmt.Errorf("mkdir %s: %w", filepath.Dir(path), err)
	}

	tStart := time.Now()

	backend, berr := ledgerbackend.NewBufferedStorageBackend(bsbCfg, ds, schema)
	if berr != nil {
		return 0, 0, 0, fmt.Errorf("NewBufferedStorageBackend: %w", berr)
	}
	defer backend.Close()

	// PrepareRange waits on the backend (sets up prefetch workers + may
	// fetch initial chunks). Count it as blocked-on-BSB so writer_only
	// reflects only the packfile-writing cost.
	tPrep := time.Now()
	if perr := backend.PrepareRange(ctx, ledgerbackend.BoundedRange(first, last)); perr != nil {
		return 0, 0, 0, fmt.Errorf("PrepareRange[%d,%d]: %w", first, last, perr)
	}
	blocked += time.Since(tPrep)

	writer, werr := ledger.NewColdStoreWriter(path, first, writerOpts)
	if werr != nil {
		return 0, 0, 0, fmt.Errorf("NewColdStoreWriter %s: %w", path, werr)
	}
	// Close cleans up a partial pack if Commit didn't run; no-op after Commit.
	defer writer.Close()

	for seq := first; seq <= last; seq++ {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return 0, 0, 0, ctxErr
		}
		tWait := time.Now()
		raw, gerr := backend.GetLedgerRaw(ctx, seq)
		blocked += time.Since(tWait)
		if gerr != nil {
			return 0, 0, 0, fmt.Errorf("GetLedgerRaw(%d): %w", seq, gerr)
		}
		if aerr := writer.AppendLedger(seq, raw); aerr != nil {
			return 0, 0, 0, fmt.Errorf("AppendLedger(%d): %w", seq, aerr)
		}
		ledgers++
	}

	if cerr := writer.Commit(); cerr != nil {
		return 0, 0, 0, fmt.Errorf("commit %s: %w", path, cerr)
	}

	total = time.Since(tStart)
	return total, blocked, ledgers, nil
}

// printPackfileStats prints p50/p90/p99/max across a slice of
// packfile-level durations. Used for the across-packfiles summary
// when --num-packfiles>1.
func printPackfileStats(label string, durs []time.Duration) {
	if len(durs) == 0 {
		return
	}
	s := computeStats(durs)
	fmt.Printf("%s n=%d p50=%s p90=%s p99=%s max=%s\n",
		label, s.n,
		s.p50.Round(time.Millisecond),
		s.p90.Round(time.Millisecond),
		s.p99.Round(time.Millisecond),
		s.maxv.Round(time.Millisecond),
	)
}
