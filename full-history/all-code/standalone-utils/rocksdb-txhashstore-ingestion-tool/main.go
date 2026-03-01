package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/stellar/stellar-rpc/full-history/all-code/helpers"
	"github.com/stellar/stellar-rpc/full-history/all-code/helpers/lfs"
	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/cf"
	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/interfaces"
	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/logging"
	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/memory"
	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/store"
	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/types"
	"github.com/klauspost/compress/zstd"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/support/datastore"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// Architecture constants
const (
	BatchSize                  = 5000
	NumLfsWorkers              = 16
	NumLfsReaders              = 4
	WorkChanBuffer             = 200
	EntryChanBuffer            = 100
	ProgressInterval           = 60 * time.Second
	DefaultGCSBufferSize       = 1000
	DefaultGCSNumWorkers       = 20
	DefaultGCSBucketPath       = "sdf-ledger-close-meta/v1/ledgers/pubnet"
	DefaultGCSParallelBackends = 10
	DefaultGCSBatchSize        = 10000
)

// LedgerWork represents compressed ledger data ready for processing
type LedgerWork struct {
	LedgerSeq      uint32
	CompressedData []byte
}

// LedgerEntries represents extracted entries from a single ledger
type LedgerEntries struct {
	LedgerSeq   uint32
	EntriesByCF map[string][]types.Entry
	TxCount     int
}

// copyBytes creates a copy of the byte slice
func copyBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

// extractTxHashesFromLCM extracts transaction hashes from a LedgerCloseMeta
func extractTxHashesFromLCM(lcm xdr.LedgerCloseMeta, ledgerSeq uint32) (map[string][]types.Entry, int, error) {
	entriesByCF := make(map[string][]types.Entry)
	for _, cfName := range cf.Names {
		entriesByCF[cfName] = make([]types.Entry, 0, 32)
	}

	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(
		network.PublicNetworkPassphrase, lcm)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create tx reader for ledger %d: %w", ledgerSeq, err)
	}
	defer txReader.Close()

	ledgerSeqBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(ledgerSeqBytes, ledgerSeq)

	txCount := 0
	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, 0, fmt.Errorf("failed to read tx from ledger %d: %w", ledgerSeq, err)
		}

		txHash := tx.Result.TransactionHash[:]
		cfName := cf.GetName(txHash)

		entriesByCF[cfName] = append(entriesByCF[cfName], types.Entry{
			Key:   copyBytes(txHash),
			Value: copyBytes(ledgerSeqBytes),
		})
		txCount++
	}

	return entriesByCF, txCount, nil
}

// reader reads compressed ledger data from LFS
func reader(
	id int,
	lfsPath string,
	startSeq, endSeq uint32,
	workChan chan<- LedgerWork,
	errChan chan<- error,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	iterator, err := lfs.NewLFSRawLedgerIterator(lfsPath, startSeq, endSeq)
	if err != nil {
		select {
		case errChan <- fmt.Errorf("reader %d: failed to create iterator: %w", id, err):
		default:
		}
		return
	}
	defer iterator.Close()

	for {
		data, hasMore, err := iterator.Next()
		if err != nil {
			select {
			case errChan <- fmt.Errorf("reader %d: failed to read ledger: %w", id, err):
			default:
			}
			return
		}
		if !hasMore {
			break
		}

		workChan <- LedgerWork{
			LedgerSeq:      data.LedgerSeq,
			CompressedData: data.CompressedData,
		}
	}
}

// worker processes ledger data (decompress, unmarshal, extract)
func worker(
	id int,
	workChan <-chan LedgerWork,
	entryChan chan<- LedgerEntries,
	errChan chan<- error,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	decoder, err := zstd.NewReader(nil)
	if err != nil {
		select {
		case errChan <- fmt.Errorf("worker %d: failed to create zstd decoder: %w", id, err):
		default:
		}
		return
	}
	defer decoder.Close()

	ledgerSeqBytes := make([]byte, 4)

	for work := range workChan {
		// 1. Decompress
		uncompressed, err := decoder.DecodeAll(work.CompressedData, nil)
		if err != nil {
			select {
			case errChan <- fmt.Errorf("worker %d: decompress failed for ledger %d: %w", id, work.LedgerSeq, err):
			default:
			}
			return
		}

		// 2. Unmarshal XDR
		var lcm xdr.LedgerCloseMeta
		if err := lcm.UnmarshalBinary(uncompressed); err != nil {
			select {
			case errChan <- fmt.Errorf("worker %d: unmarshal failed for ledger %d: %w", id, work.LedgerSeq, err):
			default:
			}
			return
		}

		// 3. Extract transaction hashes
		entriesByCF := make(map[string][]types.Entry)
		for _, cfName := range cf.Names {
			entriesByCF[cfName] = make([]types.Entry, 0, 32)
		}

		txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(
			network.PublicNetworkPassphrase, lcm)
		if err != nil {
			select {
			case errChan <- fmt.Errorf("worker %d: failed to create tx reader for ledger %d: %w", id, work.LedgerSeq, err):
			default:
			}
			return
		}

		binary.BigEndian.PutUint32(ledgerSeqBytes, work.LedgerSeq)

		txCount := 0
		for {
			tx, err := txReader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				txReader.Close()
				select {
				case errChan <- fmt.Errorf("worker %d: failed to read tx from ledger %d: %w", id, work.LedgerSeq, err):
				default:
				}
				return
			}

			txHash := tx.Result.TransactionHash[:]
			cfName := cf.GetName(txHash)

			entriesByCF[cfName] = append(entriesByCF[cfName], types.Entry{
				Key:   copyBytes(txHash),
				Value: copyBytes(ledgerSeqBytes),
			})
			txCount++
		}
		txReader.Close()

		entryChan <- LedgerEntries{
			LedgerSeq:   work.LedgerSeq,
			EntriesByCF: entriesByCF,
			TxCount:     txCount,
		}
	}
}

// processBatchLFS processes a single batch of ledgers using the Reader→Worker→Collector pipeline
func processBatchLFS(
	lfsPath string,
	batchStart, batchEnd uint32,
	numWorkers, numReaders int,
	txStore interfaces.TxHashStore,
	logger interfaces.Logger,
) (int64, time.Duration, error) {
	workChan := make(chan LedgerWork, WorkChanBuffer)
	entryChan := make(chan LedgerEntries, EntryChanBuffer)
	errChan := make(chan error, numReaders+numWorkers)

	entriesByCF := make(map[string][]types.Entry)
	for _, cfName := range cf.Names {
		entriesByCF[cfName] = make([]types.Entry, 0)
	}

	var totalTxCount int64
	var readerWg, workerWg sync.WaitGroup

	// Start workers first (before readers, so they're ready)
	for w := 0; w < numWorkers; w++ {
		workerWg.Add(1)
		go worker(w, workChan, entryChan, errChan, &workerWg)
	}

	// Calculate ledger distribution across readers
	batchLedgers := int(batchEnd - batchStart + 1)
	ledgersPerReader := batchLedgers / numReaders
	remainder := batchLedgers % numReaders

	// Start readers
	readerStart := batchStart
	for r := 0; r < numReaders; r++ {
		count := ledgersPerReader
		if r < remainder {
			count++
		}
		readerEnd := readerStart + uint32(count) - 1
		readerWg.Add(1)
		go reader(r, lfsPath, readerStart, readerEnd, workChan, errChan, &readerWg)
		readerStart = readerEnd + 1
	}

	// Start collector
	collectorDone := make(chan struct{})
	go func() {
		defer close(collectorDone)
		for entries := range entryChan {
			for cfName, cfEntries := range entries.EntriesByCF {
				entriesByCF[cfName] = append(entriesByCF[cfName], cfEntries...)
			}
			totalTxCount += int64(entries.TxCount)
		}
	}()

	// Wait sequence
	readerWg.Wait()
	close(workChan)

	workerWg.Wait()
	close(entryChan)

	<-collectorDone

	// Check for errors
	select {
	case err := <-errChan:
		return 0, 0, err
	default:
	}

	// Write batch to RocksDB and time it
	writeStart := time.Now()
	if _, err := txStore.WriteBatch(entriesByCF); err != nil {
		return 0, 0, fmt.Errorf("failed to write batch: %w", err)
	}
	writeDuration := time.Since(writeStart)

	return totalTxCount, writeDuration, nil
}

func runLFSIngestion(
	lfsPath string,
	startLedger, endLedger uint32,
	numWorkers, numReaders int,
	txStore interfaces.TxHashStore,
	logger interfaces.Logger,
	memMonitor *memory.MemoryMonitor,
) error {
	startTime := time.Now()
	totalLedgers := int64(endLedger - startLedger + 1)
	ledgersCompleted := atomic.Int64{}
	txHashesFound := atomic.Int64{}

	logger.Separator()
	logger.Info("                    STARTING INGESTION (PARALLEL)")
	logger.Separator()
	logger.Info("")

	// Process ledgers in batches
	currentLedger := startLedger
	var hasError bool

	// Track WriteBatch timing for progress logs
	var writeBatchTotalDuration time.Duration
	var recentWriteBatchTime time.Duration
	var recentWriteBatchCount int64

	onePercent := totalLedgers / 100
	if onePercent == 0 {
		onePercent = 1
	}
	lastProgressPercent := int64(0)

	for currentLedger <= endLedger {
		batchStart := currentLedger
		batchEnd := currentLedger + uint32(BatchSize) - 1
		if batchEnd > endLedger {
			batchEnd = endLedger
		}

		txCount, writeDuration, err := processBatchLFS(lfsPath, batchStart, batchEnd, numWorkers, numReaders, txStore, logger)
		if err != nil {
			logger.Error("Failed to process batch %d-%d: %v", batchStart, batchEnd, err)
			hasError = true
			break
		}

		ledgersCompleted.Add(int64(batchEnd - batchStart + 1))
		txHashesFound.Add(txCount)
		writeBatchTotalDuration += writeDuration
		recentWriteBatchTime += writeDuration
		recentWriteBatchCount++

		currentCompleted := ledgersCompleted.Load()
		if currentCompleted > 0 {
			currentPercent := currentCompleted / onePercent
			if currentPercent > lastProgressPercent {
				elapsed := time.Since(startTime)
				rate := float64(currentCompleted) / elapsed.Seconds()
				remainingLedgers := totalLedgers - currentCompleted
				etaSeconds := time.Duration(int64(float64(remainingLedgers)/rate)) * time.Second

				avgWriteBatch := time.Duration(int64(recentWriteBatchTime) / recentWriteBatchCount)

				logger.Info("[PROGRESS] Ledgers: %s/%d (%d%%) | Rate: %.0f/s | WriteBatch avg: %s | ETA: %s",
					helpers.FormatNumber(currentCompleted),
					totalLedgers,
					currentPercent,
					rate,
					helpers.FormatDuration(avgWriteBatch),
					helpers.FormatDuration(etaSeconds),
				)

				// Reset recent tracking for next 1% boundary
				recentWriteBatchTime = 0
				recentWriteBatchCount = 0
				lastProgressPercent = currentPercent
			}
		}

		currentLedger = batchEnd + 1
	}

	if hasError {
		logger.Error("Ingestion failed")
		return fmt.Errorf("ingestion failed")
	}

	totalElapsed := time.Since(startTime)
	totalTxHashes := txHashesFound.Load()
	txHashRate := float64(totalTxHashes) / totalElapsed.Seconds()

	logger.Separator()
	logger.Info("                    INGESTION COMPLETE")
	logger.Separator()
	logger.Info("")
	logger.Info("Statistics:")
	logger.Info("  Total Ledgers:     %d", totalLedgers)
	logger.Info("  Total TxHashes:    %s", helpers.FormatNumber(totalTxHashes))
	logger.Info("  Duration:          %s", helpers.FormatDuration(totalElapsed))
	logger.Info("  WriteBatch Total:  %s", helpers.FormatDuration(writeBatchTotalDuration))
	logger.Info("  Throughput:        %s/s", helpers.FormatNumber(int64(txHashRate)))
	logger.Info("  Memory (RSS):      %.2f GB", memMonitor.CurrentRSSGB())
	logger.Info("")

	return nil
}

func runGCSIngestion(
	ctx context.Context,
	gcsBucketPath string,
	gcsBufferSize, gcsNumWorkers int,
	parallelBackends, batchSize int,
	startLedger, endLedger uint32,
	txStore interfaces.TxHashStore,
	logger interfaces.Logger,
	memMonitor *memory.MemoryMonitor,
) error {
	startTime := time.Now()
	totalLedgers := int64(endLedger - startLedger + 1)

	actualWorkers := parallelBackends
	if int64(actualWorkers) > totalLedgers {
		actualWorkers = int(totalLedgers)
	}

	logger.Separator()
	logger.Info("                    STARTING INGESTION (GCS - PARALLEL)")
	logger.Separator()
	logger.Info("")
	logger.Info("Configuration:")
	logger.Info("  Ledger Range:      %d to %d (%d total)", startLedger, endLedger, totalLedgers)
	logger.Info("  Parallel Backends: %d (requested: %d)", actualWorkers, parallelBackends)
	logger.Info("  Batch Size:        %d ledgers", batchSize)
	logger.Info("  GCS Buffer Size:   %d", gcsBufferSize)
	logger.Info("  GCS Workers:       %d", gcsNumWorkers)
	logger.Info("")

	ledgersPerWorker := int(totalLedgers) / actualWorkers
	remainder := int(totalLedgers) % actualWorkers

	type workerRange struct {
		id    int
		start uint32
		end   uint32
	}

	ranges := make([]workerRange, actualWorkers)
	currentStart := startLedger

	for i := 0; i < actualWorkers; i++ {
		workerCount := ledgersPerWorker
		if i < remainder {
			workerCount++
		}

		ranges[i] = workerRange{
			id:    i,
			start: currentStart,
			end:   currentStart + uint32(workerCount) - 1,
		}
		currentStart += uint32(workerCount)
	}

	logger.Info("Worker Sub-Ranges:")
	for _, r := range ranges {
		count := r.end - r.start + 1
		logger.Info("  Worker %d: ledgers %d to %d (%d ledgers)", r.id, r.start, r.end, count)
	}
	logger.Info("")

	datastoreConfig := datastore.DataStoreConfig{
		Type:   "GCS",
		Params: map[string]string{"destination_bucket_path": gcsBucketPath},
	}
	dataStoreSchema := datastore.DataStoreSchema{
		LedgersPerFile:    1,
		FilesPerPartition: 64000,
	}
	backendConfig := ledgerbackend.BufferedStorageBackendConfig{
		BufferSize: uint32(gcsBufferSize),
		NumWorkers: uint32(gcsNumWorkers),
		RetryLimit: 3,
		RetryWait:  5 * time.Second,
	}

	logger.Info("Creating %d BufferedStorageBackend instances...", actualWorkers)

	type backendPair struct {
		backend   *ledgerbackend.BufferedStorageBackend
		dataStore datastore.DataStore
	}
	backends := make([]backendPair, actualWorkers)

	for i := 0; i < actualWorkers; i++ {
		dataStore, err := datastore.NewDataStore(ctx, datastoreConfig)
		if err != nil {
			for j := 0; j < i; j++ {
				backends[j].backend.Close()
			}
			return fmt.Errorf("failed to create GCS datastore for worker %d: %w", i, err)
		}

		backend, err := ledgerbackend.NewBufferedStorageBackend(backendConfig, dataStore, dataStoreSchema)
		if err != nil {
			for j := 0; j < i; j++ {
				backends[j].backend.Close()
			}
			return fmt.Errorf("failed to create GCS backend for worker %d: %w", i, err)
		}

		backends[i] = backendPair{backend: backend, dataStore: dataStore}
	}
	logger.Info("All %d backends created successfully", actualWorkers)

	logger.Info("Calling PrepareRange for each backend...")
	for i := 0; i < actualWorkers; i++ {
		ledgerRange := ledgerbackend.BoundedRange(ranges[i].start, ranges[i].end)
		if err := backends[i].backend.PrepareRange(ctx, ledgerRange); err != nil {
			for j := 0; j < actualWorkers; j++ {
				backends[j].backend.Close()
			}
			return fmt.Errorf("failed to prepare range for worker %d (%d-%d): %w", i, ranges[i].start, ranges[i].end, err)
		}
	}
	logger.Info("All %d backends prepared successfully", actualWorkers)
	logger.Info("")

	defer func() {
		for _, bp := range backends {
			bp.backend.Close()
		}
	}()

	ledgersCompleted := atomic.Int64{}
	txHashesFound := atomic.Int64{}

	// Timing metrics for enhanced logging (rolling averages)
	totalGetLedgerNanos := atomic.Int64{}
	getLedgerCallCount := atomic.Int64{}
	totalWriteBatchNanos := atomic.Int64{}
	writeBatchCallCount := atomic.Int64{}

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errChan := make(chan error, actualWorkers)

	var wg sync.WaitGroup

	logger.Info("Starting %d parallel workers...", actualWorkers)
	for i := 0; i < actualWorkers; i++ {
		wg.Add(1)
		go func(workerID int, workerStart, workerEnd uint32, backend *ledgerbackend.BufferedStorageBackend) {
			defer wg.Done()

			entriesByCF := make(map[string][]types.Entry)
			for _, cfName := range cf.Names {
				entriesByCF[cfName] = make([]types.Entry, 0)
			}
			var batchTxCount int64
			var batchLedgerCount int64

			for ledgerSeq := workerStart; ledgerSeq <= workerEnd; ledgerSeq++ {
				select {
				case <-workerCtx.Done():
					return
				default:
				}

				getLedgerStart := time.Now()
				ledger, err := backend.GetLedger(workerCtx, ledgerSeq)
				totalGetLedgerNanos.Add(time.Since(getLedgerStart).Nanoseconds())
				getLedgerCallCount.Add(1)
				if err != nil {
					select {
					case errChan <- fmt.Errorf("worker %d failed to get ledger %d: %w", workerID, ledgerSeq, err):
					default:
					}
					cancel()
					return
				}

				entries, txCount, err := extractTxHashesFromLCM(ledger, ledgerSeq)
				if err != nil {
					select {
					case errChan <- fmt.Errorf("worker %d failed to extract hashes from ledger %d: %w", workerID, ledgerSeq, err):
					default:
					}
					cancel()
					return
				}

				for cfName, cfEntries := range entries {
					entriesByCF[cfName] = append(entriesByCF[cfName], cfEntries...)
				}
				batchTxCount += int64(txCount)
				batchLedgerCount++

				shouldWriteBatch := false
				ledgersProcessed := int(ledgerSeq - workerStart + 1)

				if ledgersProcessed%batchSize == 0 {
					shouldWriteBatch = true
				} else if ledgerSeq == workerEnd {
					shouldWriteBatch = true
				}

				if shouldWriteBatch && len(entriesByCF[cf.Names[0]]) > 0 {
					writeBatchStart := time.Now()
					if _, err := txStore.WriteBatch(entriesByCF); err != nil {
						select {
						case errChan <- fmt.Errorf("worker %d failed to write batch: %w", workerID, err):
						default:
						}
						cancel()
						return
					}
					totalWriteBatchNanos.Add(time.Since(writeBatchStart).Nanoseconds())
					writeBatchCallCount.Add(1)

					ledgersCompleted.Add(batchLedgerCount)
					txHashesFound.Add(batchTxCount)

					entriesByCF = make(map[string][]types.Entry)
					for _, cfName := range cf.Names {
						entriesByCF[cfName] = make([]types.Entry, 0)
					}
					batchTxCount = 0
					batchLedgerCount = 0
				}
			}

			if len(entriesByCF[cf.Names[0]]) > 0 {
				writeBatchStart := time.Now()
				if _, err := txStore.WriteBatch(entriesByCF); err != nil {
					select {
					case errChan <- fmt.Errorf("worker %d failed to write final batch: %w", workerID, err):
					default:
					}
					cancel()
					return
				}
				totalWriteBatchNanos.Add(time.Since(writeBatchStart).Nanoseconds())
				writeBatchCallCount.Add(1)

				ledgersCompleted.Add(batchLedgerCount)
				txHashesFound.Add(batchTxCount)
			}
		}(ranges[i].id, ranges[i].start, ranges[i].end, backends[i].backend)
	}

	progressDone := make(chan struct{})
	go func() {
		defer close(progressDone)

		ticker := time.NewTicker(ProgressInterval)
		defer ticker.Stop()

		for {
			select {
			case <-workerCtx.Done():
				return
			case <-ticker.C:
				currentCompleted := ledgersCompleted.Load()
				if currentCompleted > 0 {
					elapsed := time.Since(startTime)
					rate := float64(currentCompleted) / elapsed.Seconds()
					remainingLedgers := totalLedgers - currentCompleted
					etaSeconds := time.Duration(int64(float64(remainingLedgers)/rate)) * time.Second
					currentPercent := (currentCompleted * 100) / totalLedgers

					// Calculate rolling averages (since last log)
					avgGetLedger := time.Duration(0)
					if count := getLedgerCallCount.Load(); count > 0 {
						avgGetLedger = time.Duration(totalGetLedgerNanos.Load() / count)
					}
					avgWriteBatch := time.Duration(0)
					if count := writeBatchCallCount.Load(); count > 0 {
						// Divide by actualWorkers to estimate serial write time per batch
						// (parallel workers inflate wall-clock time due to RocksDB internal contention)
						avgWriteBatch = time.Duration(totalWriteBatchNanos.Load() / count / int64(actualWorkers))
					}

					logger.Info("[PROGRESS] Elapsed: %s | Ledgers: %s/%d (%d%%) | Rate: %s/s | GetLedger avg: %s | WriteBatch avg: %s (%d ledger batch) | ETA: %s | TxHashes: %s",
						helpers.FormatDuration(elapsed),
						helpers.FormatNumber(currentCompleted),
						totalLedgers,
						currentPercent,
						helpers.FormatFloat(rate, 2),
						helpers.FormatDuration(avgGetLedger),
						helpers.FormatDuration(avgWriteBatch),
						batchSize,
						helpers.FormatDuration(etaSeconds),
						helpers.FormatNumber(txHashesFound.Load()),
					)

					// Reset timing counters for next rolling window
					totalGetLedgerNanos.Store(0)
					getLedgerCallCount.Store(0)
					totalWriteBatchNanos.Store(0)
					writeBatchCallCount.Store(0)
				}
			}
		}
	}()

	wg.Wait()
	cancel()

	<-progressDone

	select {
	case err := <-errChan:
		return err
	default:
	}

	totalElapsed := time.Since(startTime)
	totalTxHashes := txHashesFound.Load()
	finalLedgersCompleted := ledgersCompleted.Load()
	txHashRate := float64(totalTxHashes) / totalElapsed.Seconds()
	ledgerRate := float64(finalLedgersCompleted) / totalElapsed.Seconds()

	logger.Separator()
	logger.Info("                    INGESTION COMPLETE")
	logger.Separator()
	logger.Info("")
	logger.Info("Statistics:")
	logger.Info("  Total Ledgers:     %s", helpers.FormatNumber(finalLedgersCompleted))
	logger.Info("  Total TxHashes:    %s", helpers.FormatNumber(totalTxHashes))
	logger.Info("  Duration:          %s", helpers.FormatDuration(totalElapsed))
	logger.Info("  Ledger Rate:       %.0f/s", ledgerRate)
	logger.Info("  TxHash Rate:       %s/s", helpers.FormatNumber(int64(txHashRate)))
	logger.Info("  Memory (RSS):      %.2f GB", memMonitor.CurrentRSSGB())
	logger.Info("")

	return nil
}

func main() {
	// === Mode Selection (mutually exclusive) ===
	useLFS := flag.Bool("use-lfs", false, "Use LFS (Local File System) as data source")
	useGCS := flag.Bool("use-gcs", false, "Use GCS (Google Cloud Storage) as data source")

	// === Required for all modes ===
	startLedger := flag.Uint64("start-ledger", 0, "First ledger to ingest (required)")
	endLedger := flag.Uint64("end-ledger", 0, "Last ledger to ingest (required)")
	outputDir := flag.String("output-dir", "", "Base output directory (required)")
	logFile := flag.String("log-file", "", "Path to log file (required)")
	errorFile := flag.String("error-file", "", "Path to error file (required)")

	// === LFS mode options ===
	lfsStore := flag.String("lfs-store", "", "[LFS] Path to LFS ledger store (required with --use-lfs)")
	numLfsWorkers := flag.Int("lfs-workers", NumLfsWorkers, fmt.Sprintf("[LFS] Number of workers for decompress/unmarshal/extract (default %d)", NumLfsWorkers))
	numLfsReaders := flag.Int("lfs-readers", NumLfsReaders, fmt.Sprintf("[LFS] Number of parallel LFS I/O readers (default %d)", NumLfsReaders))

	// === GCS mode options ===
	gcsBucketPath := flag.String("gcs-bucket-path", DefaultGCSBucketPath, fmt.Sprintf("[GCS] Bucket path (default: %s)", DefaultGCSBucketPath))
	gcsBufferSize := flag.Int("gcs-buffer-size", DefaultGCSBufferSize, fmt.Sprintf("[GCS] BufferedStorageBackend buffer size (default %d)", DefaultGCSBufferSize))
	gcsNumWorkers := flag.Int("gcs-workers", DefaultGCSNumWorkers, fmt.Sprintf("[GCS] BufferedStorageBackend num workers (default %d)", DefaultGCSNumWorkers))
	gcsParallelBackends := flag.Int("gcs-parallel-backends", DefaultGCSParallelBackends, fmt.Sprintf("[GCS] Number of parallel backends, each handles a ledger sub-range (default %d)", DefaultGCSParallelBackends))
	gcsBatchSize := flag.Int("gcs-batch-size", DefaultGCSBatchSize, fmt.Sprintf("[GCS] Ledgers per RocksDB WriteBatch (default %d)", DefaultGCSBatchSize))

	// === RocksDB options ===
	disableWAL := flag.Bool("disable-wal", false, "[RocksDB] Disable Write-Ahead Log for writes (faster ingestion, less crash safety)")

	// === Flush-only mode (special mode, skips ingestion) ===
	flushOnly := flag.Bool("flush-only", false, "[Special] Flush existing RocksDB store and exit (no ingestion)")
	rocksdbPath := flag.String("rocksdb-path", "", "[Special] Path to existing RocksDB store (required for --flush-only)")

	flag.Parse()

	// Handle flush-only mode separately
	if *flushOnly {
		if *rocksdbPath == "" {
			fmt.Fprintln(os.Stderr, "Error: --rocksdb-path is required when using --flush-only")
			os.Exit(1)
		}
		if *logFile == "" || *errorFile == "" {
			fmt.Fprintln(os.Stderr, "Error: --log-file and --error-file are required")
			os.Exit(1)
		}

		logger, err := logging.NewDualLogger(*logFile, *errorFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating logger: %v\n", err)
			os.Exit(1)
		}
		defer logger.Close()

		logger.Separator()
		logger.Info("                    FLUSH-ONLY MODE")
		logger.Separator()
		logger.Info("")
		logger.Info("RocksDB Path: %s", *rocksdbPath)
		logger.Info("")

		// Open existing store in read-write mode
		settings := types.DefaultRocksDBSettings()
		settings.ReadOnly = false

		openStart := time.Now()
		txStore, err := store.OpenRocksDBTxHashStore(*rocksdbPath, &settings, logger)
		if err != nil {
			logger.Error("Failed to open RocksDB: %v", err)
			os.Exit(1)
		}
		logger.Info("RocksDB opened in %s", helpers.FormatDuration(time.Since(openStart)))
		defer txStore.Close()

		// Show stats BEFORE flush
		txStore.LogMemTableAndWALStats(logger, *rocksdbPath, "BEFORE FLUSH")

		// Flush all MemTables to SST files
		logger.Info("Flushing all MemTables to disk...")
		flushStart := time.Now()
		if err := txStore.FlushAll(); err != nil {
			logger.Error("Failed to flush MemTables: %v", err)
			os.Exit(1)
		}
		logger.Info("Flush completed in %s", helpers.FormatDuration(time.Since(flushStart)))

		// Show stats AFTER flush
		txStore.LogMemTableAndWALStats(logger, *rocksdbPath, "AFTER FLUSH")

		logger.Info("")
		logger.Info("Flush-only mode complete. WAL should now be minimal.")
		logger.Sync()
		return
	}

	// Mode validation: exactly one of --use-lfs or --use-gcs must be specified
	if !*useLFS && !*useGCS {
		fmt.Fprintln(os.Stderr, "Error: must specify either --use-lfs or --use-gcs")
		flag.Usage()
		os.Exit(1)
	}
	if *useLFS && *useGCS {
		fmt.Fprintln(os.Stderr, "Error: cannot specify both --use-lfs and --use-gcs (choose one)")
		flag.Usage()
		os.Exit(1)
	}

	// Validate LFS-specific requirements
	if *useLFS && *lfsStore == "" {
		fmt.Fprintln(os.Stderr, "Error: --lfs-store is required when using --use-lfs")
		flag.Usage()
		os.Exit(1)
	}

	if *startLedger == 0 || *endLedger == 0 ||
		*outputDir == "" || *logFile == "" || *errorFile == "" {
		fmt.Fprintln(os.Stderr, "Error: --start-ledger, --end-ledger, --output-dir, --log-file, and --error-file are required")
		flag.Usage()
		os.Exit(1)
	}

	// Validate GCS-specific flags
	if *useGCS {
		if *gcsParallelBackends < 1 {
			fmt.Fprintln(os.Stderr, "Error: --gcs-parallel-backends must be >= 1")
			os.Exit(1)
		}
		if *gcsBatchSize < 100 {
			fmt.Fprintln(os.Stderr, "Error: --gcs-batch-size must be >= 100")
			os.Exit(1)
		}
	}

	logger, err := logging.NewDualLogger(*logFile, *errorFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Close()

	rockdbPath := filepath.Join(*outputDir, "rocksdb")
	if helpers.FileExists(rockdbPath) {
		fmt.Fprintf(os.Stderr, "Error: RocksDB store already exists at %s\n", rockdbPath)
		os.Exit(1)
	}

	if err := os.MkdirAll(*outputDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating output directory: %v\n", err)
		os.Exit(1)
	}

	logger.Separator()
	logger.Info("                    SIMPLE HASHSTORE INGESTION TOOL")
	logger.Separator()
	logger.Info("")
	logger.Info("Configuration:")
	logger.Info("  Start Ledger:    %d", *startLedger)
	logger.Info("  End Ledger:      %d", *endLedger)
	logger.Info("  Output Dir:      %s", *outputDir)
	if *useGCS {
		logger.Info("  Mode:            GCS (Google Cloud Storage)")
		logger.Info("  Bucket Path:     %s", *gcsBucketPath)
		logger.Info("  Buffer Size:     %d", *gcsBufferSize)
		logger.Info("  GCS Workers:     %d", *gcsNumWorkers)
		logger.Info("  Parallel Backends: %d", *gcsParallelBackends)
		logger.Info("  Batch Size:      %d ledgers", *gcsBatchSize)
	} else {
		logger.Info("  Mode:            LFS (Local File System)")
		logger.Info("  LFS Store:       %s", *lfsStore)
		logger.Info("  Workers:         %d (decompress/unmarshal/extract)", *numLfsWorkers)
		logger.Info("  Readers:         %d (LFS I/O)", *numLfsReaders)
		logger.Info("  Batch Size:      %d ledgers", BatchSize)
	}

	logger.Info("")

	memMonitor := memory.NewMemoryMonitor(logger, memory.DefaultRAMWarningThresholdGB)
	defer memMonitor.Stop()

	settings := types.DefaultRocksDBSettings()
	settings.ReadOnly = false
	settings.DisableWAL = *disableWAL

	txStore, err := store.OpenRocksDBTxHashStore(rockdbPath, &settings, logger)
	if err != nil {
		logger.Error("Failed to open RocksDB: %v", err)
		os.Exit(1)
	}
	defer txStore.Close()

	logger.Info("RocksDB store created with 16 column families")
	logger.Info("")

	var ingestionErr error
	if *useGCS {
		ctx := context.Background()
		ingestionErr = runGCSIngestion(ctx, *gcsBucketPath, *gcsBufferSize, *gcsNumWorkers,
			*gcsParallelBackends, *gcsBatchSize,
			uint32(*startLedger), uint32(*endLedger), txStore, logger, memMonitor)
	} else {
		ingestionErr = runLFSIngestion(*lfsStore, uint32(*startLedger), uint32(*endLedger),
			*numLfsWorkers, *numLfsReaders, txStore, logger, memMonitor)
	}

	if ingestionErr != nil {
		logger.Error("Ingestion failed: %v", ingestionErr)
		os.Exit(1)
	}

	// Flush all MemTables to SST files before closing
	// This ensures WAL can be cleaned up and no recovery is needed on next open
	logger.Info("")
	logger.Info("Flushing all MemTables to disk...")
	flushStart := time.Now()
	if err := txStore.FlushAll(); err != nil {
		logger.Error("Failed to flush MemTables: %v", err)
		os.Exit(1)
	}
	logger.Info("Flush completed in %s", helpers.FormatDuration(time.Since(flushStart)))
	logger.Info("")

	logger.Sync()
}
