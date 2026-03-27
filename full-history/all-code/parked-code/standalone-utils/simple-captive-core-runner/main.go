// Package main implements a simple CaptiveStellarCore benchmark utility.
//
// This tool measures the performance of CaptiveStellarCore's PrepareRange
// and GetLedger operations, reporting comprehensive timing metrics including
// percentiles (p50, p90, p95, p99).
//
// Usage:
//
//	./simple-captive-core-runner \
//	  --stellar-core-binary /path/to/stellar-core \
//	  --stellar-core-toml-config /path/to/stellar-core.toml \
//	  --start-ledger 50000000 \
//	  --end-ledger 50001000 \
//	  --log-file benchmark.log \
//	  --error-file benchmark.err
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/stellar/stellar-rpc/full-history/all-code/helpers"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/network"
)

// =============================================================================
// Configuration
// =============================================================================

// Config holds the benchmark configuration
type Config struct {
	StellarCoreBinary string
	StellarCoreToml   string
	StartLedger       uint32
	EndLedger         uint32
	ProgressEvery     uint32
	LedgerTimeout     time.Duration
	MaxSamples        int
	LogFile           string
	ErrorFile         string
}

// =============================================================================
// Timing Statistics
// =============================================================================

// TimingStats tracks timing statistics with percentile calculation support
type TimingStats struct {
	Samples    []time.Duration
	Min        time.Duration
	Max        time.Duration
	Sum        time.Duration
	Count      int64
	SampleRate int
}

func NewTimingStats(maxSamples int, totalItems int) *TimingStats {
	sampleRate := 1
	if totalItems > maxSamples {
		sampleRate = (totalItems + maxSamples - 1) / maxSamples
	}
	return &TimingStats{
		Samples:    make([]time.Duration, 0, maxSamples),
		Min:        time.Duration(1<<63 - 1),
		SampleRate: sampleRate,
	}
}

func (ts *TimingStats) Record(d time.Duration) {
	ts.Sum += d
	ts.Count++
	if d < ts.Min {
		ts.Min = d
	}
	if d > ts.Max {
		ts.Max = d
	}
	if ts.Count%int64(ts.SampleRate) == 0 {
		ts.Samples = append(ts.Samples, d)
	}
}

// Avg returns the average duration
func (ts *TimingStats) Avg() time.Duration {
	if ts.Count == 0 {
		return 0
	}
	return time.Duration(int64(ts.Sum) / ts.Count)
}

func (ts *TimingStats) Percentile(p float64) time.Duration {
	if len(ts.Samples) == 0 {
		return 0
	}

	sorted := make([]time.Duration, len(ts.Samples))
	copy(sorted, ts.Samples)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	idx := int(float64(len(sorted)-1) * p / 100.0)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// =============================================================================
// Logger
// =============================================================================

// Logger provides dual logging to stdout and files
type Logger struct {
	logFile   *os.File
	errorFile *os.File
}

// NewLogger creates a new Logger with log and error file outputs
func NewLogger(logPath, errorPath string) (*Logger, error) {
	logFile, err := os.Create(logPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create log file: %w", err)
	}

	errorFile, err := os.Create(errorPath)
	if err != nil {
		logFile.Close()
		return nil, fmt.Errorf("failed to create error file: %w", err)
	}

	return &Logger{logFile: logFile, errorFile: errorFile}, nil
}

// Close closes the logger files
func (l *Logger) Close() {
	if l.logFile != nil {
		l.logFile.Close()
	}
	if l.errorFile != nil {
		l.errorFile.Close()
	}
}

func (l *Logger) timestamp() string {
	return time.Now().UTC().Format("2006-01-02T15:04:05Z")
}

// Info logs an info message to log file
func (l *Logger) Info(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	line := fmt.Sprintf("[%s] [INFO] %s\n", l.timestamp(), msg)
	l.logFile.WriteString(line)
}

// Error logs an error message to log file and error file
func (l *Logger) Error(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	line := fmt.Sprintf("[%s] [ERROR] %s\n", l.timestamp(), msg)
	l.logFile.WriteString(line)
	l.errorFile.WriteString(line)
}

// Separator logs a visual separator line
func (l *Logger) Separator() {
	line := "================================================================================\n"
	l.logFile.WriteString(line)
}

// Sync flushes the log files to disk
func (l *Logger) Sync() {
	l.logFile.Sync()
	l.errorFile.Sync()
}

// =============================================================================
// Main Entry Point
// =============================================================================

func main() {
	config := parseFlags()

	if err := validateConfig(config); err != nil {
		log.Fatalf("Configuration error: %v", err)
	}

	logger, err := NewLogger(config.LogFile, config.ErrorFile)
	if err != nil {
		log.Fatalf("Failed to create logger: %v", err)
	}
	defer logger.Close()

	if err := runBenchmark(config, logger); err != nil {
		logger.Error("Benchmark failed: %v", err)
		os.Exit(1)
	}
}

func parseFlags() *Config {
	config := &Config{}

	flag.StringVar(&config.StellarCoreBinary, "stellar-core-binary", "", "Path to stellar-core binary (required)")
	flag.StringVar(&config.StellarCoreToml, "stellar-core-toml-config", "", "Path to stellar-core TOML config (required)")

	var startLedger, endLedger, progressEvery uint
	flag.UintVar(&startLedger, "start-ledger", 0, "Starting ledger sequence (required)")
	flag.UintVar(&endLedger, "end-ledger", 0, "Ending ledger sequence (required)")
	flag.UintVar(&progressEvery, "progress-every", 100, "Print progress every N ledgers")

	flag.DurationVar(&config.LedgerTimeout, "ledger-timeout", 60*time.Second, "Timeout per GetLedger call")
	flag.IntVar(&config.MaxSamples, "max-samples", 100000, "Maximum samples to store for percentile calculation")

	flag.StringVar(&config.LogFile, "log-file", "benchmark.log", "Path to log file")
	flag.StringVar(&config.ErrorFile, "error-file", "benchmark.err", "Path to error file")

	flag.Parse()

	config.StartLedger = uint32(startLedger)
	config.EndLedger = uint32(endLedger)
	config.ProgressEvery = uint32(progressEvery)

	return config
}

func validateConfig(config *Config) error {
	// Required flags
	if config.StellarCoreBinary == "" {
		return fmt.Errorf("--stellar-core-binary is required")
	}
	if config.StellarCoreToml == "" {
		return fmt.Errorf("--stellar-core-toml-config is required")
	}
	if config.StartLedger == 0 {
		return fmt.Errorf("--start-ledger is required")
	}
	if config.EndLedger == 0 {
		return fmt.Errorf("--end-ledger is required")
	}

	// Validate range
	if config.EndLedger < config.StartLedger {
		return fmt.Errorf("--end-ledger (%d) must be >= --start-ledger (%d)", config.EndLedger, config.StartLedger)
	}

	// Validate binary exists and is executable
	absPath, err := filepath.Abs(config.StellarCoreBinary)
	if err != nil {
		return fmt.Errorf("invalid stellar-core-binary path: %w", err)
	}
	info, err := os.Stat(absPath)
	if os.IsNotExist(err) {
		return fmt.Errorf("stellar-core binary not found: %s", absPath)
	}
	if err != nil {
		return fmt.Errorf("failed to stat stellar-core binary: %w", err)
	}
	if info.IsDir() {
		return fmt.Errorf("stellar-core-binary is a directory, not a file: %s", absPath)
	}
	// Check if executable (Unix)
	if info.Mode()&0111 == 0 {
		return fmt.Errorf("stellar-core binary is not executable: %s", absPath)
	}
	config.StellarCoreBinary = absPath

	// Validate TOML exists
	absToml, err := filepath.Abs(config.StellarCoreToml)
	if err != nil {
		return fmt.Errorf("invalid stellar-core-toml-config path: %w", err)
	}
	if _, err := os.Stat(absToml); os.IsNotExist(err) {
		return fmt.Errorf("stellar-core TOML config not found: %s", absToml)
	}
	config.StellarCoreToml = absToml

	return nil
}

// =============================================================================
// Benchmark Logic
// =============================================================================

func runBenchmark(config *Config, logger *Logger) error {
	ctx := context.Background()

	totalLedgers := config.EndLedger - config.StartLedger + 1

	// Print configuration
	logger.Separator()
	logger.Info("CAPTIVESTELLARCORE BENCHMARK")
	logger.Separator()
	logger.Info("")
	logger.Info("CONFIGURATION:")
	logger.Info("  Stellar Core Binary: %s", config.StellarCoreBinary)
	logger.Info("  Stellar Core TOML:   %s", config.StellarCoreToml)
	logger.Info("  Ledger Range:        %d - %d (%s ledgers)",
		config.StartLedger, config.EndLedger, helpers.FormatNumber(int64(totalLedgers)))
	logger.Info("  Progress Every:      %d ledgers", config.ProgressEvery)
	logger.Info("  Ledger Timeout:      %s", config.LedgerTimeout)
	logger.Info("  Max Samples:         %d", config.MaxSamples)
	logger.Info("  Log File:            %s", config.LogFile)
	logger.Info("  Error File:          %s", config.ErrorFile)
	logger.Info("")
	logger.Separator()
	logger.Info("")

	// Initialize CaptiveStellarCore
	logger.Info("Initializing CaptiveStellarCore...")

	captiveCoreTomlParams := ledgerbackend.CaptiveCoreTomlParams{
		NetworkPassphrase:  network.PublicNetworkPassphrase,
		HistoryArchiveURLs: network.PublicNetworkhistoryArchiveURLs,
		CoreBinaryPath:     config.StellarCoreBinary,
		EmitVerboseMeta:    true,
	}

	captiveCoreToml, err := ledgerbackend.NewCaptiveCoreTomlFromFile(config.StellarCoreToml, captiveCoreTomlParams)
	if err != nil {
		return fmt.Errorf("failed to load captive core toml: %w", err)
	}

	captiveCoreConfig := ledgerbackend.CaptiveCoreConfig{
		BinaryPath:         config.StellarCoreBinary,
		NetworkPassphrase:  network.PublicNetworkPassphrase,
		HistoryArchiveURLs: network.PublicNetworkhistoryArchiveURLs,
		Toml:               captiveCoreToml,
	}

	backend, err := ledgerbackend.NewCaptive(captiveCoreConfig)
	if err != nil {
		return fmt.Errorf("failed to create captive core backend: %w", err)
	}
	defer backend.Close()

	logger.Info("CaptiveStellarCore initialized successfully")
	logger.Info("")

	// PrepareRange
	logger.Info("Preparing ledger range %d - %d...", config.StartLedger, config.EndLedger)
	prepareStart := time.Now()

	ledgerRange := ledgerbackend.BoundedRange(config.StartLedger, config.EndLedger)
	if err := backend.PrepareRange(ctx, ledgerRange); err != nil {
		return fmt.Errorf("failed to prepare range: %w", err)
	}

	prepareDuration := time.Since(prepareStart)
	logger.Info("PrepareRange completed in %s", helpers.FormatDuration(prepareDuration))
	logger.Info("")

	// Initialize timing stats
	getLedgerStats := NewTimingStats(config.MaxSamples, int(totalLedgers))

	// Track bytes
	var totalBytes int64

	// Progress tracking
	benchmarkStart := time.Now()
	var lastProgressTime time.Time
	processedCount := uint32(0)

	logger.Separator()
	logger.Info("FETCHING LEDGERS")
	logger.Separator()
	logger.Info("")

	// Fetch each ledger
	for seq := config.StartLedger; seq <= config.EndLedger; seq++ {
		// Create timeout context for this ledger
		ledgerCtx, cancel := context.WithTimeout(ctx, config.LedgerTimeout)

		// Time the GetLedger call
		getLedgerStart := time.Now()
		lcm, err := backend.GetLedger(ledgerCtx, seq)
		getLedgerDuration := time.Since(getLedgerStart)
		cancel()

		if err != nil {
			return fmt.Errorf("failed to get ledger %d: %w", seq, err)
		}

		// Record timing
		getLedgerStats.Record(getLedgerDuration)

		// Track bytes
		lcmBytes, err := lcm.MarshalBinary()
		if err != nil {
			return fmt.Errorf("failed to marshal ledger %d: %w", seq, err)
		}
		totalBytes += int64(len(lcmBytes))

		processedCount++

		// Progress output
		if processedCount%config.ProgressEvery == 0 || seq == config.EndLedger {
			elapsed := time.Since(benchmarkStart)
			avgPerLedger := elapsed / time.Duration(processedCount)
			remaining := uint32(config.EndLedger - seq)
			eta := avgPerLedger * time.Duration(remaining)

			pct := float64(processedCount) / float64(totalLedgers) * 100.0
			rate := float64(processedCount) / elapsed.Seconds()

			if time.Since(lastProgressTime) > 100*time.Millisecond || seq == config.EndLedger {
				logger.Info("Progress: %d/%d (%.1f%%) | Rate: %.2f ledgers/s | ETA: %s",
					processedCount, totalLedgers, pct, rate, helpers.FormatDuration(eta))
				lastProgressTime = time.Now()
			}
		}
	}

	totalDuration := time.Since(benchmarkStart)

	// Calculate percentiles
	p50 := getLedgerStats.Percentile(50)
	p90 := getLedgerStats.Percentile(90)
	p95 := getLedgerStats.Percentile(95)
	p99 := getLedgerStats.Percentile(99)

	// Print results
	logger.Info("")
	logger.Separator()
	logger.Separator()
	logger.Info("                          BENCHMARK RESULTS")
	logger.Separator()
	logger.Separator()
	logger.Info("")

	logger.Info("SUMMARY:")
	logger.Info("  Ledgers Fetched:     %s", helpers.FormatNumber(int64(processedCount)))
	logger.Info("  Total Time:          %s", helpers.FormatDuration(totalDuration))
	logger.Info("  Throughput:          %s", helpers.FormatRate(int64(processedCount), totalDuration))
	logger.Info("  Total Bytes:         %s", helpers.FormatBytes(totalBytes))
	logger.Info("  Avg Bytes/Ledger:    %s", helpers.FormatBytes(totalBytes/int64(processedCount)))
	logger.Info("")

	logger.Info("PREPARE RANGE:")
	logger.Info("  Duration:            %s", helpers.FormatDuration(prepareDuration))
	logger.Info("")

	logger.Info("GET LEDGER TIMING:")
	logger.Info("  Min:                 %s", helpers.FormatDuration(getLedgerStats.Min))
	logger.Info("  Max:                 %s", helpers.FormatDuration(getLedgerStats.Max))
	logger.Info("  Avg:                 %s", helpers.FormatDuration(getLedgerStats.Avg()))
	logger.Info("")
	logger.Info("  Percentiles:")
	logger.Info("    p50 (median):      %s", helpers.FormatDuration(p50))
	logger.Info("    p90:               %s", helpers.FormatDuration(p90))
	logger.Info("    p95:               %s", helpers.FormatDuration(p95))
	logger.Info("    p99:               %s", helpers.FormatDuration(p99))
	logger.Info("")

	logger.Separator()
	logger.Separator()
	logger.Info("Benchmark completed successfully!")
	logger.Info("")

	logger.Sync()

	return nil
}
