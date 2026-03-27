// =============================================================================
// pkg/logging/logger.go - Dual Logging Implementation
// =============================================================================
//
// This package provides a dual-output logger that writes:
//   - Informational messages to a log file
//   - Error messages to a separate error file
//
// SCOPED LOGGING:
//   Loggers can be scoped with a prefix using WithScope(). This creates a child
//   logger that prefixes all messages with the scope name, e.g.:
//
//     logger := NewDualLogger("app.log", "error.log")
//     ingestionLog := logger.WithScope("INGEST")
//     ingestionLog.Info("Processing ledger 1000") // → [2006-01-02 15:04:05.000] [INGEST] Processing ledger 1000
//
//   The parent logger continues to work without the prefix:
//     logger.Info("Starting workflow") // → [2006-01-02 15:04:05.000] Starting workflow
//
// =============================================================================

package logging

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/interfaces"
)

// =============================================================================
// Constants
// =============================================================================

const (
	// SeparatorLine is the visual separator used in logs
	SeparatorLine = "========================================================================="

	// TimeFormat is the timestamp format for log messages
	TimeFormat = "2006-01-02 15:04:05.000"
)

// =============================================================================
// DualLogger Implementation
// =============================================================================

// DualLogger implements the Logger interface with separate log and error files.
type DualLogger struct {
	mu        sync.Mutex
	logFile   *os.File
	errorFile *os.File
	logPath   string
	errorPath string
}

// NewDualLogger creates a new DualLogger that writes to the specified files.
// If the files exist, they are truncated.
func NewDualLogger(logPath, errorPath string) (*DualLogger, error) {
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file %s: %w", logPath, err)
	}

	errorFile, err := os.OpenFile(errorPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		logFile.Close()
		return nil, fmt.Errorf("failed to open error file %s: %w", errorPath, err)
	}

	return &DualLogger{
		logFile:   logFile,
		errorFile: errorFile,
		logPath:   logPath,
		errorPath: errorPath,
	}, nil
}

// WithScope creates a scoped logger that prefixes all messages with the scope name.
// The returned ScopedLogger shares the same underlying files as the parent.
//
// Example:
//
//	ingestLog := logger.WithScope("INGEST")
//	ingestLog.Info("Starting") // → [timestamp] [INGEST] Starting
func (l *DualLogger) WithScope(scope string) interfaces.Logger {
	return &ScopedLogger{
		parent: l,
		scope:  scope,
	}
}

// Info logs an informational message to the log file.
func (l *DualLogger) Info(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	timestamp := time.Now().Format(TimeFormat)
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(l.logFile, "[%s] %s\n", timestamp, msg)
}

// Error logs an error message to both the error file and log file.
func (l *DualLogger) Error(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	timestamp := time.Now().Format(TimeFormat)
	msg := fmt.Sprintf(format, args...)

	fmt.Fprintf(l.errorFile, "[%s] ERROR: %s\n", timestamp, msg)
	fmt.Fprintf(l.logFile, "[%s] ERROR: %s\n", timestamp, msg)
}

// Separator logs a visual separator line to the log file.
func (l *DualLogger) Separator() {
	l.mu.Lock()
	defer l.mu.Unlock()

	fmt.Fprintln(l.logFile, SeparatorLine)
}

// Sync forces a flush of all log data to disk.
func (l *DualLogger) Sync() {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.logFile.Sync()
	l.errorFile.Sync()
}

// Close closes all log files after syncing.
func (l *DualLogger) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.logFile != nil {
		l.logFile.Sync()
		l.logFile.Close()
		l.logFile = nil
	}

	if l.errorFile != nil {
		l.errorFile.Sync()
		l.errorFile.Close()
		l.errorFile = nil
	}
}

// =============================================================================
// ScopedLogger - Logger with a Prefix
// =============================================================================

// ScopedLogger wraps a DualLogger and prefixes all messages with a scope name.
// This is useful for phase-specific logging where you want to identify the source.
//
// ScopedLogger shares the underlying files with its parent DualLogger.
// Closing the parent will close the files; do not close ScopedLogger directly.
type ScopedLogger struct {
	parent *DualLogger
	scope  string
}

// WithScope creates a nested scoped logger.
// The scopes are combined: parent.WithScope("A").WithScope("B") → [A:B]
func (l *ScopedLogger) WithScope(scope string) interfaces.Logger {
	return &ScopedLogger{
		parent: l.parent,
		scope:  l.scope + ":" + scope,
	}
}

// Info logs an informational message with the scope prefix.
func (l *ScopedLogger) Info(format string, args ...interface{}) {
	l.parent.mu.Lock()
	defer l.parent.mu.Unlock()

	timestamp := time.Now().Format(TimeFormat)
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(l.parent.logFile, "[%s] [%s] %s\n", timestamp, l.scope, msg)
}

// Error logs an error message with the scope prefix.
func (l *ScopedLogger) Error(format string, args ...interface{}) {
	l.parent.mu.Lock()
	defer l.parent.mu.Unlock()

	timestamp := time.Now().Format(TimeFormat)
	msg := fmt.Sprintf(format, args...)

	fmt.Fprintf(l.parent.errorFile, "[%s] [%s] ERROR: %s\n", timestamp, l.scope, msg)
	fmt.Fprintf(l.parent.logFile, "[%s] [%s] ERROR: %s\n", timestamp, l.scope, msg)
}

// Separator logs a visual separator line (no scope prefix for separators).
func (l *ScopedLogger) Separator() {
	l.parent.mu.Lock()
	defer l.parent.mu.Unlock()

	fmt.Fprintln(l.parent.logFile, SeparatorLine)
}

// Sync forces a flush of all log data to disk.
func (l *ScopedLogger) Sync() {
	l.parent.Sync()
}

// Close is a no-op for ScopedLogger. Close the parent DualLogger instead.
func (l *ScopedLogger) Close() {
	// No-op: ScopedLogger does not own the files
}

// =============================================================================
// QueryLogger - Specialized Logger for Query Operations
// =============================================================================

// QueryLogger handles logging for SIGHUP query operations.
type QueryLogger struct {
	mu         sync.Mutex
	outputFile *os.File
	logFile    *os.File
	errorFile  *os.File
}

// NewQueryLogger creates a new QueryLogger.
func NewQueryLogger(outputPath, logPath, errorPath string) (*QueryLogger, error) {
	outputFile, err := os.OpenFile(outputPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open query output file %s: %w", outputPath, err)
	}

	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		outputFile.Close()
		return nil, fmt.Errorf("failed to open query log file %s: %w", logPath, err)
	}

	errorFile, err := os.OpenFile(errorPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		outputFile.Close()
		logFile.Close()
		return nil, fmt.Errorf("failed to open query error file %s: %w", errorPath, err)
	}

	ql := &QueryLogger{
		outputFile: outputFile,
		logFile:    logFile,
		errorFile:  errorFile,
	}

	// Write CSV header
	fmt.Fprintln(ql.outputFile, "txHash,ledgerSeq,queryTimeUs")

	return ql, nil
}

// Result logs a successful query result to the CSV output file.
func (ql *QueryLogger) Result(txHashHex string, ledgerSeq uint32, queryTimeUs int64) {
	ql.mu.Lock()
	defer ql.mu.Unlock()

	fmt.Fprintf(ql.outputFile, "%s,%d,%d\n", txHashHex, ledgerSeq, queryTimeUs)
}

// NotFound logs a query for a txHash that was not found.
func (ql *QueryLogger) NotFound(txHashHex string, queryTimeUs int64) {
	ql.mu.Lock()
	defer ql.mu.Unlock()

	fmt.Fprintf(ql.outputFile, "%s,-1,%d\n", txHashHex, queryTimeUs)
}

// Stats logs query statistics to the statistics log file.
func (ql *QueryLogger) Stats(format string, args ...interface{}) {
	ql.mu.Lock()
	defer ql.mu.Unlock()

	timestamp := time.Now().Format(TimeFormat)
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(ql.logFile, "[%s] %s\n", timestamp, msg)
}

// Error logs an error to the query error file.
func (ql *QueryLogger) Error(format string, args ...interface{}) {
	ql.mu.Lock()
	defer ql.mu.Unlock()

	timestamp := time.Now().Format(TimeFormat)
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(ql.errorFile, "[%s] ERROR: %s\n", timestamp, msg)
}

// Sync forces a flush of all query log data to disk.
func (ql *QueryLogger) Sync() {
	ql.mu.Lock()
	defer ql.mu.Unlock()

	ql.outputFile.Sync()
	ql.logFile.Sync()
	ql.errorFile.Sync()
}

// Close closes all query log files after syncing.
func (ql *QueryLogger) Close() {
	ql.mu.Lock()
	defer ql.mu.Unlock()

	if ql.outputFile != nil {
		ql.outputFile.Sync()
		ql.outputFile.Close()
		ql.outputFile = nil
	}
	if ql.logFile != nil {
		ql.logFile.Sync()
		ql.logFile.Close()
		ql.logFile = nil
	}
	if ql.errorFile != nil {
		ql.errorFile.Sync()
		ql.errorFile.Close()
		ql.errorFile = nil
	}
}
