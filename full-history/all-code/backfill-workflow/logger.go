package backfill

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

// =============================================================================
// DualLogger
// =============================================================================
//
// DualLogger writes info-level messages to a main log file and error-level
// messages to both the main log and a dedicated error file. All output also
// goes to stdout/stderr for operator visibility.
//
// Format: [2006-01-02 15:04:05] [SCOPE] message
//
// The logger is safe for concurrent use — all writes are serialized via mutex.

// DualLoggerConfig holds the configuration for creating a DualLogger.
type DualLoggerConfig struct {
	// LogFile is the path to the main log file. Created if it doesn't exist.
	LogFile string

	// ErrorFile is the path to the error log file. Created if it doesn't exist.
	ErrorFile string

	// Scope is the initial scope prefix (e.g., "BACKFILL").
	Scope string
}

// dualLogger implements Logger with dual file output.
type dualLogger struct {
	mu        sync.Mutex
	logFile   *os.File
	errorFile *os.File
	scope     string
}

// NewDualLogger creates a DualLogger that writes to the specified files.
// Both files are opened in append mode and created if they don't exist.
func NewDualLogger(cfg DualLoggerConfig) (Logger, error) {
	logFile, err := os.OpenFile(cfg.LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file %s: %w", cfg.LogFile, err)
	}

	errorFile, err := os.OpenFile(cfg.ErrorFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		logFile.Close()
		return nil, fmt.Errorf("failed to open error file %s: %w", cfg.ErrorFile, err)
	}

	return &dualLogger{
		logFile:   logFile,
		errorFile: errorFile,
		scope:     cfg.Scope,
	}, nil
}

func (l *dualLogger) formatLine(msg string) string {
	ts := time.Now().Format("2006-01-02 15:04:05")
	return fmt.Sprintf("[%s] [%s] %s\n", ts, l.scope, msg)
}

func (l *dualLogger) Info(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	line := l.formatLine(msg)

	l.mu.Lock()
	defer l.mu.Unlock()

	fmt.Fprint(os.Stdout, line)
	l.logFile.WriteString(line)
}

func (l *dualLogger) Error(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	line := l.formatLine(msg)

	l.mu.Lock()
	defer l.mu.Unlock()

	fmt.Fprint(os.Stderr, line)
	l.logFile.WriteString(line)
	l.errorFile.WriteString(line)
}

func (l *dualLogger) Separator() {
	l.Info("───────────────────────────────────────────────────")
}

func (l *dualLogger) Sync() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.logFile.Sync()
	l.errorFile.Sync()
}

func (l *dualLogger) Close() {
	l.Sync()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.logFile.Close()
	l.errorFile.Close()
}

func (l *dualLogger) WithScope(scope string) Logger {
	newScope := l.scope + ":" + scope
	return &scopedLogger{parent: l, scope: newScope}
}

// =============================================================================
// ScopedLogger
// =============================================================================
//
// ScopedLogger delegates to its parent DualLogger but with a modified scope.
// Scopes nest: WithScope("RANGE").WithScope("0000") → "BACKFILL:RANGE:0000".

type scopedLogger struct {
	parent *dualLogger
	scope  string
}

func (s *scopedLogger) formatLine(msg string) string {
	ts := time.Now().Format("2006-01-02 15:04:05")
	return fmt.Sprintf("[%s] [%s] %s\n", ts, s.scope, msg)
}

func (s *scopedLogger) Info(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	line := s.formatLine(msg)

	s.parent.mu.Lock()
	defer s.parent.mu.Unlock()

	fmt.Fprint(os.Stdout, line)
	s.parent.logFile.WriteString(line)
}

func (s *scopedLogger) Error(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	line := s.formatLine(msg)

	s.parent.mu.Lock()
	defer s.parent.mu.Unlock()

	fmt.Fprint(os.Stderr, line)
	s.parent.logFile.WriteString(line)
	s.parent.errorFile.WriteString(line)
}

func (s *scopedLogger) Separator() {
	s.Info("───────────────────────────────────────────────────")
}

func (s *scopedLogger) Sync()  { s.parent.Sync() }
func (s *scopedLogger) Close() { s.parent.Close() }

func (s *scopedLogger) WithScope(scope string) Logger {
	return &scopedLogger{parent: s.parent, scope: s.scope + ":" + scope}
}

// =============================================================================
// NopLogger (for tests)
// =============================================================================

// nopLogger is a no-op Logger used as a default when no logger is configured.
type nopLogger struct{}

// NewNopLogger returns a Logger that discards all output.
func NewNopLogger() Logger { return &nopLogger{} }

func (n *nopLogger) Info(format string, args ...interface{})  {}
func (n *nopLogger) Error(format string, args ...interface{}) {}
func (n *nopLogger) Separator()                               {}
func (n *nopLogger) Sync()                                    {}
func (n *nopLogger) Close()                                   {}
func (n *nopLogger) WithScope(scope string) Logger            { return n }

// =============================================================================
// TestLogger (captures output for assertions)
// =============================================================================

// TestLogger captures all log output for test assertions.
// It is safe for concurrent use.
type TestLogger struct {
	mu       sync.Mutex
	Messages []string
	Errors   []string
	scope    string
}

// NewTestLogger creates a TestLogger with the given scope.
func NewTestLogger(scope string) *TestLogger {
	return &TestLogger{scope: scope}
}

func (tl *TestLogger) Info(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	tl.mu.Lock()
	defer tl.mu.Unlock()
	tl.Messages = append(tl.Messages, fmt.Sprintf("[%s] %s", tl.scope, msg))
}

func (tl *TestLogger) Error(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	tl.mu.Lock()
	defer tl.mu.Unlock()
	tl.Errors = append(tl.Errors, fmt.Sprintf("[%s] %s", tl.scope, msg))
}

func (tl *TestLogger) Separator() {
	tl.Info("────────────────────────────────────────")
}

func (tl *TestLogger) Sync()  {}
func (tl *TestLogger) Close() {}

func (tl *TestLogger) WithScope(scope string) Logger {
	return &TestLogger{
		Messages: tl.Messages, // share slices for visibility
		Errors:   tl.Errors,
		scope:    tl.scope + ":" + scope,
	}
}

// HasMessage returns true if any info message contains the substring.
func (tl *TestLogger) HasMessage(substr string) bool {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	for _, m := range tl.Messages {
		if strings.Contains(m, substr) {
			return true
		}
	}
	return false
}

// HasError returns true if any error message contains the substring.
func (tl *TestLogger) HasError(substr string) bool {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	for _, e := range tl.Errors {
		if strings.Contains(e, substr) {
			return true
		}
	}
	return false
}
