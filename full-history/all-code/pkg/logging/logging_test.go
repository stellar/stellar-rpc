package logging

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDualLogger(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "test.log")
	errFile := filepath.Join(dir, "test-error.log")

	logger, err := NewDualLogger(DualLoggerConfig{
		LogFile:   logFile,
		ErrorFile: errFile,
		Scope:     "TEST",
	})
	if err != nil {
		t.Fatalf("NewDualLogger failed: %v", err)
	}

	logger.Info("hello %s", "world")
	logger.Error("something went %s", "wrong")
	logger.Separator()
	logger.Close()

	// Read log file — should contain both info and error messages
	logData, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("read log file: %v", err)
	}
	logStr := string(logData)
	if !strings.Contains(logStr, "[TEST] hello world") {
		t.Errorf("log file missing info message, got: %s", logStr)
	}
	if !strings.Contains(logStr, "[TEST] something went wrong") {
		t.Errorf("log file missing error message, got: %s", logStr)
	}

	// Read error file — should contain only error messages
	errData, err := os.ReadFile(errFile)
	if err != nil {
		t.Fatalf("read error file: %v", err)
	}
	errStr := string(errData)
	if !strings.Contains(errStr, "[TEST] something went wrong") {
		t.Errorf("error file missing error message, got: %s", errStr)
	}
	if strings.Contains(errStr, "hello world") {
		t.Errorf("error file should not contain info messages, got: %s", errStr)
	}
}

func TestScopedLogger(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "test.log")
	errFile := filepath.Join(dir, "test-error.log")

	logger, err := NewDualLogger(DualLoggerConfig{
		LogFile:   logFile,
		ErrorFile: errFile,
		Scope:     "BACKFILL",
	})
	if err != nil {
		t.Fatalf("NewDualLogger failed: %v", err)
	}

	scoped := logger.WithScope("RANGE").WithScope("0000")
	scoped.Info("processing chunk %d", 350)
	logger.Close()

	logData, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("read log file: %v", err)
	}
	logStr := string(logData)
	if !strings.Contains(logStr, "[BACKFILL:RANGE:0000] processing chunk 350") {
		t.Errorf("scoped message not formatted correctly, got: %s", logStr)
	}
}

func TestTestLogger(t *testing.T) {
	tl := NewTestLogger("TEST")
	tl.Info("info %d", 1)
	tl.Error("error %d", 2)

	if !tl.HasMessage("info 1") {
		t.Error("HasMessage should find 'info 1'")
	}
	if !tl.HasError("error 2") {
		t.Error("HasError should find 'error 2'")
	}
	if tl.HasMessage("nonexistent") {
		t.Error("HasMessage should not find 'nonexistent'")
	}
}

func TestNopLogger(t *testing.T) {
	// Just verify it doesn't panic
	log := NewNopLogger()
	log.Info("test %d", 1)
	log.Error("test %d", 2)
	log.Separator()
	log.Sync()
	log.WithScope("sub").Info("nested")
	log.Close()
}

func TestScopeDepthFiltering(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "test.log")
	errFile := filepath.Join(dir, "test-error.log")

	logger, err := NewDualLogger(DualLoggerConfig{
		LogFile:       logFile,
		ErrorFile:     errFile,
		Scope:         "ROOT",
		MaxScopeDepth: 2,
	})
	if err != nil {
		t.Fatalf("NewDualLogger failed: %v", err)
	}

	// Depth 1 — should write
	d1 := logger.WithScope("A")
	d1.Info("depth-1-visible")

	// Depth 2 — should write
	d2 := d1.WithScope("B")
	d2.Info("depth-2-visible")

	// Depth 3 — should be silenced (NopLogger)
	d3 := d2.WithScope("C")
	d3.Info("depth-3-hidden")

	logger.Close()

	logData, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("read log file: %v", err)
	}
	logStr := string(logData)

	if !strings.Contains(logStr, "depth-1-visible") {
		t.Errorf("depth 1 message should be present, got: %s", logStr)
	}
	if !strings.Contains(logStr, "depth-2-visible") {
		t.Errorf("depth 2 message should be present, got: %s", logStr)
	}
	if strings.Contains(logStr, "depth-3-hidden") {
		t.Errorf("depth 3 message should be silenced, got: %s", logStr)
	}
}

func TestScopeDepthUnlimited(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "test.log")
	errFile := filepath.Join(dir, "test-error.log")

	logger, err := NewDualLogger(DualLoggerConfig{
		LogFile:       logFile,
		ErrorFile:     errFile,
		Scope:         "ROOT",
		MaxScopeDepth: 0, // unlimited
	})
	if err != nil {
		t.Fatalf("NewDualLogger failed: %v", err)
	}

	// Chain to depth 5 — all should write
	l := logger
	for i := 0; i < 5; i++ {
		l = l.WithScope("L")
	}
	l.Info("deep-message")
	logger.Close()

	logData, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("read log file: %v", err)
	}
	if !strings.Contains(string(logData), "deep-message") {
		t.Errorf("unlimited depth should allow all messages, got: %s", string(logData))
	}
}

func TestScopeDepthCascades(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "test.log")
	errFile := filepath.Join(dir, "test-error.log")

	logger, err := NewDualLogger(DualLoggerConfig{
		LogFile:       logFile,
		ErrorFile:     errFile,
		Scope:         "ROOT",
		MaxScopeDepth: 1,
	})
	if err != nil {
		t.Fatalf("NewDualLogger failed: %v", err)
	}

	// Depth 1 — allowed
	d1 := logger.WithScope("A")
	d1.Info("depth-1-ok")

	// Depth 2 — NopLogger
	d2 := d1.WithScope("B")
	d2.Info("depth-2-nop")

	// Depth 3 — WithScope on NopLogger returns NopLogger
	d3 := d2.WithScope("C")
	d3.Info("depth-3-nop")

	// Verify d2 and d3 are both nopLogger
	if _, ok := d2.(*nopLogger); !ok {
		t.Errorf("depth 2 should be nopLogger, got %T", d2)
	}
	if _, ok := d3.(*nopLogger); !ok {
		t.Errorf("depth 3 should be nopLogger (cascaded), got %T", d3)
	}

	logger.Close()

	logData, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("read log file: %v", err)
	}
	logStr := string(logData)

	if !strings.Contains(logStr, "depth-1-ok") {
		t.Errorf("depth 1 message should be present, got: %s", logStr)
	}
	if strings.Contains(logStr, "depth-2-nop") || strings.Contains(logStr, "depth-3-nop") {
		t.Errorf("depth 2+ messages should be silenced, got: %s", logStr)
	}
}
