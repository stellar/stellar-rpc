package backfill

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
