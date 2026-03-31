package memory

import (
	"testing"

	"github.com/stellar/stellar-rpc/full-history/pkg/logging"
)

func TestMonitor(t *testing.T) {
	log := logging.NewTestLogger("MEM")
	mon := NewMonitor(MonitorConfig{
		WarningThresholdGB: 100.0,
		Logger:             log,
	})
	defer mon.Stop()

	// Check should return a positive value (process has some RSS)
	rss := mon.Check()
	if rss <= 0 {
		t.Errorf("Check() returned %d, expected positive RSS", rss)
	}

	// PeakRSSGB should be positive
	peak := mon.PeakRSSGB()
	if peak <= 0 {
		t.Errorf("PeakRSSGB() returned %f, expected positive", peak)
	}

	// LogSummary should not panic
	mon.LogSummary(log)
}

func TestMonitorSnapshot(t *testing.T) {
	log := logging.NewTestLogger("MEM")
	mon := NewMonitor(MonitorConfig{
		WarningThresholdGB: 100.0,
		Logger:             log,
	})
	defer mon.Stop()

	snap := mon.Snapshot()
	if snap.CurrentRSS <= 0 {
		t.Errorf("Snapshot().CurrentRSS = %d, expected positive", snap.CurrentRSS)
	}
	if snap.PeakRSS < snap.CurrentRSS {
		t.Errorf("Snapshot().PeakRSS %d < CurrentRSS %d", snap.PeakRSS, snap.CurrentRSS)
	}
	if snap.HeapAlloc == 0 {
		t.Error("Snapshot().HeapAlloc should be non-zero")
	}
	if snap.HeapSys == 0 {
		t.Error("Snapshot().HeapSys should be non-zero")
	}
	if snap.NumGoroutine <= 0 {
		t.Errorf("Snapshot().NumGoroutine = %d, expected positive", snap.NumGoroutine)
	}
}

func TestNopMonitor(t *testing.T) {
	mon := NewNopMonitor(24.3)

	if got := mon.PeakRSSGB(); got != 24.3 {
		t.Errorf("PeakRSSGB() = %f, want 24.3", got)
	}
	if got := mon.Check(); got == 0 {
		t.Error("Check() should return non-zero for 24.3 GB")
	}

	snap := mon.Snapshot()
	rssGB := 24.3
	expectedBytes := int64(rssGB * 1024 * 1024 * 1024)
	if snap.CurrentRSS != expectedBytes {
		t.Errorf("Snapshot().CurrentRSS = %d, want %d", snap.CurrentRSS, expectedBytes)
	}
	if snap.PeakRSS != expectedBytes {
		t.Errorf("Snapshot().PeakRSS = %d, want %d", snap.PeakRSS, expectedBytes)
	}

	log := logging.NewTestLogger("TEST")
	mon.LogSummary(log)
	mon.Stop()
}
