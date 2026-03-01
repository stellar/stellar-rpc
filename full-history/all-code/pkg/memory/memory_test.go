package memory

import (
	"testing"

	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/logging"
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

	// CurrentRSSGB should be positive
	gb := mon.CurrentRSSGB()
	if gb <= 0 {
		t.Errorf("CurrentRSSGB() returned %f, expected positive", gb)
	}

	// PeakRSSGB should be >= CurrentRSSGB
	peak := mon.PeakRSSGB()
	if peak < gb {
		t.Errorf("PeakRSSGB() %f < CurrentRSSGB() %f", peak, gb)
	}

	// LogSummary should not panic
	mon.LogSummary(log)
}

func TestNopMonitor(t *testing.T) {
	mon := NewNopMonitor(24.3)

	if got := mon.CurrentRSSGB(); got != 24.3 {
		t.Errorf("CurrentRSSGB() = %f, want 24.3", got)
	}
	if got := mon.PeakRSSGB(); got != 24.3 {
		t.Errorf("PeakRSSGB() = %f, want 24.3", got)
	}
	if got := mon.Check(); got == 0 {
		t.Error("Check() should return non-zero for 24.3 GB")
	}

	log := logging.NewTestLogger("TEST")
	mon.LogSummary(log)
	mon.Stop()
}
