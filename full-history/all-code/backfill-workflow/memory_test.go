package backfill

import "testing"

func TestMemoryMonitor(t *testing.T) {
	log := NewTestLogger("MEM")
	mon := NewMemoryMonitor(MemoryMonitorConfig{
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

func TestNopMemoryMonitor(t *testing.T) {
	mon := NewNopMemoryMonitor(24.3)

	if got := mon.CurrentRSSGB(); got != 24.3 {
		t.Errorf("CurrentRSSGB() = %f, want 24.3", got)
	}
	if got := mon.PeakRSSGB(); got != 24.3 {
		t.Errorf("PeakRSSGB() = %f, want 24.3", got)
	}
	if got := mon.Check(); got == 0 {
		t.Error("Check() should return non-zero for 24.3 GB")
	}

	log := NewTestLogger("TEST")
	mon.LogSummary(log)
	mon.Stop()
}
