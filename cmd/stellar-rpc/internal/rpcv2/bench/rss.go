package bench

import (
	"time"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
)

// recordPeakRSS records the run's peak resident set size, read through
// readRSS in bytes, as the driverPeakRSS row — the memory high-water mark of
// the whole process. Production passes observability.ReadPeakRSS, which
// verify-cold shares. VmHWM never decreases, so reading it after the run body
// still reports the run's peak even though the stores have already closed. A
// failed read (no /proc) logs a warning and skips the row; it does not fail
// the run.
//
// The row's "duration" columns carry BYTES, not nanoseconds (see the
// driverPeakRSS constant and the fileSpecs doc comment).
func recordPeakRSS(logger *supportlog.Entry, sink *csvSink, readRSS func() (uint64, error)) {
	rssBytes, err := readRSS()
	if err != nil {
		// Warn, not Debug: the bench logger runs at Info, and a report
		// missing this row should say why.
		logger.Warnf("peak RSS unavailable, skipping %s row: %v", driverPeakRSS, err)
		return
	}
	//nolint:gosec // a byte count fits time.Duration's int64 range for any real process
	sink.observe(fileDriver, driverPeakRSS, time.Duration(rssBytes), 0)
}
