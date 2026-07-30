package bench

import (
	"time"

	"github.com/prometheus/procfs"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
)

// readPeakRSS returns VmHWM — the kernel's peak resident set size for this
// process, in bytes — from /proc/self/status. VmHWM counts every resident
// page the process ever held, including RocksDB's C++ allocations (block
// cache, memtables, write buffers) that a Go heap profile cannot see. procfs
// reads /proc, so it errors on an OS without one (macOS).
func readPeakRSS() (uint64, error) {
	self, err := procfs.Self()
	if err != nil {
		return 0, err
	}
	status, err := self.NewStatus()
	if err != nil {
		return 0, err
	}
	return status.VmHWM, nil
}

// recordPeakRSS records the run's peak resident set size, read through
// readRSS in bytes, as the driverPeakRSS row — the memory high-water mark of
// the whole process. VmHWM never decreases, so reading it after the run body
// still reports the run's peak even though the stores have already closed. A
// failed read (no /proc — see readPeakRSS) logs a warning and skips the row;
// it does not fail the run.
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
