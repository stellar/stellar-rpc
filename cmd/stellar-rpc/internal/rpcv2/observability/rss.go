package observability

import "github.com/prometheus/procfs"

// ReadPeakRSS returns VmHWM — the kernel's peak resident set size for this
// process, in bytes — from /proc/self/status. VmHWM counts every resident
// page the process ever held, so it sees what a Go heap profile cannot:
// RocksDB's C++ allocations through cgo, and file pages this process maps.
// Both sit outside the Go heap, which is why a Go memory limit alone does not
// describe a process's real footprint. VmHWM never decreases, so it can be
// read after the stores have closed and still report the run's peak.
//
// procfs reads /proc, so this errors on an OS without one (macOS).
func ReadPeakRSS() (uint64, error) {
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
