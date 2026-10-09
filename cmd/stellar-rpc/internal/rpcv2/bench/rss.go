package bench

import "github.com/prometheus/procfs"

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
