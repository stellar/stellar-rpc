//go:build linux

package memory

import (
	"os"
	"strconv"
	"strings"
)

// readProcStatmRSS reads current RSS from /proc/self/statm.
// Field 1 is resident pages; multiply by page size for bytes.
// Returns 0 on any error.
func readProcStatmRSS() int64 {
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		return 0
	}
	pages, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return 0
	}
	return pages * int64(os.Getpagesize())
}
