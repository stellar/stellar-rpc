//go:build !linux

package writeback

import "os"

// Start is a no-op off Linux, which has no sync_file_range.
func Start(*os.File, int64, int64) {}
