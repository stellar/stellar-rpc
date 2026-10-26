//go:build !linux

package packfile

import "os"

func adviseRandom(_ *os.File) {}

func prefetch(_ readAtCloser, _, _ int64) {}
