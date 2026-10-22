//go:build !linux

package packfile

import "os"

func initiateWriteback(_ *os.File, _, _ int64) {}
