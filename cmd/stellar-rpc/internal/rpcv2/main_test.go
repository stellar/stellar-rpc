package rpcv2

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain runs this package's tests under goleak: a goroutine that outlives its
// test — a daemon supervisor that didn't unwind on ctx cancel, an unjoined
// backfill goroutine — fails the suite instead of silently leaking.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
