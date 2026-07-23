package backfill

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain runs this package's tests under goleak: an executePlan worker that
// didn't unwind via gctx, or an index build still parked on a done-channel after
// g.Wait, fails the suite instead of silently leaking.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
