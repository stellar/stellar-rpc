package rpcv2

import (
	"testing"

	"go.uber.org/goleak"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/preflight"
)

// TestMain runs this package's tests under goleak: a goroutine that outlives its
// test — a daemon body that didn't unwind on ctx cancel, an unjoined
// backfill goroutine — fails the suite instead of silently leaking.
func TestMain(m *testing.M) {
	preflight.RunIfWorker()
	goleak.VerifyTestMain(m)
}
