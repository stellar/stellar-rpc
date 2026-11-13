package integrationtest

import (
	"os"
	"testing"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/preflight"
)

// The daemons these tests start run their preflight workers from the test binary.
func TestMain(m *testing.M) {
	preflight.RunIfWorker()
	os.Exit(m.Run())
}
