package infrastructure

import (
	"os"
	"testing"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/daemon"
)

// STELLAR_RPC_INTEGRATION_TESTS_DAEMON picks the daemon every test in the
// package runs against: "rpcv1" (the default) or "rpcv2".
const daemonEnvVar = "STELLAR_RPC_INTEGRATION_TESTS_DAEMON"

const (
	daemonRPCv1 = "rpcv1"
	daemonRPCv2 = "rpcv2"
)

// rpcDaemon is the whole surface the harness needs from the daemon under test.
// The rest of the harness talks to the daemon over HTTP like a client would.
type rpcDaemon interface {
	// start launches the daemon and fills Test.testPorts with its RPC and admin
	// ports. It returns once the daemon is running, not once it is healthy.
	start()
	// close stops the daemon and blocks until it has stopped.
	close()
	// exited yields the error the daemon stopped with, if it stopped on its
	// own. A nil channel means the daemon reports exits some other way.
	exited() <-chan error
	// logger is the logger the harness injected into the daemon.
	logger() *supportlog.Entry
}

func selectedDaemon(t testing.TB) string {
	kind := os.Getenv(daemonEnvVar)
	switch kind {
	case "":
		return daemonRPCv1
	case daemonRPCv1, daemonRPCv2:
		return kind
	default:
		t.Fatalf("%s=%q: want %q or %q", daemonEnvVar, kind, daemonRPCv1, daemonRPCv2)
		return ""
	}
}

func (i *Test) newRPCDaemon() rpcDaemon {
	switch selectedDaemon(i.t) {
	case daemonRPCv2:
		return &rpcv2Daemon{test: i}
	default:
		return &rpcv1Daemon{test: i}
	}
}

// Logger returns the logger the harness injected into the running daemon.
func (i *Test) Logger() *supportlog.Entry {
	return i.daemon.logger()
}

// RPCv1Daemon returns the in-process rpcv1 daemon behind test. It fails the
// test when another daemon is running, so a test that reaches into rpcv1
// internals fails with a clear message instead of a nil dereference.
func RPCv1Daemon(t testing.TB, test *Test) *daemon.Daemon {
	v1, ok := test.daemon.(*rpcv1Daemon)
	if !ok {
		t.Fatalf("this test reads rpcv1 internals and cannot run with %s=%s", daemonEnvVar, selectedDaemon(t))
		return nil
	}
	return v1.daemon
}
