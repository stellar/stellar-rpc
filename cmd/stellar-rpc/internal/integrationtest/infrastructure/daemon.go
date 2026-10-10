package infrastructure

import (
	"context"
	"os"
	"testing"
	"time"

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

// runningDaemon drives one daemon's Run(ctx) error on its own goroutine and
// supplies the close and exited halves of rpcDaemon. Both daemons stop the
// same way: cancel the context, then wait for Run to return. A nil
// *runningDaemon, which is what an adapter holds before start, has nothing to
// stop and nothing to report.
type runningDaemon struct {
	t       testing.TB
	name    string
	timeout time.Duration
	cancel  context.CancelFunc
	// done carries the daemon's exit error once, for waitForRPC. stopped is
	// closed at the same moment and is what close waits on, so a close after
	// waitForRPC already consumed the error does not wait out the timeout.
	done    chan error
	stopped chan struct{}
}

func startDaemon(t testing.TB, name string, timeout time.Duration,
	run func(ctx context.Context) error,
) *runningDaemon {
	ctx, cancel := context.WithCancel(context.Background())
	r := &runningDaemon{
		t:       t,
		name:    name,
		timeout: timeout,
		cancel:  cancel,
		done:    make(chan error, 1),
		stopped: make(chan struct{}),
	}
	go func() {
		r.done <- run(ctx)
		close(r.stopped)
	}()
	return r
}

// close cancels the daemon and waits for Run to return. Run returns nil on a
// cancel, so an error still on done means the daemon died on its own during
// the test, which fails it. A Run that outlives the timeout fails it too.
func (r *runningDaemon) close() {
	if r == nil {
		return
	}
	r.cancel()
	select {
	case <-r.stopped:
		select {
		case err := <-r.done:
			if err != nil {
				r.t.Errorf("%s daemon failed during the test: %v", r.name, err)
			}
		default: // waitForRPC already reported the exit error
		}
	case <-time.After(r.timeout):
		r.t.Errorf("%s daemon did not stop within %s", r.name, r.timeout)
	}
}

func (r *runningDaemon) exited() <-chan error {
	if r == nil {
		return nil
	}
	return r.done
}
