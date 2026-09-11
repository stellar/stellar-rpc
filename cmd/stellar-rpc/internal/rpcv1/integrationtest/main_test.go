package integrationtest

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/fsouza/fake-gcs-server/fakestorage"
)

// The Google Cloud Storage client library reads STORAGE_EMULATOR_HOST from the
// process environment. Setting it from inside a test means calling t.Setenv,
// and Go forbids t.Setenv in a test that calls t.Parallel(). Starting one fake
// server here, before any test runs, keeps the datastore tests parallel: they
// share the server and each one takes its own bucket.
var sharedGCSServer *fakestorage.Server

func TestMain(m *testing.M) {
	// Every test in this package skips when STELLAR_RPC_INTEGRATION_TESTS_ENABLED
	// is unset, so there is nothing to serve. Binding a listener anyway would
	// fail the whole package in an environment that cannot open a loopback
	// socket, instead of skipping as it should.
	if os.Getenv("STELLAR_RPC_INTEGRATION_TESTS_ENABLED") != "" {
		server, err := fakestorage.NewServerWithOptions(fakestorage.Options{
			Scheme:     "http",
			PublicHost: "127.0.0.1",
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to start fake GCS server: %v\n", err)
			os.Exit(1)
		}
		sharedGCSServer = server
		os.Setenv("STORAGE_EMULATOR_HOST", server.URL())
	}

	code := m.Run()

	if sharedGCSServer != nil {
		sharedGCSServer.Stop()
	}
	os.Exit(code)
}

// newGCSBucket creates a bucket named after the test on the shared fake GCS
// server. Two tests never get the same name, so objects written by one test are
// invisible to the others.
func newGCSBucket(t *testing.T) string {
	// Callers reach this before infrastructure.NewTest, which is what normally
	// skips a disabled integration test, so the skip has to happen here too.
	if sharedGCSServer == nil {
		t.Skip("skipping integration test: STELLAR_RPC_INTEGRATION_TESTS_ENABLED not set")
	}
	name := strings.ToLower(strings.NewReplacer("/", "-", "_", "-").Replace(t.Name()))
	sharedGCSServer.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: name})
	return name
}

// skipLimitsUpgrade returns the value to pass as TestConfig.ApplyLimits when a
// test does not need Core's Soroban resource limits raised. Skipping the
// upgrade saves about 19 seconds of setup.
//
//	test := infrastructure.NewTest(t, &infrastructure.TestConfig{
//		ApplyLimits: skipLimitsUpgrade(),
//	})
//
// Only a test that never submits a Soroban transaction may skip it. Anything
// that uploads a contract, invokes one, or calls simulateTransaction needs the
// raised limits and must leave ApplyLimits unset.
func skipLimitsUpgrade() *string {
	skip := ""
	return &skip
}
