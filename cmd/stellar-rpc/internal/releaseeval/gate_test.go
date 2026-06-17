package releaseeval

import (
	"os"
	"testing"
)

// TestReleaseGate is the live release-evaluation gate, run by the report job
// with RELEASE_EVAL_RUN=1. It is not a unit test: it reads the downloaded child
// artifacts, writes the report, posts the PR comment, and fails on a breach.
func TestReleaseGate(t *testing.T) {
	if os.Getenv("RELEASE_EVAL_RUN") != "1" {
		t.Skip("RELEASE_EVAL_RUN not set; the gate runs only in the report job")
	}
	if RunGate() {
		t.Fatal("release gate failed")
	}
}
