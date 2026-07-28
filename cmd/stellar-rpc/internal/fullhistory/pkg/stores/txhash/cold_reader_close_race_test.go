package txhash

// These tests pin the unsafe half of ColdReader's documented contract:
// "Get is NOT safe concurrent with Close". Close munmaps the index, and the
// closed flag only rejects Gets that start after Close returns; a Get
// already inside QueryPayload reads unmapped memory, which is an
// unrecoverable runtime fault that kills the process.
//
// The consequence for lifecycle design: no component may Close a ColdReader
// while another goroutine could still be inside Get. In particular, a
// reaper that closes retired readers after a grace period is unsafe if a
// Get can outlive that grace period (a request goroutine abandoned by the
// duration limiter, blocked on a page fault against a degraded disk).
//
// The probe child races spinning Gets against Close in cycles and dies with
// a memory fault when the munmap lands inside a Get. The parent asserts
// that death. If ColdReader gains a drain barrier (Close waits for
// in-flight Gets, like rocksdb.Store's lifecycle RWMutex), the child
// becomes crash-free and this pair should be replaced by a deterministic
// "Close waits for in-flight Get" contract test.

import (
	"errors"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const unmapProbeCycles = 2000

func TestColdReader_CloseVsInflightGet_Unprotected(t *testing.T) {
	if testing.Short() {
		t.Skip("subprocess crash probe; skipped in -short")
	}

	cmd := exec.CommandContext(t.Context(), os.Args[0],
		"-test.run=^TestColdReader_UnmapDuringGet_Probe$")
	// Few Ps + many spinner goroutines maximizes the chance a goroutine is
	// preempted between the closed-flag check and its mmap reads, so it
	// resumes after the munmap.
	cmd.Env = append(os.Environ(), "TXHASH_UNMAP_PROBE=1", "GOMAXPROCS=2")
	out, runErr := cmd.CombinedOutput()

	if runErr == nil {
		t.Fatalf("probe child survived %d Get-vs-Close race cycles without a fault; "+
			"could not reproduce the unmap hazard on this run\noutput:\n%s",
			unmapProbeCycles, out)
	}
	outStr := string(out)
	require.True(t,
		strings.Contains(outStr, "unexpected fault address") ||
			strings.Contains(outStr, "SIGSEGV") ||
			strings.Contains(outStr, "SIGBUS"),
		"probe child died, but not from a memory fault:\n%s", outStr)
}

// TestColdReader_UnmapDuringGet_Probe is the subprocess body for
// TestColdReader_CloseVsInflightGet_Unprotected. It is expected to die with
// a memory fault; it only passes (survives) if Close never unmapped under
// an in-flight Get across every cycle.
func TestColdReader_UnmapDuringGet_Probe(t *testing.T) {
	if os.Getenv("TXHASH_UNMAP_PROBE") != "1" {
		t.Skip("probe child; run via TestColdReader_CloseVsInflightGet_Unprotected")
	}

	idxPath, entries := buildColdFixture(t, 512)

	for range unmapProbeCycles {
		r, err := OpenColdReader(idxPath)
		require.NoError(t, err)

		var wg sync.WaitGroup
		const spinners = 32
		for g := range spinners {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := g; ; i++ {
					if _, err := r.Get(entries[i%len(entries)].hash); errors.Is(err, ErrClosed) {
						return
					}
				}
			}()
		}

		// Let the spinners get deep into Get loops, then yank the mapping
		// out from under them.
		time.Sleep(50 * time.Microsecond)
		_ = r.Close()
		wg.Wait()
	}
}
