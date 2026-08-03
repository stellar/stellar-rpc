// Package runsettest is shared test support for the seal-publish protocol
// runset owns: a publish log that totally orders dirent barriers against
// manifest writes, an in-memory recording Manifest, and the one assertion
// both hot engines pin their barrier-before-PutRuns order with. The barrier
// is the half of the protocol runset deliberately leaves per-engine (each
// engine's fsyncDir seam, upstream of Publish), so its ordering gate lives
// here as one implementation instead of a hand-rolled copy per engine — the
// drift shape the runset extraction exists to kill. Imported only from _test
// files; nothing in it ships in the daemon binary.
package runsettest

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// PublishLog totally orders barrier and manifest calls for the ordering
// pins. Each engine's pendingSeal hand-off already sequences the sealer
// goroutine's barrier before the writer goroutine's PutRuns; the mutex keeps
// the log valid under any schedule, so a regression shows up as misordered
// events, never as a race.
type PublishLog struct {
	mu     sync.Mutex
	events []string
}

// FsyncDir records a dirent barrier over dir and succeeds. Assign it to the
// engine's fsyncDir seam in place of durable.FsyncDir.
func (l *PublishLog) FsyncDir(dir string) error {
	l.add("fsync " + dir)
	return nil
}

func (l *PublishLog) add(ev string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, ev)
}

func (l *PublishLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.events...)
}

// RecordingManifest is an in-memory runset.Manifest that logs every PutRuns
// into the same publish log the barrier recorder writes to — the two sides
// of the ordering pin share one clock. Log must be set.
type RecordingManifest struct {
	Log *PublishLog

	mu         sync.Mutex
	names      []string
	lastSealed uint32
}

// PutRuns logs "put", then stores the list like each engine's in-memory fake.
func (m *RecordingManifest) PutRuns(names []string, lastSealed uint32) error {
	m.Log.add("put")
	m.mu.Lock()
	defer m.mu.Unlock()
	m.names = append([]string(nil), names...)
	m.lastSealed = lastSealed
	return nil
}

// GetRuns returns the stored run list and sealed frontier.
func (m *RecordingManifest) GetRuns() ([]string, uint32, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.names...), m.lastSealed, nil
}

// AssertBarrierPrecedesEveryPut pins the seal-publish order the manifest's
// authority rests on: every PutRuns — a synced write, durable the moment it
// returns — must be preceded by a dirent barrier over dir, the run
// directory, since the previous publish, and exactly wantPuts publishes must
// have landed. Creating a run under its final name makes it visible, not
// durable, so without the barrier a crash right after the manifest write can
// leave it durably naming a run whose dirent was never journaled — an
// unrecoverable warmup failure on a hot chunk.
func AssertBarrierPrecedesEveryPut(t *testing.T, log *PublishLog, dir string, wantPuts int) {
	t.Helper()
	puts, barriersSincePut := 0, 0
	for _, ev := range log.snapshot() {
		if ev == "put" {
			puts++
			require.Positive(t, barriersSincePut,
				"PutRuns #%d ran without a dirent barrier since the previous publish", puts)
			barriersSincePut = 0
			continue
		}
		require.Equal(t, "fsync "+dir, ev, "the barrier must cover the run directory")
		barriersSincePut++
	}
	require.Equal(t, wantPuts, puts, "every settled seal cycle must publish exactly once")
}
