package runset

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeManifest is an in-memory Manifest. putErr, when set, fails every
// PutRuns without recording anything.
type fakeManifest struct {
	names      []string
	lastSealed uint32
	putErr     error
}

func (m *fakeManifest) PutRuns(names []string, lastSealed uint32) error {
	if m.putErr != nil {
		return m.putErr
	}
	m.names = append([]string(nil), names...)
	m.lastSealed = lastSealed
	return nil
}

func (m *fakeManifest) GetRuns() ([]string, uint32, error) {
	return append([]string(nil), m.names...), m.lastSealed, nil
}

// fileRun is a minimal Run over a real file, recording disposal.
type fileRun struct {
	path   string
	closed bool
}

func (r *fileRun) RunPath() string { return r.path }
func (r *fileRun) CloseRun()       { r.closed = true }

func mkRun(t *testing.T, dir, name string) *fileRun {
	t.Helper()
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, []byte("run"), 0o644))
	return &fileRun{path: path}
}

// TestPublish_NamesLiveThenFresh pins the manifest NAME list a publish
// stores: basenames only, live order then fresh, at the given frontier —
// the exact list warmup will reopen, in order.
func TestPublish_NamesLiveThenFresh(t *testing.T) {
	dir := t.TempDir()
	live := []*fileRun{mkRun(t, dir, "seal-000001.run"), mkRun(t, dir, "seal-000002.run")}
	fresh := mkRun(t, dir, "merge-000003.run")
	m := &fakeManifest{}

	require.NoError(t, Publish(m, live, fresh, 42))
	names, lastSealed, err := m.GetRuns()
	require.NoError(t, err)
	assert.Equal(t, []string{"seal-000001.run", "seal-000002.run", "merge-000003.run"}, names)
	assert.Equal(t, uint32(42), lastSealed)
	for _, r := range append(live, fresh) {
		assert.False(t, r.closed, "a successful publish closes nothing")
		_, serr := os.Stat(r.path)
		assert.NoError(t, serr, "a successful publish unlinks nothing")
	}
}

// TestPublish_ReplaceAllListsOnlyFresh pins the merge fold's shape: with no
// surviving live runs the merged run is the manifest's only entry.
func TestPublish_ReplaceAllListsOnlyFresh(t *testing.T) {
	dir := t.TempDir()
	merged := mkRun(t, dir, "merge-000007.run")
	m := &fakeManifest{}

	require.NoError(t, Publish(m, nil, merged, 7))
	names, lastSealed, err := m.GetRuns()
	require.NoError(t, err)
	assert.Equal(t, []string{"merge-000007.run"}, names)
	assert.Equal(t, uint32(7), lastSealed)
}

// TestPublish_FailureDisposesFresh pins the protocol's failure contract
// (formerly the twin per-engine ManifestFailureDisposesRun tests): a run the
// manifest never listed is unpublished — closed and unlinked — while the
// live runs stay open and on disk, still listed by the previous manifest
// value.
func TestPublish_FailureDisposesFresh(t *testing.T) {
	dir := t.TempDir()
	live := []*fileRun{mkRun(t, dir, "seal-000001.run")}
	fresh := mkRun(t, dir, "seal-000002.run")
	m := &fakeManifest{putErr: errors.New("manifest unavailable")}

	err := Publish(m, live, fresh, 9)
	require.ErrorIs(t, err, m.putErr, "PutRuns's error must surface unwrapped")
	assert.True(t, fresh.closed, "un-listed run's handle must be closed")
	_, serr := os.Stat(fresh.path)
	require.ErrorIs(t, serr, os.ErrNotExist, "un-listed run's file must be removed")
	assert.False(t, live[0].closed, "live runs must stay open for readers")
	_, serr = os.Stat(live[0].path)
	require.NoError(t, serr, "live runs' files must remain")
	names, _, gerr := m.GetRuns()
	require.NoError(t, gerr)
	assert.Empty(t, names, "a failed publish must not have recorded anything")
}

// TestSweepOrphans pins the warmup sweep: unreferenced files go,
// manifest-listed files and directories stay.
func TestSweepOrphans(t *testing.T) {
	dir := t.TempDir()
	listed := mkRun(t, dir, "seal-000001.run")
	orphan := mkRun(t, dir, "seal-000002.run")
	sub := filepath.Join(dir, "subdir")
	require.NoError(t, os.Mkdir(sub, 0o755))

	require.NoError(t, SweepOrphans(dir, []string{"seal-000001.run"}))
	_, err := os.Stat(listed.path)
	assert.NoError(t, err, "listed run must survive the sweep")
	_, err = os.Stat(orphan.path)
	assert.ErrorIs(t, err, os.ErrNotExist, "orphan must be swept")
	_, err = os.Stat(sub)
	assert.NoError(t, err, "directories are left alone")

	require.Error(t, SweepOrphans(filepath.Join(dir, "missing"), nil),
		"an unreadable run directory must fail loudly")
}

// TestNextSealSeq pins the sequence resume: one past the highest suffix
// across exactly the given prefixes, ignoring foreign and malformed names.
func TestNextSealSeq(t *testing.T) {
	names := []string{"seal-000005.run", "merge-000009.run", "junk", "seal-x.run"}
	assert.Equal(t, 1, NextSealSeq(nil, "seal"), "empty manifest starts the sequence")
	assert.Equal(t, 6, NextSealSeq(names, "seal"), "foreign prefixes must not bump the sequence")
	assert.Equal(t, 10, NextSealSeq(names, "seal", "merge"), "the max ranges over all given prefixes")
	assert.Equal(t, 1, NextSealSeq([]string{"junk", "seal-x.run"}, "seal", "merge"))
}

// TestNextSealSeq_ParsesTheEnginesOwnNames closes the format→parse loop the
// grammar rests on: the producers are three fmt.Sprintf sites in the engines'
// startSeal (stores/event, stores/txhash), the parser is here, and nothing
// else pins the agreement. Names are formatted EXACTLY as those sites format
// them — same verb, same zero-pad width, same extension — so a change to
// either half that silently stops the sequence from resuming (a fresh seal
// then overwrites a live run) fails here.
func TestNextSealSeq_ParsesTheEnginesOwnNames(t *testing.T) {
	name := func(prefix string, seq int) string { return fmt.Sprintf("%s-%06d.run", prefix, seq) }
	for _, seq := range []int{1, 7, 42, 999999} {
		assert.Equal(t, seq+1, NextSealSeq([]string{name("seal", seq)}, "seal"),
			"a name the txhash engine wrote must resume at one past its sequence")
	}
	// The events engine writes both prefixes off one counter (startSeal's
	// seal, then the merge fold's), and warmup resumes past whichever is
	// newest.
	assert.Equal(t, 13, NextSealSeq([]string{name("seal", 11), name("merge", 12)}, "seal", "merge"))
}
