package bench

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
)

// A staged file gets its final name only on publish; discard leaves nothing.
func TestStagedFilesPublishAndDiscard(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, queryBenchFile)

	var s stagedFiles
	require.NoError(t, s.stage(path, []byte("lines")))
	assert.NoFileExists(t, path)
	require.NoError(t, s.publish())
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, "lines", string(data))
	assert.Equal(t, []string{queryBenchFile}, dirNames(t, dir), "no temporary file stays")

	other := filepath.Join(t.TempDir(), queryBenchFile)
	require.NoError(t, s.stage(other, []byte("lines")))
	s.discard()
	assert.Empty(t, dirNames(t, filepath.Dir(other)))
}

// A stage that cannot write leaves no file, under either name.
func TestStagedFilesStageFailure(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o500))
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

	var s stagedFiles
	require.Error(t, s.stage(filepath.Join(dir, queryBenchFile), []byte("lines")))
	assert.Empty(t, dirNames(t, dir))
	require.NoError(t, s.publish(), "nothing was staged")
	assert.Empty(t, dirNames(t, dir))
}

// runStagingCommand runs a bench command whose run stages bench.txt and then
// calls after, and returns the command's error.
func runStagingCommand(t *testing.T, out string, after func(env runEnv) error) error {
	t.Helper()
	cmd := newBenchCommand("stage", "test", &profileFlags{}, func(string) error { return nil },
		func(_ context.Context, _ *supportlog.Entry, env runEnv) error {
			if err := env.Staged.stage(filepath.Join(env.OutDir, queryBenchFile), []byte("lines")); err != nil {
				return err
			}
			return after(env)
		})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"--out", out})
	return cmd.Execute()
}

// bench.txt appears only when the final run.json records the run as ok.
func TestBenchFileFollowsRunRecord(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		out := filepath.Join(t.TempDir(), "out")
		require.NoError(t, runStagingCommand(t, out, func(runEnv) error { return nil }))
		assert.FileExists(t, filepath.Join(out, queryBenchFile))
	})
	t.Run("failed run", func(t *testing.T) {
		out := filepath.Join(t.TempDir(), "out")
		boom := errors.New("boom")
		require.ErrorIs(t, runStagingCommand(t, out, func(runEnv) error { return boom }), boom)
		assert.Equal(t, []string{runRecordFile}, dirNames(t, out))
	})
	t.Run("final run.json write fails", func(t *testing.T) {
		out := filepath.Join(t.TempDir(), "out")
		t.Cleanup(func() { _ = os.Chmod(out, 0o700) })
		err := runStagingCommand(t, out, func(env runEnv) error { return os.Chmod(env.OutDir, 0o500) })
		require.ErrorContains(t, err, runRecordFile)
		assert.NoFileExists(t, filepath.Join(out, queryBenchFile))
	})
}
