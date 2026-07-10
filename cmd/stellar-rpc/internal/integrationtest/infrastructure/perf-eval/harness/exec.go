package harness

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)

// RunStreaming runs name in dir (with extra env appended) and streams combined
// output to our log. On failure, the error carries the last tailN lines.
func RunStreaming(ctx context.Context, dir string, env []string, tailN int, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), env...)
	// The full stream goes to stderr (and on to the box's user-data log), but we
	// keep a bounded tail in memory for the error.
	tail := &tailWriter{max: 64 << 10}
	w := io.MultiWriter(os.Stderr, tail)
	cmd.Stdout, cmd.Stderr = w, w
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%w\n%s", err, lastLines(tail.String(), tailN))
	}
	return nil
}

// BailInstance writes the leg's failure body to resultsFile and returns msg as
// the error for the runner to exit with.
func BailInstance(resultsFile, title, runID, targetSHA, msg string) error {
	body := fmt.Sprintf("❌ **%s failed** (run %s on `%s`)\n\n```\n%s\n```\n", title, runID, targetSHA, msg)
	_ = os.WriteFile(resultsFile, []byte(body), 0o644)
	return errors.New(msg)
}

// tailWriter is a ring buffer-writer that retains the last max bytes written to it.
type tailWriter struct {
	max int
	buf []byte
}

func (w *tailWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	if len(w.buf) > w.max {
		w.buf = w.buf[len(w.buf)-w.max:]
	}
	return len(p), nil
}

func (w *tailWriter) String() string { return string(w.buf) }

func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}
