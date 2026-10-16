package bench

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// stagedFiles holds report files under temporary names until publish gives
// them their final names.
type stagedFiles struct {
	files []stagedFile
}

type stagedFile struct {
	tmp, path string
}

// stage writes data to a temporary file next to path and keeps it for
// publish. On error it removes the temporary file and keeps nothing.
func (s *stagedFiles) stage(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*.tmp")
	if err != nil {
		return fmt.Errorf("stage %s: %w", path, err)
	}
	_, werr := tmp.Write(data)
	// Readable like the CSVs that os.Create writes; the report is not secret.
	merr := tmp.Chmod(0o644)
	cerr := tmp.Close()
	if err := errors.Join(werr, merr, cerr); err != nil {
		_ = os.Remove(tmp.Name())
		return fmt.Errorf("stage %s: %w", path, err)
	}
	s.files = append(s.files, stagedFile{tmp: tmp.Name(), path: path})
	return nil
}

// publish renames every staged file to its final name.
func (s *stagedFiles) publish() error {
	for _, f := range s.files {
		if err := os.Rename(f.tmp, f.path); err != nil {
			return fmt.Errorf("publish %s: %w", f.path, err)
		}
	}
	s.files = nil
	return nil
}

// discard removes every staged file.
func (s *stagedFiles) discard() {
	for _, f := range s.files {
		_ = os.Remove(f.tmp)
	}
	s.files = nil
}
