package config

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/durable"
)

// Root preparation. The daemon creates every configured storage root up front
// (fresh deployments start from nothing) and fsyncs the just-created directory
// chain: MkdirAll does not fsync the direntries naming the new dirs, and the
// one-write protocol's grandparent fsync only reaches a root's contents, not
// the root's own link — so without this a fresh-deploy crash could lose a
// whole storage tree while the synced catalog still advertises a "frozen"
// artifact under it.
//
// Single-process enforcement rides on the catalog: catalog.Open holds RocksDB's
// own exclusive LOCK, so a second daemon on the same catalog fails to start.
// The other roots need no lock of their own — cold artifact trees are
// write-once behind the one-write protocol (two writers produce identical
// bytes and the atomic rename resolves the race: redundant work, not
// corruption), and each hot chunk DB carries its own RocksDB LOCK.

// PrepareRoots creates each non-empty root (if absent) and fsyncs the new
// directory chain. Idempotent — existing roots are left untouched.
func PrepareRoots(roots ...string) error {
	for _, root := range roots {
		if root == "" {
			continue
		}
		abs, err := filepath.Abs(root)
		if err != nil {
			return fmt.Errorf("resolve storage root %q: %w", root, err)
		}

		existing := durable.DeepestExistingDir(abs)
		if err := os.MkdirAll(abs, 0o755); err != nil {
			return fmt.Errorf("create storage root %q: %w", abs, err)
		}
		if err := durable.FsyncNewDirs(existing, abs); err != nil {
			return fmt.Errorf("fsync storage root %q: %w", abs, err)
		}
	}
	return nil
}
