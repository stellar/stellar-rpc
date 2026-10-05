package rocksdb

import (
	"errors"
	"fmt"
	"iter"
	"os"
	"path/filepath"

	"github.com/linxGnu/grocksdb"
)

// loadDirName is where LoadSorted builds a file before RocksDB takes it: a
// subdirectory of the store's own directory, which RocksDB does not scan.
const loadDirName = "loading"

// LoadSorted writes entries, which must ascend by key, as one file and moves
// it into cf. Readers see all of the entries or none, and they survive a
// crash once LoadSorted returns. An empty sequence is an error.
//
// The entries bypass the WAL and the memtable, but other writes to the store
// wait while RocksDB installs the file. The entries' key range must not
// overlap keys cf already holds: the file then lands below every other file
// of cf, and cf never needs compacting. An overlap fails the load.
//
// Each entry is written before the next is pulled, so entries may reuse
// their buffers. The store's lifecycle lock is held throughout, so Close
// waits for the load and entries must not call back into the store.
func (s *Store) LoadSorted(cf string, entries iter.Seq2[[]byte, []byte]) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.checkOpen(); err != nil {
		return err
	}
	if s.cfg.ReadOnly {
		return errors.New("rocksdb: LoadSorted on a read-only store")
	}
	if cf == "" {
		cf = defaultCFName
	}
	cfh, err := s.resolveCF(cf)
	if err != nil {
		return err
	}
	if err := s.loadSorted(cf, cfh, entries); err != nil {
		return fmt.Errorf("rocksdb: load sorted entries into %q: %w", cf, err)
	}
	return nil
}

func (s *Store) loadSorted(cf string, cfh *grocksdb.ColumnFamilyHandle, entries iter.Seq2[[]byte, []byte]) error {
	dir := filepath.Join(s.cfg.Path, loadDirName)
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return err
	}
	file, err := os.CreateTemp(dir, "*.sst")
	if err != nil {
		return err
	}
	path := file.Name()
	// Nothing is left to remove once RocksDB has moved the file.
	defer func() { _ = os.Remove(path) }()
	if err := file.Close(); err != nil {
		return err
	}

	env := grocksdb.NewDefaultEnvOptions()
	defer env.Destroy()
	// The writer copies cf's options, so the file carries cf's block size,
	// filter and compression.
	w := grocksdb.NewSSTFileWriter(env, s.cfOpts[cf])
	defer w.Destroy()
	if err := w.Open(path); err != nil {
		return err
	}
	for key, value := range entries {
		if err := w.Put(key, value); err != nil {
			return err
		}
	}
	if err := w.Finish(); err != nil {
		return err
	}

	opts := grocksdb.NewDefaultIngestExternalFileOptions()
	defer opts.Destroy()
	opts.SetMoveFiles(true)
	opts.SetFailIfNotBottommostLevel(true)
	return s.db.IngestExternalFileCF(cfh, []string{path}, opts)
}
