package backfill

import (
	"context"
	"os"
	"testing"

	"github.com/stellar/stellar-rpc/full-history/pkg/fsutil"
	"github.com/stellar/stellar-rpc/full-history/pkg/geometry"
	"github.com/stellar/stellar-rpc/full-history/pkg/logging"
)

func TestProcessChunkTask_NoopWhenComplete(t *testing.T) {
	meta := NewMockMetaStore()
	meta.SetChunkLFS(42)
	meta.SetChunkTxHash(42)
	meta.SetChunkEvents(42)

	task := &processChunkTask{
		id:      ProcessChunkTaskID(42),
		chunkID: 42,
		meta:    meta,
		log:     logging.NewNopLogger(),
		geo:     geometry.TestGeometry(),
	}

	err := task.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	// No factory was set — if it tried to create a source, it would panic.
	// The fact that it didn't panic proves the no-op path was taken.
}

func TestBuildTxHashTask_NoopWhenComplete(t *testing.T) {
	meta := NewMockMetaStore()
	meta.SetIndexTxHash(0)

	task := &buildTxHashIndexTask{
		id:      BuildTxHashIndexTaskID(0),
		indexID: 0,
		meta:    meta,
		log:     logging.NewNopLogger(),
		geo:     geometry.TestGeometry(),
	}

	err := task.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	// No RecSplitFlow was created — no-op.
}

func TestCleanupTxHashTask(t *testing.T) {
	geo := geometry.TestGeometry() // 5 chunks per index
	meta := NewMockMetaStore()
	txhashDir := t.TempDir()

	// Simulate: index built, chunk txhash keys + .bin files still exist.
	meta.SetIndexTxHash(0)
	firstChunk := geo.IndexFirstChunk(0)
	lastChunk := geo.IndexLastChunk(0)

	for chunkID := firstChunk; chunkID <= lastChunk; chunkID++ {
		meta.SetChunkTxHash(chunkID)

		// Create .bin file on disk.
		binPath := RawTxHashPath(txhashDir, chunkID)
		fsutil.EnsureDir(RawTxHashDir(txhashDir, chunkID))
		os.WriteFile(binPath, []byte("fake bin data"), 0644)
	}

	task := &cleanupTxHashTask{
		id:            CleanupTxHashTaskID(0),
		indexID:       0,
		txHashRawPath: txhashDir,
		meta:          meta,
		log:           logging.NewNopLogger(),
		geo:           geo,
	}

	err := task.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	// Verify: .bin files deleted.
	for chunkID := firstChunk; chunkID <= lastChunk; chunkID++ {
		binPath := RawTxHashPath(txhashDir, chunkID)
		if fsutil.FileExists(binPath) {
			t.Errorf("chunk %d .bin file should be deleted", chunkID)
		}
	}

	// Verify: chunk:*:txhash keys deleted.
	for chunkID := firstChunk; chunkID <= lastChunk; chunkID++ {
		done, _ := meta.IsChunkTxHashDone(chunkID)
		if done {
			t.Errorf("chunk %d txhash key should be deleted", chunkID)
		}
	}
}

func TestCleanupTxHashTask_PartiallyCleanedUp(t *testing.T) {
	geo := geometry.TestGeometry()
	meta := NewMockMetaStore()
	txhashDir := t.TempDir()

	// Simulate: chunks 0 and 1 already cleaned, chunks 2-4 still have keys.
	meta.SetChunkTxHash(2)
	meta.SetChunkTxHash(3)
	meta.SetChunkTxHash(4)

	for chunkID := uint32(2); chunkID <= 4; chunkID++ {
		binPath := RawTxHashPath(txhashDir, chunkID)
		fsutil.EnsureDir(RawTxHashDir(txhashDir, chunkID))
		os.WriteFile(binPath, []byte("data"), 0644)
	}

	task := &cleanupTxHashTask{
		id:            CleanupTxHashTaskID(0),
		indexID:       0,
		txHashRawPath: txhashDir,
		meta:          meta,
		log:           logging.NewNopLogger(),
		geo:           geo,
	}

	err := task.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	// All remaining keys should be gone.
	for chunkID := uint32(0); chunkID <= 4; chunkID++ {
		done, _ := meta.IsChunkTxHashDone(chunkID)
		if done {
			t.Errorf("chunk %d txhash key should be absent", chunkID)
		}
	}
}
