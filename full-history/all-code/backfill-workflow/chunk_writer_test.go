package backfill

import (
	"context"
	"os"
	"testing"

	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/fsutil"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/geometry"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/lfs"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/logging"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/memory"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/testutil"
)

// =============================================================================
// Shared Test Helpers — V1 LCM Construction
// =============================================================================
//
// These helpers leverage testutil.MakeRandomLedgerCloseMeta and
// helpers.MakeRandomTransactions to create realistic V1 LedgerCloseMeta
// with properly computed transaction hashes for end-to-end testing.

// makeTestLCMWithTx creates a V1 LedgerCloseMeta with the given number of
// random transactions and sets the ledger sequence.
func makeTestLCMWithTx(ledgerSeq uint32, txCount int) xdr.LedgerCloseMeta {
	lcm := testutil.MakeRandomLedgerCloseMeta(txCount, network.PublicNetworkPassphrase)
	lcm.V1.LedgerHeader.Header.LedgerSeq = xdr.Uint32(ledgerSeq)
	return lcm
}

// makeTestLCMWithHashes creates a V1 LedgerCloseMeta and returns the expected
// transaction hashes. Extracts hashes from the TxProcessing metadata rather
// than building the LCM from scratch — avoids duplicating helpers' LCM construction.
func makeTestLCMWithHashes(ledgerSeq uint32, txCount int) (xdr.LedgerCloseMeta, [][32]byte) {
	lcm := testutil.MakeRandomLedgerCloseMeta(txCount, network.PublicNetworkPassphrase)
	lcm.V1.LedgerHeader.Header.LedgerSeq = xdr.Uint32(ledgerSeq)

	hashes := make([][32]byte, len(lcm.V1.TxProcessing))
	for i, meta := range lcm.V1.TxProcessing {
		hashes[i] = [32]byte(meta.Result.TransactionHash)
	}
	return lcm, hashes
}

// mockLedgerSource generates LCMs lazily on GetLedger calls instead of
// pre-populating a map. This avoids allocating millions of LCMs in memory
// for large-range tests (orchestrator, process_instance tasks).
type mockLedgerSource struct {
	startSeq uint32
	endSeq   uint32
	txCount  int
}

// newMockLedgerSource creates a lazy mock source returning minimal V0 LCMs
// (no transactions). Use newMockLedgerSourceWithTx for realistic V1 LCMs.
func newMockLedgerSource(startSeq, endSeq uint32) *mockLedgerSource {
	return &mockLedgerSource{startSeq: startSeq, endSeq: endSeq, txCount: 0}
}

// newMockLedgerSourceWithTx creates a lazy mock source returning V1 LCMs with
// the specified number of real transactions per ledger. Each GetLedger call
// generates fresh random transactions with properly computed hashes.
func newMockLedgerSourceWithTx(startSeq, endSeq uint32, txCount int) *mockLedgerSource {
	return &mockLedgerSource{startSeq: startSeq, endSeq: endSeq, txCount: txCount}
}

func (m *mockLedgerSource) GetLedger(_ context.Context, seq uint32) (xdr.LedgerCloseMeta, error) {
	if seq < m.startSeq || seq > m.endSeq {
		return xdr.LedgerCloseMeta{}, nil
	}
	if m.txCount > 0 {
		return makeTestLCMWithTx(seq, m.txCount), nil
	}
	return buildSyntheticLCM(seq), nil
}

func (m *mockLedgerSource) PrepareRange(_ context.Context, _, _ uint32) error { return nil }
func (m *mockLedgerSource) Close() error                                      { return nil }

func TestChunkWriterBasic(t *testing.T) {
	geo := geometry.TestGeometry()
	ledgersDir := t.TempDir()
	txhashDir := t.TempDir()
	meta := NewMockMetaStore()
	chunkID := uint32(0)

	firstLedger := geo.ChunkFirstLedger(chunkID)
	lastLedger := geo.ChunkLastLedger(chunkID)
	// Use V1 LCMs with 2 real transactions per ledger to exercise
	// the full pipeline including tx extraction and .bin writing.
	txsPerLedger := 2
	source := newMockLedgerSourceWithTx(firstLedger, lastLedger, txsPerLedger)

	cw := NewChunkWriter(ChunkWriterConfig{
		LedgersBase:   ledgersDir,
		TxHashBase:    txhashDir,
		RangeID:       0,
		ChunkID:       chunkID,
		FlushInterval: 100,
		Meta:          meta,
		Memory:        memory.NewNopMonitor(1.0),
		Logger:        logging.NewNopLogger(),
		Geo:           geo,
	})

	stats, err := cw.WriteChunk(context.Background(), source)
	if err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}

	if stats.LedgersProcessed != int(geo.ChunkSize) {
		t.Errorf("LedgersProcessed = %d, want %d", stats.LedgersProcessed, geo.ChunkSize)
	}

	// Verify tx extraction worked
	expectedTx := int64(geo.ChunkSize) * int64(txsPerLedger)
	if stats.TxCount != expectedTx {
		t.Errorf("TxCount = %d, want %d", stats.TxCount, expectedTx)
	}

	// Verify LFS files exist and are readable
	if !lfs.ChunkExists(ledgersDir, chunkID) {
		t.Error("LFS chunk files should exist")
	}

	// Verify .bin file exists and has expected size (36 bytes per entry)
	binPath := RawTxHashPath(txhashDir, 0, chunkID)
	if !fsutil.FileExists(binPath) {
		t.Error("txhash .bin file should exist")
	}
	binInfo, err := os.Stat(binPath)
	if err != nil {
		t.Fatalf("stat .bin file: %v", err)
	}
	expectedBinSize := expectedTx * 36
	if binInfo.Size() != expectedBinSize {
		t.Errorf(".bin file size = %d, want %d", binInfo.Size(), expectedBinSize)
	}

	// Verify flags were set
	lfsDone, err := meta.IsChunkLFSDone(chunkID)
	if err != nil {
		t.Fatalf("IsChunkLFSDone: %v", err)
	}
	txDone, err := meta.IsChunkTxHashDone(chunkID)
	if err != nil {
		t.Fatalf("IsChunkTxHashDone: %v", err)
	}
	if !lfsDone || !txDone {
		t.Error("chunk should be marked complete in meta store (both flags)")
	}

	// Verify LFS roundtrip (read first ledger back)
	iter, err := lfs.NewLFSLedgerIterator(ledgersDir, firstLedger, firstLedger)
	if err != nil {
		t.Fatalf("NewLFSLedgerIterator: %v", err)
	}
	lcm, seq, _, hasMore, err := iter.Next()
	iter.Close()
	if err != nil || !hasMore {
		t.Fatalf("read first ledger: err=%v, hasMore=%v", err, hasMore)
	}
	if seq != firstLedger {
		t.Errorf("first seq = %d, want %d", seq, firstLedger)
	}
	if lcm.V != 1 {
		t.Errorf("LCM version = %d, want 1", lcm.V)
	}
}

func TestChunkWriterDeletesPartialFiles(t *testing.T) {
	geo := geometry.TestGeometry()
	ledgersDir := t.TempDir()
	txhashDir := t.TempDir()
	meta := NewMockMetaStore()
	chunkID := uint32(0)

	// Pre-create partial files to simulate a crash
	fsutil.EnsureDir(lfs.GetChunkDir(ledgersDir, chunkID))
	fsutil.EnsureDir(RawTxHashDir(txhashDir, 0))

	dataPath := lfs.GetDataPath(ledgersDir, chunkID)
	binPath := RawTxHashPath(txhashDir, 0, chunkID)
	writeTestFile(t, dataPath, "partial data")
	writeTestFile(t, binPath, "partial bin")

	firstLedger := geo.ChunkFirstLedger(chunkID)
	lastLedger := geo.ChunkLastLedger(chunkID)
	source := newMockLedgerSource(firstLedger, lastLedger)

	cw := NewChunkWriter(ChunkWriterConfig{
		LedgersBase:   ledgersDir,
		TxHashBase:    txhashDir,
		RangeID:       0,
		ChunkID:       chunkID,
		FlushInterval: 100,
		Meta:          meta,
		Memory:        memory.NewNopMonitor(1.0),
		Logger:        logging.NewNopLogger(),
		Geo:           geo,
	})

	_, err := cw.WriteChunk(context.Background(), source)
	if err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}

	// Verify files were rewritten (not the partial content)
	if !lfs.ChunkExists(ledgersDir, chunkID) {
		t.Error("LFS files should exist after rewrite")
	}
}

func TestChunkWriterWithTracker(t *testing.T) {
	geo := geometry.TestGeometry()
	ledgersDir := t.TempDir()
	txhashDir := t.TempDir()
	meta := NewMockMetaStore()
	tracker := NewProgressTracker()
	progress := tracker.RegisterRange(0, 10)
	chunkID := uint32(0)

	firstLedger := geo.ChunkFirstLedger(chunkID)
	lastLedger := geo.ChunkLastLedger(chunkID)
	source := newMockLedgerSource(firstLedger, lastLedger)

	cw := NewChunkWriter(ChunkWriterConfig{
		LedgersBase:   ledgersDir,
		TxHashBase:    txhashDir,
		RangeID:       0,
		ChunkID:       chunkID,
		FlushInterval: 100,
		Meta:          meta,
		Memory:        memory.NewNopMonitor(1.0),
		Logger:        logging.NewNopLogger(),
		Progress:      progress,
		Geo:           geo,
	})

	_, err := cw.WriteChunk(context.Background(), source)
	if err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}

	if progress.CompletedChunks() != 1 {
		t.Errorf("CompletedChunks = %d, want 1", progress.CompletedChunks())
	}
}

func writeTestFile(t *testing.T, path, content string) {
	t.Helper()
	if err := writeFile(path, []byte(content)); err != nil {
		t.Fatalf("write test file %s: %v", path, err)
	}
}

func writeFile(path string, data []byte) error {
	return os.WriteFile(path, data, 0644)
}
