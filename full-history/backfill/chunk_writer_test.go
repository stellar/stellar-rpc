package backfill

import (
	"context"
	"os"
	"testing"

	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stellar/stellar-rpc/full-history/pkg/fsutil"
	"github.com/stellar/stellar-rpc/full-history/pkg/geometry"
	"github.com/stellar/stellar-rpc/full-history/pkg/logging"
	"github.com/stellar/stellar-rpc/full-history/pkg/memory"
	"github.com/stellar/stellar-rpc/full-history/pkg/testutil"
)

// =============================================================================
// Shared Test Helpers
// =============================================================================

func makeTestLCMWithTx(ledgerSeq uint32, txCount int) xdr.LedgerCloseMeta {
	lcm := testutil.MakeRandomLedgerCloseMeta(txCount, network.PublicNetworkPassphrase)
	lcm.V1.LedgerHeader.Header.LedgerSeq = xdr.Uint32(ledgerSeq)
	return lcm
}

func makeTestLCMWithHashes(ledgerSeq uint32, txCount int) (xdr.LedgerCloseMeta, [][32]byte) {
	lcm := testutil.MakeRandomLedgerCloseMeta(txCount, network.PublicNetworkPassphrase)
	lcm.V1.LedgerHeader.Header.LedgerSeq = xdr.Uint32(ledgerSeq)

	hashes := make([][32]byte, len(lcm.V1.TxProcessing))
	for i, meta := range lcm.V1.TxProcessing {
		hashes[i] = [32]byte(meta.Result.TransactionHash)
	}
	return lcm, hashes
}

type mockLedgerSource struct {
	startSeq uint32
	endSeq   uint32
	txCount  int
}

func newMockLedgerSource(startSeq, endSeq uint32) *mockLedgerSource {
	return &mockLedgerSource{startSeq: startSeq, endSeq: endSeq, txCount: 0}
}

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

// =============================================================================
// Per-Output Idempotency Tests
// =============================================================================

func TestChunkWriter_AllDone_Noop(t *testing.T) {
	meta := NewMockMetaStore()
	meta.SetChunkLFS(42)
	meta.SetChunkTxHash(42)
	meta.SetChunkEvents(42)

	cw := NewChunkWriter(ChunkWriterConfig{
		ChunkID:       42,
		LedgersPath:   t.TempDir(),
		TxHashRawPath: t.TempDir(),
		EventsPath:    t.TempDir(),
		Meta:          meta,
		Logger:        logging.NewNopLogger(),
		Geo:           geometry.TestGeometry(),
	})

	stats, err := cw.WriteChunk(context.Background(), nil)
	if err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}
	if !stats.Skipped {
		t.Error("expected no-op (skipped) when all outputs are complete")
	}
}

func TestChunkWriter_OnlyMissingOutputs(t *testing.T) {
	geo := geometry.TestGeometry()
	meta := NewMockMetaStore()
	chunkID := uint32(0)

	// LFS already done.
	meta.SetChunkLFS(chunkID)

	ledgersDir := t.TempDir()
	txhashDir := t.TempDir()
	eventsDir := t.TempDir()

	firstLedger := geo.ChunkFirstLedger(chunkID)
	lastLedger := geo.ChunkLastLedger(chunkID)
	source := newMockLedgerSourceWithTx(firstLedger, lastLedger, 2)

	cw := NewChunkWriter(ChunkWriterConfig{
		ChunkID:       chunkID,
		LedgersPath:   ledgersDir,
		TxHashRawPath: txhashDir,
		EventsPath:    eventsDir,
		Meta:          meta,
		Memory:        memory.NewNopMonitor(1.0),
		Logger:        logging.NewNopLogger(),
		Geo:           geo,
	})

	stats, err := cw.WriteChunk(context.Background(), source)
	if err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}

	if stats.Skipped {
		t.Error("should not skip — txhash and events are missing")
	}

	// TxHash should now be done.
	txDone, _ := meta.IsChunkTxHashDone(chunkID)
	if !txDone {
		t.Error("txhash flag should be set")
	}

	// Events should now be done.
	eventsDone, _ := meta.IsChunkEventsDone(chunkID)
	if !eventsDone {
		t.Error("events flag should be set")
	}

	// Verify .bin file exists.
	binPath := RawTxHashPath(txhashDir, chunkID)
	if !fsutil.FileExists(binPath) {
		t.Error("txhash .bin file should exist")
	}
}

func TestChunkWriter_IndependentFsyncOrder(t *testing.T) {
	geo := geometry.TestGeometry()
	meta := NewMockMetaStore()
	chunkID := uint32(0)

	firstLedger := geo.ChunkFirstLedger(chunkID)
	lastLedger := geo.ChunkLastLedger(chunkID)
	source := newMockLedgerSourceWithTx(firstLedger, lastLedger, 1)

	cw := NewChunkWriter(ChunkWriterConfig{
		ChunkID:       chunkID,
		LedgersPath:   t.TempDir(),
		TxHashRawPath: t.TempDir(),
		EventsPath:    t.TempDir(),
		Meta:          meta,
		Memory:        memory.NewNopMonitor(1.0),
		Logger:        logging.NewNopLogger(),
		Geo:           geo,
	})

	_, err := cw.WriteChunk(context.Background(), source)
	if err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}

	// All three flags should be set independently.
	lfsDone, _ := meta.IsChunkLFSDone(chunkID)
	txDone, _ := meta.IsChunkTxHashDone(chunkID)
	eventsDone, _ := meta.IsChunkEventsDone(chunkID)
	if !lfsDone || !txDone || !eventsDone {
		t.Errorf("all flags should be set: lfs=%v tx=%v events=%v", lfsDone, txDone, eventsDone)
	}

	// Verify calls are individual SetChunkLFS, SetChunkTxHash, SetChunkEvents (not SetChunkFlags).
	foundLFS, foundTx, foundEvents := false, false, false
	for _, call := range meta.Calls {
		if call == "SetChunkLFS(0)" {
			foundLFS = true
		}
		if call == "SetChunkTxHash(0)" {
			foundTx = true
		}
		if call == "SetChunkEvents(0)" {
			foundEvents = true
		}
	}
	if !foundLFS || !foundTx || !foundEvents {
		t.Errorf("expected individual flag calls, got: %v", meta.Calls)
	}
}

func TestChunkWriterBasic(t *testing.T) {
	geo := geometry.TestGeometry()
	meta := NewMockMetaStore()
	chunkID := uint32(0)

	firstLedger := geo.ChunkFirstLedger(chunkID)
	lastLedger := geo.ChunkLastLedger(chunkID)
	txsPerLedger := 2
	source := newMockLedgerSourceWithTx(firstLedger, lastLedger, txsPerLedger)

	ledgersDir := t.TempDir()
	txhashDir := t.TempDir()

	cw := NewChunkWriter(ChunkWriterConfig{
		ChunkID:       chunkID,
		LedgersPath:   ledgersDir,
		TxHashRawPath: txhashDir,
		EventsPath:    t.TempDir(),
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

	expectedTx := int64(geo.ChunkSize) * int64(txsPerLedger)
	if stats.TxCount != expectedTx {
		t.Errorf("TxCount = %d, want %d", stats.TxCount, expectedTx)
	}

	// Verify .bin file size.
	binPath := RawTxHashPath(txhashDir, chunkID)
	binInfo, err := os.Stat(binPath)
	if err != nil {
		t.Fatalf("stat .bin file: %v", err)
	}
	if binInfo.Size() != expectedTx*36 {
		t.Errorf(".bin file size = %d, want %d", binInfo.Size(), expectedTx*36)
	}

	// Verify all flags set.
	lfsDone, _ := meta.IsChunkLFSDone(chunkID)
	txDone, _ := meta.IsChunkTxHashDone(chunkID)
	eventsDone, _ := meta.IsChunkEventsDone(chunkID)
	if !lfsDone || !txDone || !eventsDone {
		t.Errorf("flags: lfs=%v tx=%v events=%v", lfsDone, txDone, eventsDone)
	}
}

func writeTestFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write test file %s: %v", path, err)
	}
}
