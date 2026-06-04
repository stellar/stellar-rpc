package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/xdr"

	chunkPkg "github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// writeLCMFile writes a framed-XDR LedgerCloseMeta stream whose ledgers carry
// the given sequence numbers, mirroring apply-load's METADATA_OUTPUT_STREAM.
func writeLCMFile(t *testing.T, seqs []uint32) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "meta.xdr")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer f.Close()
	for _, s := range seqs {
		lcm := xdr.LedgerCloseMeta{
			V: 0,
			V0: &xdr.LedgerCloseMetaV0{
				LedgerHeader: xdr.LedgerHeaderHistoryEntry{
					Header: xdr.LedgerHeader{LedgerSeq: xdr.Uint32(s)},
				},
			},
		}
		if err := xdr.MarshalFramed(f, lcm); err != nil {
			t.Fatalf("MarshalFramed seq=%d: %v", s, err)
		}
	}
	return path
}

// collectSeqs drains an lcmStream for one chunk and returns the decoded
// ledger sequences it yielded (or the first error).
func collectSeqs(t *testing.T, st *lcmStream, want int) ([]uint32, error) {
	t.Helper()
	rng := ledgerbackend.BoundedRange(1, uint32(want))
	var got []uint32
	for raw, err := range st.RawLedgers(context.Background(), rng) {
		if err != nil {
			return got, err
		}
		var lcm xdr.LedgerCloseMeta
		if err := lcm.UnmarshalBinary(raw); err != nil {
			t.Fatalf("unmarshal yielded payload: %v", err)
		}
		got = append(got, lcm.LedgerSequence())
	}
	return got, nil
}

func TestLCMStreamSkipsSetupAndMapsBlocks(t *testing.T) {
	// 3 setup ledgers (seq 1..3) then 12 benchmark ledgers (seq 100..111).
	// checkpoint=3 means everything <=3 is setup.
	setup := []uint32{1, 2, 3}
	bench := []uint32{100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111}
	file := writeLCMFile(t, append(append([]uint32{}, setup...), bench...))

	const base = chunkPkg.ID(7) // arbitrary base; block = chunkID - base

	tests := []struct {
		name    string
		chunkID chunkPkg.ID
		want    int
		expect  []uint32
	}{
		{"block0", base, 4, []uint32{100, 101, 102, 103}},
		{"block1", base + 1, 4, []uint32{104, 105, 106, 107}},
		{"block2", base + 2, 4, []uint32{108, 109, 110, 111}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st := &lcmStream{
				opts:    lcmOpts{file: file, checkpoint: 3, baseChunk: base},
				chunkID: tc.chunkID,
			}
			got, err := collectSeqs(t, st, tc.want)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) != len(tc.expect) {
				t.Fatalf("got %v, want %v", got, tc.expect)
			}
			for i := range got {
				if got[i] != tc.expect[i] {
					t.Fatalf("got %v, want %v", got, tc.expect)
				}
			}
		})
	}
}

func TestLCMStreamNoCheckpointSkip(t *testing.T) {
	// checkpoint=0: nothing is skipped, the first frame is benchmark block 0.
	file := writeLCMFile(t, []uint32{10, 11, 12})
	st := &lcmStream{opts: lcmOpts{file: file, checkpoint: 0, baseChunk: 1}, chunkID: 1}
	got, err := collectSeqs(t, st, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 3 || got[0] != 10 || got[2] != 12 {
		t.Fatalf("got %v, want [10 11 12]", got)
	}
}

func TestLCMStreamNotEnoughLedgers(t *testing.T) {
	// 2 benchmark ledgers but the chunk asks for 4 → short-read error.
	file := writeLCMFile(t, []uint32{1, 100, 101})
	st := &lcmStream{opts: lcmOpts{file: file, checkpoint: 1, baseChunk: 1}, chunkID: 1}
	_, err := collectSeqs(t, st, 4)
	if err == nil {
		t.Fatalf("expected short-read error, got nil")
	}
}
