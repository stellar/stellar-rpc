package txhash

// run_reader.go — the ONE streaming reader for TXHRUN01 run files (format
// and header gates live beside the writer in hotindex_seal.go). runSource
// drains a run sequentially through a 128KiB buffer, verifying as it
// streams: CRC64 folded over the payload (checked against the header at
// end-of-stream), hash sortedness, and per-record seq containment in the
// header's ledger range — with the header count already cross-checked
// against the file size at open. Two drivers consume it: warmup's routing
// rebuild (openSealedRun) drains one run and takes the verified fd for the
// lookup path (handoff), and the freeze merge (cold_freeze.go) drains every
// manifest-listed run as one k-way source.

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc64"
	"io"
	"os"
)

// runSource streams one sealed run file sequentially, folding CRC64 over
// the drain: the CRC (and the header count vs file size) is verified by the
// time the source reports end-of-stream, so a torn or corrupt run fails its
// consumer loudly. Per-record hash order is checked as it streams (merge
// correctness and the .bin's sort order both ride on it), and every seq
// must sit inside the header's promised ledger range.
type runSource struct {
	path      string
	f         *os.File
	br        *bufio.Reader
	crc       hash.Hash64
	hdr       runHeader
	recs      int
	remaining int
	rec       [runRecordLen]byte
	prev      [rowHashLen]byte
}

func (r *runSource) records() int { return r.recs }

// openRunSource opens and structurally validates a run for sequential
// draining: header magic/shape (readRunHeader) and count vs file size
// (runRecordCount), the shared open gates.
func openRunSource(path string) (*runSource, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("run %s: %w", path, err)
	}
	hdr, err := readRunHeader(f, path)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	records, err := runRecordCount(f, path, hdr)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return &runSource{
		path: path, f: f, br: bufio.NewReaderSize(f, 128<<10),
		crc: crc64.New(crcRunTable), hdr: hdr, recs: records, remaining: records,
	}, nil
}

func (r *runSource) hash() []byte { return r.rec[:rowHashLen] }

func (r *runSource) seq() uint32 { return binary.LittleEndian.Uint32(r.rec[rowHashLen:]) }

func (r *runSource) advance() (bool, error) {
	if r.remaining == 0 {
		if r.crc.Sum64() != r.hdr.crc {
			return false, fmt.Errorf("run %s: payload crc mismatch (file %016x, computed %016x)",
				r.path, r.hdr.crc, r.crc.Sum64())
		}
		return false, nil
	}
	idx := r.recs - r.remaining
	if idx > 0 {
		copy(r.prev[:], r.rec[:rowHashLen])
	}
	if _, err := io.ReadFull(r.br, r.rec[:]); err != nil {
		return false, fmt.Errorf("run %s: record %d: %w", r.path, idx, err)
	}
	_, _ = r.crc.Write(r.rec[:])
	if idx > 0 && bytes.Compare(r.prev[:], r.rec[:rowHashLen]) > 0 {
		return false, fmt.Errorf("run %s: record %d out of hash order", r.path, idx)
	}
	if seq := r.seq(); seq < r.hdr.first || seq > r.hdr.last {
		return false, fmt.Errorf("run %s: record %d seq %d outside [%d,%d]",
			r.path, idx, seq, r.hdr.first, r.hdr.last)
	}
	r.remaining--
	return true, nil
}

// close releases the file handle; safe in any state, a no-op after handoff.
func (r *runSource) close() {
	if r.f != nil {
		_ = r.f.Close()
	}
}

// handoff surrenders the open file handle — the fd whose bytes the drain
// verified — and neuters close(), so the verified fd IS the fd that serves
// lookup preads (openSealedRun). Must be the caller's last call on the
// source.
func (r *runSource) handoff() *os.File {
	f := r.f
	r.f = nil
	return f
}
