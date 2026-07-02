package backfill

import (
	"sync/atomic"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// fakeHotChunk is a test HotChunk: a hand-set MaxCommittedSeq + an injectable
// LedgerStream source, counting closes when closedTo is non-nil.
type fakeHotChunk struct {
	maxSeq   uint32
	present  bool
	maxErr   error
	source   ledgerbackend.LedgerStream
	closedTo *atomic.Int32
}

func (h *fakeHotChunk) MaxCommittedSeq() (uint32, bool, error) {
	return h.maxSeq, h.present, h.maxErr
}
func (h *fakeHotChunk) Source() ledgerbackend.LedgerStream { return h.source }
func (h *fakeHotChunk) Close() error {
	if h.closedTo != nil {
		h.closedTo.Add(1)
	}
	return nil
}

// fakeHotProbe is a test HotProbe: returns its fake chunk when ok, an error when
// openErr is set, or (nil,false,nil) for "no ready hot DB". Counts opens via
// openedTo when non-nil.
type fakeHotProbe struct {
	chunk    *fakeHotChunk
	ok       bool
	openErr  error
	openedTo *atomic.Int32
}

func (p *fakeHotProbe) OpenHotChunk(chunk.ID) (HotChunk, bool, error) {
	if p.openedTo != nil {
		p.openedTo.Add(1)
	}
	if p.openErr != nil {
		return nil, false, p.openErr
	}
	if !p.ok {
		return nil, false, nil
	}
	return p.chunk, true, nil
}
