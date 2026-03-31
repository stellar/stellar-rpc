package backfill

import (
	"context"
	"fmt"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stellar/stellar-rpc/full-history/pkg/fsutil"
	"github.com/stellar/stellar-rpc/full-history/pkg/lfs"
)

// =============================================================================
// LFS Packfile Source
// =============================================================================
//
// LFSPackfileSource implements LedgerSource by reading from an already-written
// LFS packfile. Used for the LFS-first path: when chunk:{C}:lfs is set but
// chunk:{C}:txhash is absent, we read ledgers from the local LFS file instead
// of re-fetching from GCS.
//
// This is faster and removes the GCS dependency for the txhash re-write path.
//
// Internally wraps lfs.LFSLedgerIterator for sequential reads through the chunk.

// LFSPackfileSource reads ledgers from a local LFS packfile.
type LFSPackfileSource struct {
	ledgersPath string
	chunkID     uint32
	iter        *lfs.LFSLedgerIterator

	// Cache: the iterator is sequential, but GetLedger may be called
	// for any ledger in the chunk. We keep the last-read LCM in case
	// the caller asks for them in order (the common case for ChunkWriter).
	lastSeq uint32
	lastLCM xdr.LedgerCloseMeta
	hasLast bool
}

// NewLFSPackfileSource opens the LFS packfile for the given chunk.
func NewLFSPackfileSource(ledgersPath string, chunkID uint32, startSeq, endSeq uint32) (*LFSPackfileSource, error) {
	// Verify the chunk exists before creating the iterator
	if !fsutil.FileExists(LedgerPackPath(ledgersPath, chunkID)) {
		return nil, fmt.Errorf("LFS chunk %d does not exist in %s", chunkID, ledgersPath)
	}

	iter, err := lfs.NewLFSLedgerIterator(ledgersPath, startSeq, endSeq)
	if err != nil {
		return nil, fmt.Errorf("open LFS packfile for chunk %d: %w", chunkID, err)
	}

	return &LFSPackfileSource{
		ledgersPath: ledgersPath,
		chunkID:     chunkID,
		iter:        iter,
	}, nil
}

func (s *LFSPackfileSource) GetLedger(_ context.Context, ledgerSeq uint32) (xdr.LedgerCloseMeta, error) {
	// Fast path: return cached LCM if it matches
	if s.hasLast && s.lastSeq == ledgerSeq {
		return s.lastLCM, nil
	}

	// Sequential read: advance the iterator until we reach the requested ledger.
	// ChunkWriter calls GetLedger in order, so this loop body typically
	// executes exactly once per call.
	for {
		lcm, seq, _, hasMore, err := s.iter.Next()
		if err != nil {
			return xdr.LedgerCloseMeta{}, fmt.Errorf("read ledger %d from LFS chunk %d: %w", ledgerSeq, s.chunkID, err)
		}
		if !hasMore {
			return xdr.LedgerCloseMeta{}, fmt.Errorf("ledger %d not found in LFS chunk %d: iterator exhausted", ledgerSeq, s.chunkID)
		}
		if seq == ledgerSeq {
			s.lastSeq = seq
			s.lastLCM = lcm
			s.hasLast = true
			return lcm, nil
		}
		if seq > ledgerSeq {
			return xdr.LedgerCloseMeta{}, fmt.Errorf("ledger %d not found in LFS chunk %d: iterator advanced past to %d", ledgerSeq, s.chunkID, seq)
		}
	}
}

func (s *LFSPackfileSource) PrepareRange(_ context.Context, _, _ uint32) error {
	return nil // LFS files are local — no prefetching needed
}

func (s *LFSPackfileSource) Close() error {
	if s.iter != nil {
		s.iter.Close()
		s.iter = nil
	}
	return nil
}
