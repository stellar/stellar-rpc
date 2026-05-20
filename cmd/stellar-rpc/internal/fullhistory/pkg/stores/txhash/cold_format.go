package txhash

// cold_format.go defines the on-disk format for the cold txhash index
// and the streamhash MPHF wrapper shared by the cold writer and reader.
//
// The cold index is a single global file covering the entire cold
// ledger store: one streamhash MPHF over every (txhash, ledgerSeq)
// pair. Unlike the eventstore (which needs a separate index.pack for
// variable-length roaring bitmaps), the txhash payload is a fixed
// 4-byte ledgerSeq that fits natively in streamhash's per-key payload
// slot. One file is enough.
//
// On-disk layout:
//
//   - WithUnsortedInput()         streamhash buffers keys in temp files
//                                 and builds blocks in parallel on Finish.
//                                 AddKey order is whatever the caller
//                                 chooses; here it's cold-pack scan order.
//   - WithPayload(coldPayloadSize)        per-key payload = uint32 ledgerSeq.
//   - WithFingerprint(coldFingerprintSize)  4-byte fingerprint that streamhash
//                                 verifies internally on Query — unseen
//                                 keys return streamerrors.ErrNotFound
//                                 directly, so the caller doesn't need a
//                                 second post-Lookup check.

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/tamirms/streamhash"
	streamerrors "github.com/tamirms/streamhash/errors"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
)

// ColdIndexName is the conventional filename for the cold txhash
// index. Producers and consumers compose the full path as
// {coldRoot}/{ColdIndexName}.
const ColdIndexName = "txhash.idx"

// coldPayloadSize — bytes of per-key payload in the MPHF. The payload
// is a uint32 ledgerSeq; streamhash packs the low payloadSize bytes
// of the uint64 passed to AddKey in little-endian order.
const coldPayloadSize = 4

// coldFingerprintSize — bytes of per-key fingerprint streamhash
// verifies on Query. 4 bytes gives ~1-in-2^32 residual collision
// probability for unseen keys, which is sufficient for txhash:
// the consequence of a false positive is a misrouted ledgerSeq
// that downstream tx-hash verification (linear scan in the LCM)
// will reject anyway.
const coldFingerprintSize = 4

// ErrEmptyBuildSet is returned when the caller tries to construct
// a ColdIndexWriter with totalKeys == 0. A streamhash MPHF over zero
// keys is degenerate.
var ErrEmptyBuildSet = errors.New("txhash: cannot build cold index with zero keys")

// Hash compatibility: streamhash's AddKey/Query take the first 16
// bytes of the supplied key as the routing identity and do NOT
// re-hash. 32-byte SHA-256 transaction hashes are already uniformly
// distributed in their prefix bits, so the caller passes hash[:]
// through unchanged. No PreHashInPlace, no double-hashing.

// coldMPHF wraps a streamhash Index for the txhash cold lookup path.
// Lifecycle is owned by ColdReader, which provides the close guard;
// methods are unexported because nothing outside this package calls
// them.
type coldMPHF struct {
	idx *streamhash.Index
}

// newColdBuilder constructs a streamhash builder configured for the
// cold txhash index. Shared by the writer constructor and any
// in-process build helpers in tests so the option set stays in one
// place.
func newColdBuilder(ctx context.Context, outputPath string, totalKeys uint64) (*streamhash.Builder, error) {
	if totalKeys == 0 {
		return nil, ErrEmptyBuildSet
	}
	b, err := streamhash.NewBuilder(ctx, outputPath, totalKeys,
		streamhash.WithUnsortedInput(),
		streamhash.WithPayload(coldPayloadSize),
		streamhash.WithFingerprint(coldFingerprintSize),
	)
	if err != nil {
		return nil, fmt.Errorf("txhash: create streamhash builder: %w", err)
	}
	return b, nil
}

// openColdMPHF mmaps the streamhash index at path. The streamhash
// library opens, mmaps, and closes the fd internally (Open closes
// the underlying file after mmap, per POSIX); Close releases the
// mmap.
func openColdMPHF(path string) (*coldMPHF, error) {
	idx, err := streamhash.Open(path)
	if err != nil {
		return nil, fmt.Errorf("txhash: open cold index %s: %w", path, err)
	}
	return &coldMPHF{idx: idx}, nil
}

// lookup returns the ledgerSeq stored under hash, or stores.ErrNotFound
// when streamhash's fingerprint check proves hash was not in the build
// set. Concurrent calls are safe — streamhash.Index supports concurrent
// reads.
func (m *coldMPHF) lookup(hash [32]byte) (uint32, error) {
	payload, err := m.idx.QueryPayload(hash[:])
	if err != nil {
		if errors.Is(err, streamerrors.ErrNotFound) {
			return 0, stores.ErrNotFound
		}
		return 0, fmt.Errorf("txhash: cold MPHF query: %w", err)
	}
	if payload > math.MaxUint32 {
		// Payload size is coldPayloadSize (4); streamhash zero-extends
		// to uint64. A value above MaxUint32 would mean the on-disk
		// representation was wider than configured — corruption.
		return 0, fmt.Errorf("%w: txhash: ledgerSeq payload %d exceeds uint32", stores.ErrCorrupt, payload)
	}
	return uint32(payload), nil
}

func (m *coldMPHF) close() error {
	if err := m.idx.Close(); err != nil {
		return fmt.Errorf("txhash: close cold MPHF: %w", err)
	}
	return nil
}
