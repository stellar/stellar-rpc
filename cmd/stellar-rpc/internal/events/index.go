package events

import (
	"fmt"
	"iter"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/tamirms/streamhash"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// TermKey is a 16-byte hash identifying a unique (field, value) pair
// in the bitmap index.
type TermKey [16]byte

// Field identifies which indexed field a term belongs to.
type Field byte

const (
	FieldContractID Field = 0
	FieldTopic0     Field = 1
	FieldTopic1     Field = 2
	FieldTopic2     Field = 3
	FieldTopic3     Field = 4
)

// ComputeTermKey computes a 16-byte term key by hashing the field byte
// followed by the value bytes: xxh3_128(field || value), encoded as
// two little-endian uint64s.
//
// Routed through streamhash.PreHashInPlace so the hash function and
// byte encoding stay aligned with the MPHF builder downstream
// (mphf.go). Including the field byte in the hash input ensures the
// same value in different fields produces different keys.
func ComputeTermKey(value []byte, field Field) TermKey {
	// Prepend field byte to value for hashing.
	// Stack-allocated buffer avoids heap allocation for typical ScVal sizes.
	var scratch [128]byte
	n := len(value) + 1
	var buf []byte
	if n <= len(scratch) {
		buf = scratch[:n]
	} else {
		buf = make([]byte, n)
	}
	buf[0] = byte(field)
	copy(buf[1:], value)

	var key TermKey
	streamhash.PreHashInPlace(key[:], buf)
	return key
}

// BitmapIndex is the read/write surface for an in-chunk term index.
// Implementations map TermKeys to roaring bitmaps of chunk-relative
// event IDs and must be safe for one writer and multiple readers.
//
// Concurrency:
//
//   - AddTo is idempotent. Callers must add eventIDs in monotonically
//     increasing order per term; the same (key, eventID) pair has the
//     same effect added once or many times.
//   - Get returns the BORROWED bitmap pointer (no defensive clone).
//     Callers MUST NOT mutate it — the bitmap is the live mirror
//     state in dense mode and a mutation would corrupt the index
//     for every other reader. Returned pointers are valid as long
//     as no concurrent AddTo touches the same key; in the hot
//     store callers serialize via the HotStore's exclusive ingest
//     lock (reads happen between IngestLedgerEvents calls).
//   - All holds a read lock for the duration of the iteration. The
//     yielded *roaring.Bitmap is the live pointer inside the index —
//     valid only inside the iteration body. The read lock prevents
//     concurrent writers (AddTo); concurrent Get calls under the
//     same read lock are safe because both are read-only.
//
// Freeze coordination is the orchestrator's responsibility — stop
// AddTo before iterating via All. Concurrent AddTo would still be
// blocked by the read lock, but it isn't expected to happen.
type BitmapIndex interface {
	AddTo(key TermKey, eventIDs ...uint32)
	Get(key TermKey) (*roaring.Bitmap, error)
	All() iter.Seq2[TermKey, *roaring.Bitmap]
	Len() int64
}

// TermsFor returns the 16-byte hashed term keys (contractID + topics
// 0..MaxTopicCount-1) applicable to ev. Topics beyond MaxTopicCount
// are not indexed because the getEvents filter API can't match them
// anyway (see topicField).
//
// Callers pair each returned key with the event's chunk-relative
// event ID at the call site — TermsFor itself doesn't track event
// IDs because they're a property of the writer's scheduling
// (start_id + i), not of the event's contents.
//
// Used by:
//   - Writer.IngestLedgerEvents — derives terms internally on
//     each per-ledger commit.
//   - Backfill — derives terms per payload while populating an
//     in-memory EventIndex for later WriteIndex.
//
// The freeze path does NOT call this — it reuses the hot reader's
// already-built in-memory index directly via WriteIndex.
//
// A MarshalBinary failure on a topic surfaces as an error rather
// than a silent skip. Stellar-core has already validated the XDR
// when it closed the ledger, so a failure here signals corruption
// somewhere in the pipeline — the right response is to reject the
// ledger commit (leaving on-disk state untouched), not to
// under-index events and continue.
func TermsFor(ev xdr.ContractEvent) ([]TermKey, error) {
	var keys []TermKey
	if ev.ContractId != nil {
		keys = append(keys, ComputeTermKey(ev.ContractId[:], FieldContractID))
	}
	v0, ok := ev.Body.GetV0()
	if !ok {
		return keys, nil
	}
	for i, topic := range v0.Topics {
		if i >= protocol.MaxTopicCount {
			break
		}
		raw, err := topic.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("events: marshal topic %d: %w", i, err)
		}
		keys = append(keys, ComputeTermKey(raw, topicField(i)))
	}
	return keys, nil
}

// topicField maps a topic position (0..MaxTopicCount-1) to its
// indexed Field. We index up to protocol.MaxTopicCount topic positions
// because that's the maximum a getEvents filter can match against;
// topics past that are not queryable, so indexing them would be
// unreachable storage.
//
// The switch arms below must cover [0, MaxTopicCount-1]. If
// MaxTopicCount changes (or a new Field is added to the enum) without
// updating this function, the panic fires loudly during tests instead
// of silently misrouting topic[N] into FieldTopic3's index slot.
func topicField(i int) Field {
	switch i {
	case 0:
		return FieldTopic0
	case 1:
		return FieldTopic1
	case 2:
		return FieldTopic2
	case 3:
		return FieldTopic3
	}
	panic(fmt.Sprintf("topicField: index %d out of range (MaxTopicCount=%d)", i, protocol.MaxTopicCount))
}
