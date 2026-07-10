// Package events defines the canonical binary format for a
// single event as stored in the full-history pipeline.
//
// The same bytes are produced by hot ingest, written into the
// per-Chunk `events_data_{C}` RocksDB column family, and later
// streamed unchanged into a cold `events.pack` record at freeze
// time. Backfill writers use the same format. Choosing one format
// here means no re-serialization happens anywhere downstream.
//
// Format (version 0x01):
//
//	offset  size  field
//	0       1     version            (0x01)
//	1       32    txHash             (xdr.Hash)
//	33      4     ledgerSequence     (uint32 BE)
//	37      4     txIdx              (uint32 BE)
//	41      4     opIdx              (uint32 BE; MaxUint32 = -1 sentinel)
//	45      8     ledgerClosedAt     (int64 BE, Unix seconds)
//	53      4     eventIdx           (uint32 BE)
//	57      4     contractEventLen N (uint32 BE)
//	61      N     contractEvent      (xdr.ContractEvent.MarshalBinary)
//
// eventIdx is the per-event index within its (ledger, txIdx, opIdx) cursor
// group: an operation event's position within its operation, and a V4 stage
// event's position within its stage group (BeforeAllTxs at (0, 0), AfterAllTxs
// at (TransactionMask, 0), AfterTx at (txIdx, OperationMask)). Together with
// (txIdx, opIdx) it forms the v1 getEvents event ID's <TOID>-<eventIdx>
// trailing component, which is part of the public API today — and it is
// assigned identically to the SQLite path (db/event.go), so the two backends
// return the same IDs.
//
// Because PayloadsFromLedgerEvents emits each ledger in ascending getEvents cursor
// order, every (txIdx, opIdx) group is contiguous in stream order, so eventIdx
// could be reconstructed positionally at read time. It is stored explicitly
// anyway: that keeps the public ID format byte-stable without a payload
// version bump and reingestion of already-frozen cold chunks if the cursor
// scheme is ever changed (the field can simply be ignored). See
// stellar/stellar-rpc#779.
//
// The leading version byte exists so that already-frozen Chunks remain
// readable when the metadata schema evolves. Unmarshal additionally requires a
// record's declared ContractEvent length to consume every remaining byte:
// records are stored and read back whole, so any slack means the bytes were
// not written by this layout, and the mismatch fails loudly rather than
// silently aliasing the wrong event slice.
package events

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// PayloadVersion is the current event payload format version.
const PayloadVersion byte = 0x01

const (
	versionLen        = 1
	txHashLen         = 32
	ledgerSeqLen      = 4
	txIdxLen          = 4
	opIdxLen          = 4
	ledgerClosedAtLen = 8
	eventIdxLen       = 4
	contractEventLen  = 4

	// headerLen is the size of the fixed-width prefix that precedes
	// the variable-length ContractEvent XDR bytes.
	headerLen = versionLen + txHashLen + ledgerSeqLen + txIdxLen + opIdxLen +
		ledgerClosedAtLen + eventIdxLen + contractEventLen
)

// ErrUnknownPayloadVersion is returned by Unmarshal when the leading
// version byte is not recognized by this binary.
var ErrUnknownPayloadVersion = errors.New("events: unknown format version")

// ErrShortPayloadBuffer is returned when the encoded payload is shorter
// than the header or its declared ContractEvent length.
var ErrShortPayloadBuffer = errors.New("events: buffer too short")

// ErrPayloadLengthMismatch is returned when a record's declared ContractEvent
// length does not account for every remaining byte. Records are stored and
// read back whole (one RocksDB value / one packfile item per payload), so any
// slack means the record was not written by this layout (e.g. a header of a
// different width), and is rejected instead of silently shifting the event
// bytes.
var ErrPayloadLengthMismatch = errors.New("events: payload length mismatch")

// Payload is the in-memory form of one stored event. Every field is
// material — query results, indexing, and cursor encoding all read
// from it. Storing LedgerSequence alongside the per-event metadata
// keeps the reader path self-contained: no inverse-lookup against
// events_offsets at fetch time.
type Payload struct {
	TxHash         xdr.Hash
	LedgerSequence uint32
	TxIdx          uint32
	OpIdx          uint32
	LedgerClosedAt int64
	// EventIdx is the per-event index within this event's (ledger, txIdx,
	// opIdx) cursor group — the trailing <TOID>-<eventIdx> component of the
	// v1 getEvents event ID. Assigned identically to the SQLite path
	// (db/event.go) so both backends produce the same IDs.
	EventIdx uint32
	// ContractEventBytes is the raw ContractEvent XDR
	// (xdr.ContractEvent.MarshalBinary output).
	ContractEventBytes []byte
}

// Marshal returns the canonical wire representation of p in a freshly
// allocated buffer the caller owns.
func (p *Payload) Marshal() ([]byte, error) {
	return p.MarshalInto(nil)
}

// MarshalInto writes the canonical wire representation of p into dst,
// reusing dst's capacity when it is large enough, and returns the
// result. Callers that marshal many payloads pass one reused buffer to
// avoid a per-payload allocation; the returned slice aliases dst and is
// valid only until the next call reusing it. A reused dst is
// single-owner: do not share one buffer across goroutines. Marshal is
// the owned-buffer variant (dst == nil).
func (p *Payload) MarshalInto(dst []byte) ([]byte, error) {
	if len(p.ContractEventBytes) == 0 {
		return nil, errors.New("events: Payload has no ContractEventBytes to marshal")
	}
	eventBytes := p.ContractEventBytes

	need := headerLen + len(eventBytes)
	buf := dst[:0]
	if cap(buf) < need {
		buf = make([]byte, need)
	} else {
		buf = buf[:need]
	}
	off := 0
	buf[off] = PayloadVersion
	off += versionLen
	copy(buf[off:off+txHashLen], p.TxHash[:])
	off += txHashLen
	binary.BigEndian.PutUint32(buf[off:], p.LedgerSequence)
	off += ledgerSeqLen
	binary.BigEndian.PutUint32(buf[off:], p.TxIdx)
	off += txIdxLen
	binary.BigEndian.PutUint32(buf[off:], p.OpIdx)
	off += opIdxLen
	binary.BigEndian.PutUint64(buf[off:], uint64(p.LedgerClosedAt)) //nolint:gosec // ledger close-time fits in int64
	off += ledgerClosedAtLen
	binary.BigEndian.PutUint32(buf[off:], p.EventIdx)
	off += eventIdxLen
	binary.BigEndian.PutUint32(buf[off:], uint32(len(eventBytes))) //nolint:gosec // event size bounded by protocol limits
	off += contractEventLen
	copy(buf[off:], eventBytes)
	return buf, nil
}

// Unmarshal parses a wire-form payload into p: it reads the fixed-width
// header into p's scalar fields and aliases the raw ContractEvent XDR
// bytes into p.ContractEventBytes (no copy, no XDR decode). It rejects any
// unknown version byte (returning ErrUnknownPayloadVersion) so older
// binaries fail loudly rather than silently misinterpreting newer records.
//
// IMPORTANT — buffer-lifetime contract: the returned ContractEventBytes
// slice ALIASES into data and is valid only as long as data is. The
// eventstore read paths apply this two ways:
//
//   - FetchEvents passes data that outlives the returned slice — hot from
//     rocksdb.BatchMultiGet (freshly allocated, caller-owned), cold by
//     cloning the borrowed packfile.ReadItems buffer — so its Payloads are
//     safe to retain.
//   - FetchRange / All pass the iterator's borrowed buffer directly
//     (rocksdb.IterateRange / packfile.ReadRange, valid only for the
//     current step), so each yielded Payload is borrowed; a consumer that
//     retains one past the step must clone its ContractEventBytes.
func (p *Payload) Unmarshal(data []byte) error {
	eventBytes, err := p.unmarshalHeader(data)
	if err != nil {
		return err
	}
	p.ContractEventBytes = eventBytes
	return nil
}

// unmarshalHeader parses the fixed-width header out of data into p's
// scalar fields and returns the raw ContractEvent XDR bytes (a slice
// into data).
func (p *Payload) unmarshalHeader(data []byte) ([]byte, error) {
	if len(data) < versionLen {
		return nil, ErrShortPayloadBuffer
	}
	if data[0] != PayloadVersion {
		return nil, fmt.Errorf("%w: 0x%02x", ErrUnknownPayloadVersion, data[0])
	}
	if len(data) < headerLen {
		return nil, ErrShortPayloadBuffer
	}

	off := versionLen
	copy(p.TxHash[:], data[off:off+txHashLen])
	off += txHashLen
	p.LedgerSequence = binary.BigEndian.Uint32(data[off:])
	off += ledgerSeqLen
	p.TxIdx = binary.BigEndian.Uint32(data[off:])
	off += txIdxLen
	p.OpIdx = binary.BigEndian.Uint32(data[off:])
	off += opIdxLen
	p.LedgerClosedAt = int64(binary.BigEndian.Uint64(data[off:])) //nolint:gosec // ledger close-time fits in int64
	off += ledgerClosedAtLen
	p.EventIdx = binary.BigEndian.Uint32(data[off:])
	off += eventIdxLen
	eventLen := binary.BigEndian.Uint32(data[off:])
	off += contractEventLen

	if uint64(len(data)-off) < uint64(eventLen) { //nolint:gosec // len-int diff is non-negative; bounded above by len
		return nil, ErrShortPayloadBuffer
	}
	// Records are read back whole, so the declared length must consume every
	// remaining byte (see the package doc and ErrPayloadLengthMismatch).
	if uint64(len(data)-off) != uint64(eventLen) { //nolint:gosec // same bounds as above
		return nil, fmt.Errorf("%w: %d trailing bytes", ErrPayloadLengthMismatch, len(data)-off-int(eventLen))
	}
	return data[off : off+int(eventLen)], nil
}
