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
// The leading version byte exists so that already-frozen Chunks
// remain readable when the metadata schema evolves.
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

// Payload is the in-memory form of one stored event. Every field is
// material — query results, indexing, and cursor encoding all read
// from it. Storing LedgerSequence alongside the per-event metadata
// keeps the reader path self-contained: no inverse-lookup against
// events_offsets at fetch time.
//
// Three population modes:
//
//  1. Producer / struct path (LCMToPayloads + producers that hand-build
//     Payloads from decoded LCM data): ContractEvent populated;
//     ContractEventBytes and Terms left nil. Marshal calls
//     ContractEvent.MarshalBinary().
//  2. Producer / view path (LCMToPayloadsFromRaw): ContractEventBytes
//     and Terms populated, ContractEvent left zero. Marshal uses
//     ContractEventBytes verbatim. HotStore ingest reads Terms directly
//     instead of re-deriving via TermsFor.
//  3. Consumer / view path (UnmarshalView, used by the read path when
//     a Reader implementation is opened with its view-mode option —
//     HotStore.WithXDRViews / ColdReaderOptions.UseXDRViews):
//     ContractEventBytes populated, ContractEvent zeroed, Terms
//     cleared. Downstream consumers (postFilter, RPC serializer)
//     must read off ContractEventBytes via xdr.ContractEventView.
//     See UnmarshalView for the buffer-lifetime contract.
//
// Marshal prefers ContractEventBytes when set, falling back to
// ContractEvent.MarshalBinary() otherwise. So a Payload produced by
// either view-based path round-trips through Marshal unchanged. A
// Payload from mode (1) round-trips via MarshalBinary as before.
//
// Unmarshal (consumer / struct path) populates ContractEvent and
// explicitly clears ContractEventBytes + Terms so a re-Unmarshalled
// Payload doesn't leak stale view-bytes from a prior decode into a
// subsequent Marshal.
type Payload struct {
	TxHash         xdr.Hash
	LedgerSequence uint32
	TxIdx          uint32
	OpIdx          uint32
	LedgerClosedAt int64
	EventIdx       uint32
	ContractEvent  xdr.ContractEvent
	// ContractEventBytes is the raw ContractEvent XDR. Set by view-based
	// producers OR by UnmarshalView on the consumer side. Optional.
	ContractEventBytes []byte
	// Terms is the precomputed []TermKey for this event. Set by
	// view-based producers only. Optional.
	Terms []TermKey
}

// Marshal returns the canonical wire representation of p.
func (p *Payload) Marshal() ([]byte, error) {
	eventBytes := p.ContractEventBytes
	if eventBytes == nil {
		var err error
		eventBytes, err = p.ContractEvent.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("events: marshal contract event: %w", err)
		}
	}

	buf := make([]byte, headerLen+len(eventBytes))
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

// Unmarshal parses a wire-form payload into p. It rejects any
// unknown version byte (returning ErrUnknownPayloadVersion) so that older
// binaries fail loudly rather than silently misinterpreting newer
// records.
func (p *Payload) Unmarshal(data []byte) error {
	eventBytes, err := p.unmarshalHeader(data)
	if err != nil {
		return err
	}
	p.ContractEvent = xdr.ContractEvent{}
	if err := p.ContractEvent.UnmarshalBinary(eventBytes); err != nil {
		return fmt.Errorf("events: unmarshal contract event: %w", err)
	}
	// Clear the producer-side caches so a reused Payload doesn't carry
	// stale view-bytes / pre-derived term keys from a prior decode.
	// Without this, Marshal on a re-Unmarshalled Payload would emit the
	// PREVIOUS payload's ContractEvent bytes (Marshal prefers
	// ContractEventBytes over ContractEvent when both are set).
	p.ContractEventBytes = nil
	p.Terms = nil
	return nil
}

// UnmarshalView is the view-based counterpart to Unmarshal: it parses
// the fixed-width header into p's scalar fields and aliases the raw
// ContractEvent XDR bytes into p.ContractEventBytes without calling
// ContractEvent.UnmarshalBinary. p.ContractEvent stays zero-valued.
//
// IMPORTANT — buffer-lifetime contract: the returned ContractEventBytes
// slice ALIASES into data. The caller MUST guarantee that data outlives
// every consumer of p.ContractEventBytes. Among the eventstore read
// paths:
//
//   - HotStore.FetchEvents (rocksdb.BatchMultiGet): values are freshly
//     allocated and caller-owned. Safe to pass directly.
//   - HotStore.FetchRange (rocksdb.IterateRange), ColdReader.FetchEvents
//     (packfile.ReadItems), ColdReader.FetchRange (packfile.ReadRange):
//     yielded buffers are zero-copy refs that are invalidated on the
//     next iteration step (or on fn-return, for ReadItems). Callers in
//     these paths MUST bytes.Clone(data) before passing in, otherwise
//     post-iteration consumers (e.g. postFilter accumulating into a
//     []Payload) observe garbage.
//
// Used by the read path when configured for view-based consumption
// (eventstore.Reader's UseXDRViews option); downstream callers must
// work off p.ContractEventBytes rather than p.ContractEvent. Symmetric
// to LCMToPayloadsFromRaw on the ingest side: zero MarshalBinary /
// UnmarshalBinary on the path.
func (p *Payload) UnmarshalView(data []byte) error {
	eventBytes, err := p.unmarshalHeader(data)
	if err != nil {
		return err
	}
	p.ContractEvent = xdr.ContractEvent{}
	p.ContractEventBytes = eventBytes
	p.Terms = nil
	return nil
}

// unmarshalHeader parses the fixed-width header out of data into p's
// scalar fields and returns the raw ContractEvent XDR bytes (a slice
// into data). Shared by Unmarshal and UnmarshalView; the two diverge
// only on whether to UnmarshalBinary the returned bytes into
// ContractEvent or alias them into ContractEventBytes.
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
	return data[off : off+int(eventLen)], nil
}
