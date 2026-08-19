package query

// The getEvents v2 cursor envelope and its codec: the versioned, opaque,
// self-contained structure behind a response's cursor string. Only the server
// creates or decodes it; clients hold the encoded string and send it
// back unchanged. The codec checks well-formedness only; validating a query
// against the serving window and budgets is the handler's job.
//
// A token is "gec" (getEvents cursor), the format version in cleartext
// digits, "_", then the base64url body without padding. The cleartext
// version makes version skew visible in logs without decoding. Cursors
// never expire, so every version's decoder stays supported forever.

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event"
)

// ErrCursorMalformed rejects a cursor string that does not decode to a
// well-formed envelope.
var ErrCursorMalformed = errors.New("query: malformed cursor")

// ErrCursorUnknownVersion rejects a cursor whose version this build does
// not implement. Distinct from ErrCursorMalformed so the handler can tell a
// corrupt cursor from one minted by a newer server.
var ErrCursorUnknownVersion = errors.New("query: unknown cursor version")

const cursorPrefixV1 = "gec1_"

// maxCursorBytes caps the token length DecodeEventCursor accepts. Encode
// enforces the same cap so an oversized envelope fails at mint rather than
// on the resume that follows. Sized above anything the request pipeline can
// produce: the serving stack caps request bodies at 512 KiB
// (internal/jsonrpc), and an envelope re-wraps roughly the request's filter
// content.
const maxCursorBytes = 1 << 20

// maxCursorFilters caps the filter count in both codec directions. It
// matches the getEvents v2 proposal's 256-filter request cap: no encoder
// mints more, so a cursor beyond it is forged or corrupt. Cursors never
// expire, so this constant can only grow; lowering it would reject
// outstanding cursors minted at the old cap.
const maxCursorFilters = 256

const contractIDLen = 32

// Version-1 body layout, big-endian, in field order: flags u8, minLedger
// u32, maxLedger u32 if flagHasMax, position 5xu32 (ledger, tx, op, event,
// ledgerOrdinal) if flagHasPos, scannedLedger u32, filter count u16, then per filter:
// fflags u8, event type u8 if fflagType, topic count u8 if fflagCount
// (fflagExact marks the count exact and requires fflagCount), contract 32
// bytes if fflagContract, and per set topic bit a nonzero u32 length plus
// that many bytes. Reserved envelope flag bits must be zero; every fflags
// bit is assigned, so the next filter field takes a version bump.
// One envelope has one encoding: decode consumes the body exactly and
// re-encodes byte-identically.
const (
	flagDescending = 1 << 0
	flagHasMax     = 1 << 1
	flagHasPos     = 1 << 2
	flagsKnown     = byte(flagDescending | flagHasMax | flagHasPos)

	fflagContract = 1 << 0
	fflagType     = 1 << 5
	fflagCount    = 1 << 6
	fflagExact    = 1 << 7
)

func topicBit(i int) byte { return 1 << (1 + i) } //nolint:mnd // bit 0 is the contract

// EventPosition is the ledger-denominated identity of a delivered event: the
// TOID fields (Ledger, Tx, Op) plus the event index within the operation, and
// LedgerOrdinal, the index over all stored events of the ledger. The latter
// is portable across nodes only while ingest preserves the close meta's
// within-ledger stream order and the store's inclusion rule stays fixed. The
// pager (#774) does resume arithmetic and self-validation against these
// fields.
type EventPosition struct {
	Ledger, Tx, Op, Event, LedgerOrdinal uint32
}

// EventCursorQuery is the canonical (engine) form of the original query the
// cursor pins: ledger bounds, direction, and the engine's filters. A nil
// MaxLedger is the proposal's open upper bound: an ascending query that
// follows the chain tip across pages. Descending queries always carry one,
// pinned at query start.
type EventCursorQuery struct {
	MinLedger uint32
	MaxLedger *uint32
	Dir       Direction
	Filters   []event.Filter
}

// EventCursor is the envelope: the server-minted structure inside the opaque
// cursor string, holding the original query, the position of the last
// delivered event, and the scanned-ledger watermark. A nil Position means no
// event has been delivered yet, so resume starts from the watermark alone.
type EventCursor struct {
	Scope         EventCursorQuery
	Position      *EventPosition
	ScannedLedger uint32
}

// Encode serializes the envelope as a version-1 token. Encode fails on what
// no decode accepts: an invalid Direction, a descending query without
// MaxLedger, MinLedger above a present MaxLedger, a wrong-length ContractID,
// more than maxCursorFilters filters, or output beyond maxCursorBytes — a
// minting bug fails on the page that has it rather than on the resume that
// follows.
//
// Absent values must be nil: a zero-length non-nil ContractID or topic
// encodes as absent and decodes back as nil (no exact round-trip for them).
func (e *EventCursor) Encode() (string, error) {
	var flags byte
	switch e.Scope.Dir {
	case Ascending:
	case Descending:
		flags |= flagDescending
	default:
		return "", fmt.Errorf("query: encode cursor: invalid direction %d", e.Scope.Dir)
	}
	if e.Scope.MaxLedger != nil {
		flags |= flagHasMax
		if e.Scope.MinLedger > *e.Scope.MaxLedger {
			return "", fmt.Errorf("query: encode cursor: min ledger %d > max ledger %d",
				e.Scope.MinLedger, *e.Scope.MaxLedger)
		}
	} else if e.Scope.Dir == Descending {
		return "", errors.New("query: encode cursor: descending without a max ledger")
	}
	if e.Position != nil {
		flags |= flagHasPos
	}
	if len(e.Scope.Filters) > maxCursorFilters {
		return "", fmt.Errorf("query: encode cursor: %d filters exceeds the %d-filter cap",
			len(e.Scope.Filters), maxCursorFilters)
	}

	body := []byte{flags}
	body = binary.BigEndian.AppendUint32(body, e.Scope.MinLedger)
	if e.Scope.MaxLedger != nil {
		body = binary.BigEndian.AppendUint32(body, *e.Scope.MaxLedger)
	}
	if e.Position != nil {
		for _, v := range [...]uint32{
			e.Position.Ledger, e.Position.Tx, e.Position.Op, e.Position.Event, e.Position.LedgerOrdinal,
		} {
			body = binary.BigEndian.AppendUint32(body, v)
		}
	}
	body = binary.BigEndian.AppendUint32(body, e.ScannedLedger)
	count := uint16(len(e.Scope.Filters)) //nolint:gosec // capped at maxCursorFilters above
	body = binary.BigEndian.AppendUint16(body, count)
	for i := range e.Scope.Filters {
		var err error
		if body, err = appendFilter(body, &e.Scope.Filters[i]); err != nil {
			return "", fmt.Errorf("query: encode cursor filter %d: %w", i, err)
		}
	}

	enc := cursorPrefixV1 + base64.RawURLEncoding.EncodeToString(body)
	if len(enc) > maxCursorBytes {
		return "", fmt.Errorf("query: encode cursor: %d bytes exceeds the %d-byte decode cap",
			len(enc), maxCursorBytes)
	}
	return enc, nil
}

// filterFlags validates f's fields and computes its fflags byte.
func filterFlags(f *event.Filter) (byte, error) {
	var fflags byte
	if n := len(f.ContractID); n > 0 {
		if n != contractIDLen {
			return 0, fmt.Errorf("contract ID is %d bytes, want %d", n, contractIDLen)
		}
		fflags |= fflagContract
	}
	if f.EventType != nil {
		if !f.EventType.ValidEnum(int32(*f.EventType)) {
			return 0, fmt.Errorf("event type %d is not a known event type", *f.EventType)
		}
		fflags |= fflagType
	}
	if f.TopicCount != (event.TopicCountFilter{}) {
		if f.TopicCount.Count < 0 || f.TopicCount.Count > protocol.MaxTopicCount {
			return 0, fmt.Errorf("topic count %d outside [0, %d]",
				f.TopicCount.Count, protocol.MaxTopicCount)
		}
		fflags |= fflagCount
		if f.TopicCount.Exact {
			fflags |= fflagExact
		}
	}
	for i := range f.Topics {
		if len(f.Topics[i]) > 0 {
			fflags |= topicBit(i)
		}
	}
	return fflags, nil
}

func appendFilter(body []byte, f *event.Filter) ([]byte, error) {
	fflags, err := filterFlags(f)
	if err != nil {
		return nil, err
	}
	body = append(body, fflags)
	if fflags&fflagType != 0 {
		body = append(body, byte(*f.EventType)) //nolint:gosec // ValidEnum above bounds it
	}
	if fflags&fflagCount != 0 {
		body = append(body, byte(f.TopicCount.Count)) //nolint:gosec // bounded above
	}
	if fflags&fflagContract != 0 {
		body = append(body, f.ContractID...)
	}
	for i := range f.Topics {
		if topic := f.Topics[i]; len(topic) > 0 {
			if uint64(len(topic)) > math.MaxUint32 {
				return nil, fmt.Errorf("topic %d is %d bytes, beyond the u32 length field", i, len(topic))
			}
			body = binary.BigEndian.AppendUint32(body, uint32(len(topic))) //nolint:gosec // bounded above
			body = append(body, topic...)
		}
	}
	return body, nil
}

// DecodeEventCursor parses a cursor token into its envelope. Malformed
// input returns an error matching ErrCursorMalformed; a token whose version
// this build cannot decode returns one matching ErrCursorUnknownVersion.
func DecodeEventCursor(s string) (*EventCursor, error) {
	if s == "" {
		return nil, fmt.Errorf("%w: empty input", ErrCursorMalformed)
	}
	if len(s) > maxCursorBytes {
		return nil, fmt.Errorf("%w: %d bytes exceeds the %d-byte cap",
			ErrCursorMalformed, len(s), maxCursorBytes)
	}
	payload, err := cursorPayloadV1(s)
	if err != nil {
		return nil, err
	}
	// The base64 decoder silently skips CR and LF; reject them so one token
	// means one body.
	if strings.ContainsAny(payload, "\r\n") {
		return nil, fmt.Errorf("%w: whitespace in payload", ErrCursorMalformed)
	}
	body, err := base64.RawURLEncoding.Strict().DecodeString(payload)
	if err != nil {
		return nil, fmt.Errorf("%w: base64: %w", ErrCursorMalformed, err)
	}
	r := cursorReader{body: body}
	env, err := readEnvelope(&r)
	if err != nil {
		return nil, err
	}
	if rest := len(r.body) - r.off; rest != 0 {
		return nil, fmt.Errorf("%w: %d trailing bytes", ErrCursorMalformed, rest)
	}
	return env, nil
}

func cursorPayloadV1(s string) (string, error) {
	rest, ok := strings.CutPrefix(s, "gec")
	if !ok {
		return "", fmt.Errorf("%w: not a gec token", ErrCursorMalformed)
	}
	sep := strings.IndexByte(rest, '_')
	if sep <= 0 {
		return "", fmt.Errorf("%w: missing version", ErrCursorMalformed)
	}
	version := rest[:sep]
	for i := range len(version) {
		if version[i] < '0' || version[i] > '9' {
			return "", fmt.Errorf("%w: version %q", ErrCursorMalformed, version)
		}
	}
	if version != "1" {
		return "", fmt.Errorf("%w: %s", ErrCursorUnknownVersion, version)
	}
	return rest[sep+1:], nil
}

// cursorReader walks the fixed-layout body. Every read checks the remaining
// length first, so decode work and allocation are bounded by input length.
// Byte slices alias the decode buffer, which is owned by the returned
// envelope.
type cursorReader struct {
	body []byte
	off  int
}

var errTruncated = fmt.Errorf("%w: truncated body", ErrCursorMalformed)

func (r *cursorReader) take(n int) ([]byte, error) {
	if len(r.body)-r.off < n {
		return nil, errTruncated
	}
	b := r.body[r.off : r.off+n]
	r.off += n
	return b, nil
}

func (r *cursorReader) u8() (byte, error) {
	b, err := r.take(1)
	if err != nil {
		return 0, err
	}
	return b[0], nil
}

func (r *cursorReader) u16() (uint16, error) {
	b, err := r.take(2)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(b), nil
}

func (r *cursorReader) u32() (uint32, error) {
	b, err := r.take(4)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(b), nil
}

func readEnvelope(r *cursorReader) (*EventCursor, error) {
	flags, err := r.u8()
	if err != nil {
		return nil, err
	}
	if flags&^flagsKnown != 0 {
		return nil, fmt.Errorf("%w: reserved flag bits in %#02x", ErrCursorMalformed, flags)
	}
	env := &EventCursor{}
	if flags&flagDescending != 0 {
		env.Scope.Dir = Descending
	}
	if env.Scope.MinLedger, err = r.u32(); err != nil {
		return nil, err
	}
	switch {
	case flags&flagHasMax != 0:
		maxLedger, err := r.u32()
		if err != nil {
			return nil, err
		}
		if env.Scope.MinLedger > maxLedger {
			return nil, fmt.Errorf("%w: min ledger %d > max ledger %d",
				ErrCursorMalformed, env.Scope.MinLedger, maxLedger)
		}
		env.Scope.MaxLedger = &maxLedger
	case flags&flagDescending != 0:
		return nil, fmt.Errorf("%w: descending without a max ledger", ErrCursorMalformed)
	}
	if flags&flagHasPos != 0 {
		var p EventPosition
		for _, dst := range [...]*uint32{&p.Ledger, &p.Tx, &p.Op, &p.Event, &p.LedgerOrdinal} {
			if *dst, err = r.u32(); err != nil {
				return nil, err
			}
		}
		env.Position = &p
	}
	if env.ScannedLedger, err = r.u32(); err != nil {
		return nil, err
	}
	if env.Scope.Filters, err = readFilters(r); err != nil {
		return nil, err
	}
	return env, nil
}

func readFilters(r *cursorReader) ([]event.Filter, error) {
	count, err := r.u16()
	if err != nil {
		return nil, err
	}
	if int(count) > maxCursorFilters {
		return nil, fmt.Errorf("%w: %d filters exceeds the %d-filter cap",
			ErrCursorMalformed, count, maxCursorFilters)
	}
	if count == 0 {
		return nil, nil
	}
	filters := make([]event.Filter, count)
	for i := range filters {
		if err := readFilter(r, &filters[i]); err != nil {
			return nil, fmt.Errorf("filter %d: %w", i, err)
		}
	}
	return filters, nil
}

// readFilterTypeCount decodes the event-type and topic-count bytes
// fflags declares, validating the values and the flag combinations.
func readFilterTypeCount(r *cursorReader, fflags byte, f *event.Filter) error {
	if fflags&fflagExact != 0 && fflags&fflagCount == 0 {
		return fmt.Errorf("%w: exact bit without a topic count", ErrCursorMalformed)
	}
	if fflags&fflagType != 0 {
		b, err := r.u8()
		if err != nil {
			return err
		}
		eventType := xdr.ContractEventType(b)
		if !eventType.ValidEnum(int32(b)) {
			return fmt.Errorf("%w: event type %d", ErrCursorMalformed, b)
		}
		f.EventType = &eventType
	}
	if fflags&fflagCount != 0 {
		b, err := r.u8()
		if err != nil {
			return err
		}
		if int(b) > protocol.MaxTopicCount {
			return fmt.Errorf("%w: topic count %d above %d",
				ErrCursorMalformed, b, protocol.MaxTopicCount)
		}
		exact := fflags&fflagExact != 0
		if b == 0 && !exact {
			// "At least zero" is the wildcard, which encodes as absent.
			return fmt.Errorf("%w: topic count 0 without the exact bit", ErrCursorMalformed)
		}
		f.TopicCount = event.TopicCountFilter{Count: int(b), Exact: exact}
	}
	return nil
}

func readFilter(r *cursorReader, f *event.Filter) error {
	fflags, err := r.u8()
	if err != nil {
		return err
	}
	if err := readFilterTypeCount(r, fflags, f); err != nil {
		return err
	}
	if fflags&fflagContract != 0 {
		if f.ContractID, err = r.take(contractIDLen); err != nil {
			return err
		}
	}
	for i := range f.Topics {
		if fflags&topicBit(i) == 0 {
			continue
		}
		n, err := r.u32()
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("%w: topic %d present with zero length", ErrCursorMalformed, i)
		}
		if uint64(n) > uint64(len(r.body)-r.off) { //nolint:gosec // off never exceeds len(body)
			return errTruncated
		}
		if f.Topics[i], err = r.take(int(n)); err != nil {
			return err
		}
	}
	return nil
}
