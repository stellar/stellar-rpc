package query

// The getEvents v2 cursor envelope and its codec: the versioned, opaque,
// self-contained structure behind a response's cursor string. Only the server
// creates or decodes it; clients hold the encoded string and send it
// back unchanged. The codec checks well-formedness only; validating a query
// against the serving window and budgets is the handler's job.

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event"
)

// ErrCursorMalformed rejects a cursor string that does not decode to a
// well-formed envelope.
var ErrCursorMalformed = errors.New("query: malformed cursor")

// ErrCursorUnknownVersion rejects a cursor whose version byte this build does
// not implement. Distinct from ErrCursorMalformed so the handler can tell a
// corrupt cursor from one minted by a newer server.
var ErrCursorUnknownVersion = errors.New("query: unknown cursor version")

// cursorVersion is the envelope's format version. The byte sits outside the
// JSON so a future body format is a clean version bump.
const cursorVersion = 1

// maxCursorBytes caps the encoded string length DecodeEventCursor accepts,
// bounding decode work on hostile input. Encode enforces the same cap so an
// oversized envelope fails at mint rather than on the resume that follows.
// Sized above anything the request pipeline can produce: the serving stack
// caps request bodies at 512 KiB (internal/jsonrpc), and an envelope
// re-wraps roughly the request's filter content.
const maxCursorBytes = 1 << 20

// maxCursorFilters caps the filter count in both codec directions. It
// matches the getEvents v2 proposal's 256-filter request cap: no encoder
// mints more, so a cursor beyond it is forged or corrupt. Cursors never
// expire, so this constant can only grow; lowering it would reject
// outstanding cursors minted at the old cap.
const maxCursorFilters = 256

const contractIDLen = 32

// EventPosition is the ledger-denominated identity of a delivered event: the
// TOID fields (Ledger, Tx, Op) plus the event index within the operation, and
// K, the within-ledger index over all stored events of the ledger. K is
// portable across nodes only while ingest preserves the close meta's
// within-ledger stream order and the store's inclusion rule stays fixed.
type EventPosition struct {
	Ledger, Tx, Op, Event, K uint32
}

// EventCursorQuery is the canonical (engine) form of the original query the
// cursor pins: ledger bounds, direction, and the engine's filters.
type EventCursorQuery struct {
	MinLedger, MaxLedger uint32
	Dir                  Direction
	Filters              []event.Filter
}

// EventCursor is the envelope: the server-minted structure inside the opaque
// cursor string, holding the original query, the position of the last
// delivered event, and the scanned-ledger watermark. A nil Position means no
// event has been delivered yet, so resume starts from the watermark alone.
type EventCursor struct {
	Query         EventCursorQuery
	Position      *EventPosition
	ScannedLedger uint32
}

// The structs below define the envelope's JSON body. Their tags are the
// version-1 format: cursors never expire, so once v1 ships the key names are
// frozen and any change to them is a version bump. event.Filter carries no
// JSON tags and stays serialization-free; these conversions own the body
// shape instead. []byte fields serialize as standard base64 via
// encoding/json.

type envelopeJSON struct {
	Query    queryJSON     `json:"query"`
	Position *positionJSON `json:"position,omitempty"`
	Scanned  uint32        `json:"scanned"`
}

type queryJSON struct {
	Min     uint32       `json:"min"`
	Max     uint32       `json:"max"`
	Dir     string       `json:"dir"`
	Filters []filterJSON `json:"filters"`
}

// filterJSON is event.Filter in the body: "contract" is the raw 32-byte
// contract ID, omitted when unconstrained; "topics" is always MaxTopicCount
// entries with null for wildcard positions. A filter with no constraints at
// all is legal (match-all).
//
// Reserved key for the #904 filter fields: "type" (event type), omitempty,
// to be mapped when that PR merges since v2 filters carry it. #904's
// TopicCount is v1-only arity semantics that no envelope producer sets;
// when the coverage tripwire fires at the rebase, either map it for
// completeness (it needs two keys, "count" and "exact") or reject a set
// TopicCount at Encode. Decide then; do not drop it silently.
type filterJSON struct {
	Contract []byte                         `json:"contract,omitempty"`
	Topics   [protocol.MaxTopicCount][]byte `json:"topics"`
}

type positionJSON struct {
	Ledger uint32 `json:"ledger"`
	Tx     uint32 `json:"tx"`
	Op     uint32 `json:"op"`
	Event  uint32 `json:"event"`
	K      uint32 `json:"k"`
}

// Direction's serialized spelling. The Direction int values never
// serialize, so reordering the constants cannot silently break minted
// cursors.
const (
	directionAsc  = "asc"
	directionDesc = "desc"
)

// Encode serializes the envelope as base64url over (version byte ||
// JSON). Encode fails on an invalid Direction, a wrong-length ContractID,
// more than maxCursorFilters filters, or output beyond maxCursorBytes.
//
// Absent values must be nil: the wire format cannot tell nil from empty, so
// empty non-nil values decode back as nil (no exact round-trip). Inverted
// ledger bounds encode without error but fail the next decode.
func (e *EventCursor) Encode() (string, error) {
	dir, err := directionToJSON(e.Query.Dir)
	if err != nil {
		return "", err
	}
	if len(e.Query.Filters) > maxCursorFilters {
		return "", fmt.Errorf("query: encode cursor: %d filters exceeds the %d-filter cap",
			len(e.Query.Filters), maxCursorFilters)
	}
	w := envelopeJSON{
		Query: queryJSON{
			Min:     e.Query.MinLedger,
			Max:     e.Query.MaxLedger,
			Dir:     dir,
			Filters: make([]filterJSON, 0, len(e.Query.Filters)),
		},
		Scanned: e.ScannedLedger,
	}
	for i := range e.Query.Filters {
		f, err := filterToJSON(&e.Query.Filters[i])
		if err != nil {
			return "", fmt.Errorf("query: encode cursor filter %d: %w", i, err)
		}
		w.Query.Filters = append(w.Query.Filters, f)
	}
	if e.Position != nil {
		w.Position = &positionJSON{
			Ledger: e.Position.Ledger,
			Tx:     e.Position.Tx,
			Op:     e.Position.Op,
			Event:  e.Position.Event,
			K:      e.Position.K,
		}
	}
	body, err := json.Marshal(w)
	if err != nil {
		return "", fmt.Errorf("query: encode cursor: %w", err)
	}
	buf := make([]byte, 0, 1+len(body))
	buf = append(buf, cursorVersion)
	buf = append(buf, body...)
	enc := base64.RawURLEncoding.EncodeToString(buf)
	if len(enc) > maxCursorBytes {
		return "", fmt.Errorf("query: encode cursor: %d bytes exceeds the %d-byte decode cap",
			len(enc), maxCursorBytes)
	}
	return enc, nil
}

// DecodeEventCursor parses a cursor string into its envelope. Malformed
// input returns an error matching ErrCursorMalformed; a cursor whose version
// byte this build cannot decode returns one matching ErrCursorUnknownVersion.
func DecodeEventCursor(s string) (*EventCursor, error) {
	if s == "" {
		return nil, fmt.Errorf("%w: empty input", ErrCursorMalformed)
	}
	if len(s) > maxCursorBytes {
		return nil, fmt.Errorf("%w: %d bytes exceeds the %d-byte cap",
			ErrCursorMalformed, len(s), maxCursorBytes)
	}
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("%w: base64: %w", ErrCursorMalformed, err)
	}
	if len(raw) == 0 {
		return nil, fmt.Errorf("%w: missing version byte", ErrCursorMalformed)
	}
	if raw[0] != cursorVersion {
		return nil, fmt.Errorf("%w: %d", ErrCursorUnknownVersion, raw[0])
	}
	// Bound unmarshal allocation before parsing: every legal string value in
	// the body is base64 or a direction token, none of which contain '{', so
	// the brace count equals the object count (envelope + query + optional
	// position + one per filter). The post-unmarshal filter cap below stays
	// the authoritative check.
	if bytes.Count(raw[1:], []byte("{")) > maxCursorFilters+3 {
		return nil, fmt.Errorf("%w: object count exceeds the %d-filter cap",
			ErrCursorMalformed, maxCursorFilters)
	}
	var w envelopeJSON
	if err := json.Unmarshal(raw[1:], &w); err != nil {
		return nil, fmt.Errorf("%w: json: %w", ErrCursorMalformed, err)
	}

	dir, err := directionFromJSON(w.Query.Dir)
	if err != nil {
		return nil, err
	}
	if w.Query.Min > w.Query.Max {
		return nil, fmt.Errorf("%w: min ledger %d > max ledger %d",
			ErrCursorMalformed, w.Query.Min, w.Query.Max)
	}
	if len(w.Query.Filters) > maxCursorFilters {
		return nil, fmt.Errorf("%w: %d filters exceeds the %d-filter cap",
			ErrCursorMalformed, len(w.Query.Filters), maxCursorFilters)
	}
	env := &EventCursor{
		Query: EventCursorQuery{
			MinLedger: w.Query.Min,
			MaxLedger: w.Query.Max,
			Dir:       dir,
		},
		ScannedLedger: w.Scanned,
	}
	if len(w.Query.Filters) > 0 {
		env.Query.Filters = make([]event.Filter, len(w.Query.Filters))
		for i := range w.Query.Filters {
			f, err := filterFromJSON(&w.Query.Filters[i])
			if err != nil {
				return nil, fmt.Errorf("filter %d: %w", i, err)
			}
			env.Query.Filters[i] = f
		}
	}
	if w.Position != nil {
		env.Position = &EventPosition{
			Ledger: w.Position.Ledger,
			Tx:     w.Position.Tx,
			Op:     w.Position.Op,
			Event:  w.Position.Event,
			K:      w.Position.K,
		}
	}
	return env, nil
}

// filterToJSON and filterFromJSON normalize zero-length values to nil in
// both directions, keeping the absent-means-nil contract exact even for
// bodies a legitimate encoder never produces.

func filterToJSON(f *event.Filter) (filterJSON, error) {
	var j filterJSON
	if n := len(f.ContractID); n > 0 {
		if n != contractIDLen {
			return filterJSON{}, fmt.Errorf("contract ID is %d bytes, want %d", n, contractIDLen)
		}
		j.Contract = f.ContractID
	}
	for i, topic := range f.Topics {
		if len(topic) > 0 {
			j.Topics[i] = topic
		}
	}
	return j, nil
}

func filterFromJSON(j *filterJSON) (event.Filter, error) {
	var f event.Filter
	if n := len(j.Contract); n > 0 {
		if n != contractIDLen {
			return event.Filter{}, fmt.Errorf("%w: contract ID is %d bytes, want %d",
				ErrCursorMalformed, n, contractIDLen)
		}
		f.ContractID = j.Contract
	}
	for i, topic := range j.Topics {
		if len(topic) > 0 {
			f.Topics[i] = topic
		}
	}
	return f, nil
}

func directionToJSON(d Direction) (string, error) {
	switch d {
	case Ascending:
		return directionAsc, nil
	case Descending:
		return directionDesc, nil
	default:
		return "", fmt.Errorf("query: encode cursor: invalid direction %d", d)
	}
}

func directionFromJSON(s string) (Direction, error) {
	switch s {
	case directionAsc:
		return Ascending, nil
	case directionDesc:
		return Descending, nil
	default:
		return 0, fmt.Errorf("%w: dir %q", ErrCursorMalformed, s)
	}
}
