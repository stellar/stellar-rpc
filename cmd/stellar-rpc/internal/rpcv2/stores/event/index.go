package event

import (
	"encoding/binary"
	"fmt"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stellar/streamhash"
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
	FieldEventType  Field = 5
	FieldTopicCount Field = 6
)

// TermSchemaVersion names the term-derivation scheme: the hash function and
// byte encoding behind ComputeTermKey plus each field's value encoding. Bump
// it whenever any of those change. Adding a whole field sets a new bit in
// IndexedFieldMask instead, and is a storage-format change in its own right:
// per the design doc's versioning section it ships as a new events format id
// with a hot format bump. Both values are recorded in every index.pack's
// build stamp, and the release-1 reader accepts exactly its own pair (fails
// closed; per-id read sets arrive with the format-id grammar).
const TermSchemaVersion uint16 = 1

// allFields is the field registry, the single source of truth the mask and
// the golden tests derive from. Extend it in the same change that adds a
// Field constant.
//
//nolint:gochecknoglobals // immutable field registry, single source of truth
var allFields = []Field{
	FieldContractID, FieldTopic0, FieldTopic1, FieldTopic2, FieldTopic3,
	FieldEventType, FieldTopicCount,
}

// IndexedFieldMask is the set of indexed fields as a bitmask (bit i set means
// Field i is indexed), derived from allFields; the build stamp records it per
// artifact and the term-key golden tests iterate the same registry.
//
//nolint:gochecknoglobals // derived from the immutable field registry
var IndexedFieldMask = func() uint64 {
	var m uint64
	for _, f := range allFields {
		m |= 1 << f
	}
	return m
}()

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

// EventTypeTermKey returns the term key for eventType. Every event
// carries one, so a query for a rare type resolves through the index
// instead of scanning a chunk to find the few events that have it.
//
// The name stutters as event.EventTypeTermKey. It stays that way because
// every TermKey constructor here is named after the Field it hashes
// (FieldEventType here, FieldTopicCount -> TopicCountTermKey), and
// trimming to TypeTermKey drops the domain term: the value is an
// xdr.ContractEventType, called eventType on the wire.
//
//nolint:revive // named after FieldEventType; see the note above
func EventTypeTermKey(eventType xdr.ContractEventType) TermKey {
	var value [4]byte
	binary.BigEndian.PutUint32(value[:], uint32(eventType)) //nolint:gosec // enum keeps its own XDR width
	return ComputeTermKey(value[:], FieldEventType)
}

// topicCountOverflowBucket holds every event carrying more topics than
// a getEvents filter can name. Those events cannot be told apart by any
// filter, and closing the bucket space keeps the union covering
// "n or more" bounded.
const topicCountOverflowBucket = protocol.MaxTopicCount + 1

// The bucket space is written into cold chunks that are never rewritten,
// so MaxTopicCount fixes what every bucket in an existing artifact means.
// Growing it would silently redefine the overflow bucket: what was
// written as "more topics than MaxTopicCount" would be read back as an
// exact count. Fail the build instead, the way topicField panics when
// the constant outgrows the topic Fields.
const (
	_ uint = protocol.MaxTopicCount - 4
	_ uint = 4 - protocol.MaxTopicCount
)

// TopicCountTermKey returns the term key for the bucket holding events
// with n topics. Every event carries one, which is what makes topic
// arity answerable from the index: it is an absence property, and value
// terms can only say what an event has.
func TopicCountTermKey(n int) TermKey {
	// n is a slice length here and a validated filter field on the query
	// side, so it is never negative.
	bucket := min(n, topicCountOverflowBucket)
	return ComputeTermKey([]byte{byte(bucket)}, FieldTopicCount) //nolint:gosec // 0..overflow bucket
}

// TopicCountTermKeysAtLeast returns the buckets whose union holds every
// event with n topics or more. The overflow bucket closes the union, so
// an event carrying more topics than a filter can name is still in it.
func TopicCountTermKeysAtLeast(n int) []TermKey {
	low := min(n, topicCountOverflowBucket)
	keys := make([]TermKey, 0, topicCountOverflowBucket-low+1)
	for bucket := low; bucket <= topicCountOverflowBucket; bucket++ {
		keys = append(keys, TopicCountTermKey(bucket))
	}
	return keys
}

// maxTermsPerEvent is what TermsForBytes emits for an event carrying
// the most topics a filter can name: type, topic count, contract ID,
// and one term per topic position.
const maxTermsPerEvent = 3 + protocol.MaxTopicCount

// TermsForBytes returns the term keys for a marshaled ContractEvent,
// navigating the raw XDR via xdr.ContractEventView instead of a full
// UnmarshalBinary: its type, its topic count, its contract ID when it
// has one, and its topics 0..MaxTopicCount-1.
func TermsForBytes(eventBytes []byte) ([]TermKey, error) {
	ev := xdr.ContractEventView(eventBytes)
	keys := make([]TermKey, 0, maxTermsPerEvent)

	typeView, err := ev.Type()
	if err != nil {
		return nil, fmt.Errorf("events: view Type: %w", err)
	}
	// A type outside the enum is a hard indexing error, the same call the
	// unsupported body version below makes: the alternative is indexing
	// the event under a bucket no filter can name, which reads as "no
	// such events" rather than as a protocol the binary cannot serve.
	eventType, err := typeView.Value()
	if err != nil {
		return nil, fmt.Errorf("events: view Type value: %w", err)
	}
	keys = append(keys, EventTypeTermKey(eventType))

	cidOpt, err := ev.ContractId()
	if err != nil {
		return nil, fmt.Errorf("events: view ContractId: %w", err)
	}
	cidView, present, err := cidOpt.Unwrap()
	if err != nil {
		return nil, fmt.Errorf("events: view ContractId unwrap: %w", err)
	}
	if present {
		cid, err := cidView.Value()
		if err != nil {
			return nil, fmt.Errorf("events: view ContractId value: %w", err)
		}
		keys = append(keys, ComputeTermKey(cid[:], FieldContractID))
	}

	body, err := ev.Body()
	if err != nil {
		return nil, fmt.Errorf("events: view ContractEvent.Body: %w", err)
	}
	bodyVVal, err := body.V()
	if err != nil {
		return nil, fmt.Errorf("events: view Body.V: %w", err)
	}
	// Only Body discriminant V=0 carries topics. A future body version
	// is a hard error matching the SQLite backend (sqlitedb/event.go) — a
	// silently contractID-only index would make topic queries miss
	// real events with no signal.
	if bodyVVal != 0 {
		return nil, fmt.Errorf("events: unsupported ContractEvent body version %d", bodyVVal)
	}
	v0, err := body.V0()
	if err != nil {
		return nil, fmt.Errorf("events: view Body.V0: %w", err)
	}
	topics, err := v0.Topics()
	if err != nil {
		return nil, fmt.Errorf("events: view Body.V0.Topics: %w", err)
	}
	topicViews, err := topics.All()
	if err != nil {
		return nil, fmt.Errorf("events: view Body.V0.Topics.All: %w", err)
	}
	keys = append(keys, TopicCountTermKey(len(topicViews)))
	for i, topic := range topicViews {
		if i >= protocol.MaxTopicCount {
			break
		}
		// All returns each element trimmed to its exact size, so the
		// ScValView bytes are already the topic's raw XDR — hash them
		// directly rather than calling Raw() (which re-walks size).
		keys = append(keys, ComputeTermKey([]byte(topic), topicField(i)))
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
