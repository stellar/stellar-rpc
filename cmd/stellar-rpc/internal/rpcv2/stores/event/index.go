package event

import (
	"encoding/binary"
	"fmt"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

// Term is an indexed (field, value) pair as bytes: the field byte, then the value.
type Term string

// TermKey is a term keyed with its chunk's secret (Term.Key).
type TermKey [16]byte

func NewTerm(field Field, value []byte) Term {
	return Term(append([]byte{byte(field)}, value...))
}

func (t Term) Key(secret [stores.SecretLen]byte) TermKey {
	var scratch [128]byte
	return TermKey(stores.BlindKey(secret, append(scratch[:0], t...)))
}

func keysUnder(secret [stores.SecretLen]byte, terms []Term) []TermKey {
	keys := make([]TermKey, len(terms))
	for i, t := range terms {
		keys[i] = t.Key(secret)
	}
	return keys
}

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
// byte encoding behind Term.Key plus each field's value encoding. Bump
// it whenever any of those change. Adding a whole field sets a new bit in
// IndexedFieldMask instead and is a storage-format change in its own right;
// see the design doc's "Format versioning and upgrades" section. Both values
// are recorded in every index.pack's build stamp, and the reader accepts
// exactly its own pair.
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

// indexedFieldMask is derived from allFields once at init. It is unexported so
// no importer can reassign the value every index.pack build stamp records.
//
//nolint:gochecknoglobals // derived from the immutable field registry
var indexedFieldMask = func() uint64 {
	var m uint64
	for _, f := range allFields {
		m |= 1 << f
	}
	return m
}()

// IndexedFieldMask is the set of indexed fields as a bitmask (bit i set means
// Field i is indexed). The build stamp records it per artifact and the
// term-key golden tests iterate the same registry.
func IndexedFieldMask() uint64 { return indexedFieldMask }

// EventTypeTerm returns the term for eventType. Every event carries one,
// so a query for a rare type resolves through the index instead of
// scanning a chunk to find the few events that have it.
//
//nolint:revive // named after FieldEventType
func EventTypeTerm(eventType xdr.ContractEventType) Term {
	value := eventTypeValue(eventType)
	return NewTerm(FieldEventType, value[:])
}

func eventTypeValue(eventType xdr.ContractEventType) [4]byte {
	var value [4]byte
	binary.BigEndian.PutUint32(value[:], uint32(eventType)) //nolint:gosec // enum keeps its own XDR width
	return value
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

// TopicCountTerm returns the term for the bucket holding events with n
// topics. Every event carries one, which is what makes topic arity
// answerable from the index: it is an absence property, and value terms
// can only say what an event has.
func TopicCountTerm(n int) Term {
	return NewTerm(FieldTopicCount, []byte{topicCountBucket(n)})
}

// topicCountBucket is n's bucket; n is a slice length or a validated count, never negative.
func topicCountBucket(n int) byte {
	return byte(min(n, topicCountOverflowBucket)) //nolint:gosec // 0..overflow bucket
}

// TopicCountTermsAtLeast returns the buckets whose union holds every
// event with n topics or more. The overflow bucket closes the union, so
// an event carrying more topics than a filter can name is still in it.
func TopicCountTermsAtLeast(n int) []Term {
	low := int(topicCountBucket(n))
	terms := make([]Term, 0, topicCountOverflowBucket-low+1)
	for bucket := low; bucket <= topicCountOverflowBucket; bucket++ {
		terms = append(terms, TopicCountTerm(bucket))
	}
	return terms
}

// maxTermsPerEvent is what AppendEvent adds for an event carrying the
// most topics a filter can name: type, topic count, contract ID, and one
// term per topic position.
const maxTermsPerEvent = 3 + protocol.MaxTopicCount

// Terms holds terms back to back, so deriving them allocates nothing per term.
type Terms struct {
	buf  []byte
	ends []uint32 // term i is buf[ends[i-1]:ends[i]]
}

func (t *Terms) Reset() { t.buf, t.ends = t.buf[:0], t.ends[:0] }

func (t *Terms) Len() int { return len(t.ends) }

func (t *Terms) Key(i int, secret [stores.SecretLen]byte) TermKey {
	return TermKey(stores.BlindKey(secret, t.bytes(i)))
}

func TermsForBytes(eventBytes []byte) ([]Term, error) {
	var t Terms
	if err := t.AppendEvent(eventBytes); err != nil {
		return nil, err
	}
	terms := make([]Term, t.Len())
	for i := range terms {
		terms[i] = Term(t.bytes(i))
	}
	return terms, nil
}

// AppendEvent appends a marshaled event's terms from its XDR view: type,
// contract ID if any, topic count, then topics 0..MaxTopicCount-1.
func (t *Terms) AppendEvent(eventBytes []byte) error {
	ev := xdr.ContractEventView(eventBytes)

	typeView, err := ev.Type()
	if err != nil {
		return fmt.Errorf("events: view Type: %w", err)
	}
	// A type outside the enum is a hard indexing error, the same call the
	// unsupported body version below makes: the alternative is indexing
	// the event under a bucket no filter can name, which reads as "no
	// such events" rather than as a protocol the binary cannot serve.
	eventType, err := typeView.Value()
	if err != nil {
		return fmt.Errorf("events: view Type value: %w", err)
	}
	typeValue := eventTypeValue(eventType)
	t.add(FieldEventType, typeValue[:])

	cidOpt, err := ev.ContractId()
	if err != nil {
		return fmt.Errorf("events: view ContractId: %w", err)
	}
	cidView, present, err := cidOpt.Unwrap()
	if err != nil {
		return fmt.Errorf("events: view ContractId unwrap: %w", err)
	}
	if present {
		cid, err := cidView.Value()
		if err != nil {
			return fmt.Errorf("events: view ContractId value: %w", err)
		}
		t.add(FieldContractID, cid[:])
	}

	body, err := ev.Body()
	if err != nil {
		return fmt.Errorf("events: view ContractEvent.Body: %w", err)
	}
	bodyVVal, err := body.V()
	if err != nil {
		return fmt.Errorf("events: view Body.V: %w", err)
	}
	// Only Body discriminant V=0 carries topics. A future body version
	// is a hard error matching the SQLite backend (sqlitedb/event.go) — a
	// silently contractID-only index would make topic queries miss
	// real events with no signal.
	if bodyVVal != 0 {
		return fmt.Errorf("events: unsupported ContractEvent body version %d", bodyVVal)
	}
	v0, err := body.V0()
	if err != nil {
		return fmt.Errorf("events: view Body.V0: %w", err)
	}
	topics, err := v0.Topics()
	if err != nil {
		return fmt.Errorf("events: view Body.V0.Topics: %w", err)
	}
	topicViews, err := topics.All()
	if err != nil {
		return fmt.Errorf("events: view Body.V0.Topics.All: %w", err)
	}
	t.add(FieldTopicCount, []byte{topicCountBucket(len(topicViews))})
	for i, topic := range topicViews {
		if i >= protocol.MaxTopicCount {
			break
		}
		// All yields each topic as its exact XDR, so Raw() would only re-walk it.
		t.add(topicField(i), []byte(topic))
	}
	return nil
}

func (t *Terms) bytes(i int) []byte {
	var start uint32
	if i > 0 {
		start = t.ends[i-1]
	}
	return t.buf[start:t.ends[i]]
}

func (t *Terms) add(field Field, value []byte) {
	t.buf = append(append(t.buf, byte(field)), value...)
	t.ends = append(t.ends, uint32(len(t.buf))) //nolint:gosec // a ledger's terms fit in uint32
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
