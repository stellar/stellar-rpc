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
// MaxTermsPerEvent is the most term keys one event can contribute to the
// index: its type, its topic count, its contract ID, and topics
// 0..protocol.MaxTopicCount-1 — the protocol bound on queryable topic
// positions (topicField's panic guard pins the mapping to exactly that
// range). Arena-owning callers size per-ledger scratch with it.
const MaxTermsPerEvent = 3 + protocol.MaxTopicCount

// AppendTerms appends a marshaled ContractEvent's term keys to dst and
// returns the extended slice — TermsForBytes for arena callers: at most
// MaxTermsPerEvent keys are appended, so a writer-owned dst reused across
// events makes this path allocation-free (the hot ingest loop's shape).
// Same accept/reject decisions and key bytes as TermsForBytes (the golden
// sweep in append_terms_test.go pins the equivalence); only the topics walk
// differs — see appendTopicTerms.
func AppendTerms(dst []TermKey, eventBytes []byte) ([]TermKey, error) {
	return appendTerms(dst, eventBytes, nil)
}

// appendTerms is AppendTerms' walk with one divergence: a non-nil lanes
// DIVERTS the two closed-alphabet terms — the event's type and its
// topic-count bucket — out of dst and into lanes as lane indices, hashing
// neither (see termlanes.go). Both entry points share this body so the
// accept/reject decisions the golden sweep pins cannot drift between them.
func appendTerms(dst []TermKey, eventBytes []byte, lanes *eventLanes) ([]TermKey, error) {
	ev := xdr.ContractEventView(eventBytes)

	typeView, err := ev.Type()
	if err != nil {
		return nil, fmt.Errorf("events: view Type: %w", err)
	}
	// A type outside the enum is a hard indexing error — the alternative is
	// indexing under a bucket no filter can name (see TermsForBytes).
	eventType, err := typeView.Value()
	if err != nil {
		return nil, fmt.Errorf("events: view Type value: %w", err)
	}
	if lanes != nil {
		lanes.eventType = eventTypeLane(eventType)
	} else {
		dst = append(dst, EventTypeTermKey(eventType))
	}

	cidOpt, err := ev.ContractId()
	if err != nil {
		return nil, fmt.Errorf("events: view ContractId: %w", err)
	}
	cidView, present, err := cidOpt.Unwrap()
	if err != nil {
		return nil, fmt.Errorf("events: view ContractId unwrap: %w", err)
	}
	if present {
		cid, cerr := cidView.Value()
		if cerr != nil {
			return nil, fmt.Errorf("events: view ContractId value: %w", cerr)
		}
		dst = append(dst, ComputeTermKey(cid[:], FieldContractID))
	}

	body, err := ev.Body()
	if err != nil {
		return nil, fmt.Errorf("events: view ContractEvent.Body: %w", err)
	}
	bodyVVal, err := body.V()
	if err != nil {
		return nil, fmt.Errorf("events: view Body.V: %w", err)
	}
	// Only Body discriminant V=0 carries topics. A future body version is a
	// hard error matching TermsForBytes (and the SQLite backend) — a silently
	// contractID-only index would make topic queries miss real events with no
	// signal.
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
	return appendTopicTerms(dst, topics, lanes)
}

// appendTopicTerms hashes the first protocol.MaxTopicCount topics into dst
// via a manual Count()+Raw() walk — the allocation-free replacement for
// TopicsView.All (which builds a per-event view slice). Correctness of the
// walk rests on two view-API facts:
//
//   - Raw() sizes EVERY element of the vec — including ones past the
//     indexing cap — and errors when the total extent overruns the buffer,
//     so an event truncated anywhere inside its topics array is rejected
//     exactly like All() rejected it.
//   - At()/Iter() yield UNTRIMMED views (fat slices running to the end of
//     the buffer); only exact-extent Raw() bytes may be hashed, or trailing
//     bytes would fold into the term key.
//
// lanes carries appendTerms' divert through: a non-nil one takes the
// topic-count term, never a topic term.
func appendTopicTerms(
	dst []TermKey, topics xdr.ContractEventV0TopicsView, lanes *eventLanes,
) ([]TermKey, error) {
	// Count() applies the checked count-vs-buffer guard All() applied, so a
	// hostile count rejects here before any element walk.
	count, err := topics.Count()
	if err != nil {
		return nil, fmt.Errorf("events: view Body.V0.Topics count: %w", err)
	}
	raw, err := topics.Raw()
	if err != nil {
		return nil, fmt.Errorf("events: view Body.V0.Topics raw: %w", err)
	}
	if lanes != nil {
		lanes.topicCount = topicCountLane(int(count))
	} else {
		dst = append(dst, TopicCountTermKey(int(count)))
	}
	off := 4 // the vec's count header; raw is trimmed to the vec's exact extent
	for i := range min(count, protocol.MaxTopicCount) {
		topic, terr := xdr.ScValView(raw[off:]).Raw()
		if terr != nil {
			return nil, fmt.Errorf("events: view Body.V0.Topics[%d] raw: %w", i, terr)
		}
		dst = append(dst, ComputeTermKey(topic, topicField(i)))
		off += len(topic)
	}
	return dst, nil
}

// TermsForBytes returns the term keys (contract ID + topics
// 0..MaxTopicCount-1) for a marshaled ContractEvent, navigating the raw
// XDR via xdr.ContractEventView instead of a full UnmarshalBinary.
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
