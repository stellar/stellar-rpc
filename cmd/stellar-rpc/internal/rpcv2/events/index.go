package events

import (
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

// TermsForBytes returns the term keys (contract ID + topics
// 0..MaxTopicCount-1) for a marshaled ContractEvent, navigating the raw
// XDR via xdr.ContractEventView instead of a full UnmarshalBinary.
func TermsForBytes(eventBytes []byte) ([]TermKey, error) {
	ev := xdr.ContractEventView(eventBytes)
	var keys []TermKey

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
