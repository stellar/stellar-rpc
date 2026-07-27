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

// MaxTermsPerEvent is the most term keys one event can contribute to the
// index: its contract ID plus topics 0..protocol.MaxTopicCount-1 — the
// protocol bound on queryable topic positions (topicField's panic guard pins
// the mapping to exactly that range). Arena-owning callers size per-ledger
// scratch with it.
const MaxTermsPerEvent = 1 + protocol.MaxTopicCount

// AppendTerms appends a marshaled ContractEvent's term keys to dst and
// returns the extended slice — TermsForBytes for arena callers: at most
// MaxTermsPerEvent keys are appended, so a writer-owned dst reused across
// events makes this path allocation-free (the hot ingest loop's shape).
// This is THE term derivation — TermsForBytes delegates here — and the
// golden sweep in append_terms_test.go pins its accept/reject decisions and
// key bytes against an independent Scan()-based reference.
func AppendTerms(dst []TermKey, eventBytes []byte) ([]TermKey, error) {
	ev := xdr.NewContractEventView(eventBytes)

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
	// hard error matching the SQLite backend (sqlitedb/event.go) — a silently
	// contractID-only index would make topic queries miss real events with no
	// signal.
	if bodyVVal != 0 {
		return nil, fmt.Errorf("events: unsupported ContractEvent body version %d", bodyVVal)
	}
	v0, err := body.ArmV0()
	if err != nil {
		return nil, fmt.Errorf("events: view Body.V0: %w", err)
	}
	topics, err := v0.Topics()
	if err != nil {
		return nil, fmt.Errorf("events: view Body.V0.Topics: %w", err)
	}
	return appendTopicTerms(dst, topics)
}

// appendTopicTerms hashes the first protocol.MaxTopicCount topics into dst
// via a manual Len()+Raw() walk — allocation-free and, unlike a scan
// stopped at the indexing cap, validating
// the WHOLE vec. Correctness of the walk rests on two view-API facts:
//
//   - Raw() on the vec sizes EVERY element — including ones past the
//     indexing cap — and errors when the total extent overruns the buffer,
//     so an event truncated anywhere inside its topics array is rejected
//     even when the truncation sits past the last hashed topic (an early
//     iterator break would silently accept it).
//   - NewScValView(raw[off:]).Raw() trims each element to its exact
//     extent; only those exact bytes may be hashed, or trailing bytes would
//     fold into the term key.
func appendTopicTerms(dst []TermKey, topics xdr.ContractEventV0TopicsView) ([]TermKey, error) {
	// Len() applies the checked count-vs-buffer guard the generated
	// iteration forms apply, so a
	// hostile count rejects here before any element walk.
	count, err := topics.Len()
	if err != nil {
		return nil, fmt.Errorf("events: view Body.V0.Topics count: %w", err)
	}
	raw, err := topics.Raw()
	if err != nil {
		return nil, fmt.Errorf("events: view Body.V0.Topics raw: %w", err)
	}
	off := 4 // the vec's count header; raw is trimmed to the vec's exact extent
	for i := range min(int(count), protocol.MaxTopicCount) {
		topic, terr := xdr.NewScValView(raw[off:]).Raw()
		if terr != nil {
			return nil, fmt.Errorf("events: view Body.V0.Topics[%d] raw: %w", i, terr)
		}
		dst = append(dst, ComputeTermKey(topic, topicField(i)))
		off += len(topic)
	}
	return dst, nil
}

// TermsForBytes returns the term keys (contract ID + topics
// 0..MaxTopicCount-1) for a marshaled ContractEvent — AppendTerms without a
// caller-owned arena (a fresh slice per event). One derivation serves both
// entry points, so the accept/reject decisions and key bytes cannot drift
// between the arena-owning hot path and the per-call cold/test path.
func TermsForBytes(eventBytes []byte) ([]TermKey, error) {
	return AppendTerms(nil, eventBytes)
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
