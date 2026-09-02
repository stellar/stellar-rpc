package event

// termlanes.go — constant-key side lanes for the two terms EVERY event
// carries: its ContractEventType and its topic-count bucket. Both draw from
// a closed alphabet — the three enum members, and one bucket per topic count
// a filter can name plus the overflow one — so their term keys are known
// before any ledger arrives, and an event's entire contribution to them is
// one ascending ID appended to a fixed slot.
//
// That is what takes them off the flat-pairs path (termsort.go), where they
// were the two most expensive terms in the ledger. At ~6k events/ledger they
// were 12k of the ~30k (term, eventID) pairs the arenas held, and the worst
// 12k: sortPairPerm buckets on key byte 0, so a firehose term's 6k IDENTICAL
// keys all collapse into ONE bucket, where the comparison sort burns
// O(m log m) on pairs that arrival order already left in the required order.
// A lane skips the hash, the pair, the scatter, the sort and the group scan
// alike; buildRuns merges the lanes back into the hashed terms' run sequence
// at their byte-order positions, so the packed row is byte-identical to
// hashing them (pinned in termsort_test.go).

import (
	"bytes"
	"fmt"
	"slices"

	"github.com/stellar/go-stellar-sdk/xdr"
)

const (
	// numEventTypeLanes covers exactly the ContractEventType members
	// ContractEventTypeView.Value admits — the only types the ingest walk
	// can reach, since AppendTerms hard-fails on any other.
	numEventTypeLanes = int(xdr.ContractEventTypeDiagnostic) + 1
	// numTopicCountLanes covers buckets 0..topicCountOverflowBucket.
	numTopicCountLanes = topicCountOverflowBucket + 1
	numLanes           = numEventTypeLanes + numTopicCountLanes
)

// laneKeys is every lane's term key: the event types first (lane == the enum
// value), then the topic-count buckets (lane == numEventTypeLanes + bucket).
// Built by calling the same two constructors every reader calls, so a lane
// key cannot drift from what the query path — or a cold artifact already on
// disk — means by these terms.
//
//nolint:gochecknoglobals // derived constant table, computed once
var laneKeys = func() [numLanes]TermKey {
	var keys [numLanes]TermKey
	for eventType := range numEventTypeLanes {
		keys[eventType] = EventTypeTermKey(xdr.ContractEventType(eventType))
	}
	for bucket := range numTopicCountLanes {
		keys[numEventTypeLanes+bucket] = TopicCountTermKey(bucket)
	}
	return keys
}()

// laneOrder lists the lanes byte-sorted by their keys — the order buildRuns
// merges them into the hashed terms, which are byte-sorted by the same total
// order the packed row's terms are.
//
//nolint:gochecknoglobals // derived constant table, computed once
var laneOrder = func() [numLanes]uint8 {
	var order [numLanes]uint8
	for lane := range order {
		order[lane] = uint8(lane)
	}
	slices.SortFunc(order[:], func(a, b uint8) int {
		return bytes.Compare(laneKeys[a][:], laneKeys[b][:])
	})
	return order
}()

// eventLanes is how appendTerms reports one event's two closed-alphabet
// terms to a caller that wants them diverted: lane indices, no keys, no
// hashing. Both fields are set on every non-error return.
type eventLanes struct {
	eventType  int
	topicCount int
}

// eventTypeLane maps a ContractEventType to its lane — the enum value
// itself, which is the bound ContractEventTypeView.Value already enforces on
// everything the ingest walk sees. A type outside it panics loudly during
// tests rather than silently filing every event under lane 0, the same call
// topicField makes for a topic position past the indexed range.
func eventTypeLane(eventType xdr.ContractEventType) int {
	if eventType < 0 || int(eventType) >= numEventTypeLanes {
		panic(fmt.Sprintf("eventTypeLane: unknown ContractEventType %d", eventType))
	}
	return int(eventType)
}

// topicCountLane maps an event's topic count to its bucket lane, clamping at
// the overflow bucket exactly as TopicCountTermKey does. count comes from a
// checked vec header, so it is never negative.
func topicCountLane(count int) int {
	return numEventTypeLanes + min(count, topicCountOverflowBucket)
}
