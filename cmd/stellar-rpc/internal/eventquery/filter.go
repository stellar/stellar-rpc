// Package eventquery holds the daemon-neutral parts of the events query:
// the filter model and matcher, the stored event payload, the opaque
// queryEvents cursor and its codec, and the page arithmetic every engine
// shares. The archival daemon's chunk-walking engine (rpcv2/query) and the
// rpcv1 daemon's SQLite engine (rpcv1/sqlitedb) both build on it, so a
// cursor minted by one daemon reads the same on the other.
package eventquery

import (
	"fmt"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// Filter is one item in the union of an events query. Within a
// single filter, every constrained field is AND-ed together against
// the corresponding indexed field of an event. Fields left at their
// zero value are wildcards.
//
// Topics[i] constrains topic position i. Positions beyond
// protocol.MaxTopicCount are not indexed (see TermsForBytes).
type Filter struct {
	ContractID []byte
	Topics     [protocol.MaxTopicCount][]byte
	// EventType constrains the event's type. A nil pointer is a
	// wildcard. A filter accepting several types is several filters.
	EventType *xdr.ContractEventType
	// TopicCount constrains how many topics the event carries.
	TopicCount TopicCountFilter
}

// TopicCountFilter constrains an event's topic count to at least
// Count, or to exactly Count when Exact is set. Its zero value, "at
// least zero", is the wildcard.
//
// This is getEvents v1's topic arity: a topic filter ending in "**"
// matches events with at least as many topics as the filter names, and
// one that does not match events with exactly that many.
type TopicCountFilter struct {
	Count int
	Exact bool
}

// IsWildcard reports whether f constrains nothing.
func (f TopicCountFilter) IsWildcard() bool { return f == TopicCountFilter{} }

// Matches reports whether an event carrying n topics satisfies f. A
// negative n stands for an event with no V0 body, which carries no
// topics at all and satisfies no constraint.
func (f TopicCountFilter) Matches(n int) bool {
	if n < 0 {
		return false
	}
	if f.Exact {
		return n == f.Count
	}
	return n >= f.Count
}

// ValidateFilters rejects filter shapes no engine can serve.
func ValidateFilters(filters []Filter) error {
	for fi := range filters {
		f := &filters[fi]
		if l := len(f.ContractID); l != 0 && l != 32 {
			return fmt.Errorf(
				"events: filter[%d].ContractID must be 0 or 32 bytes, got %d", fi, l)
		}
		if f.EventType != nil && !f.EventType.ValidEnum(int32(*f.EventType)) {
			return fmt.Errorf(
				"events: filter[%d].EventType %d is not a known event type", fi, *f.EventType)
		}
		for pos, topic := range f.Topics {
			if len(topic) == 0 {
				continue
			}
			// The topic bytes are what ComputeTermKey hashes, so a filter
			// naming anything but one whole ScVal was never minted here.
			// The length comparison rejects trailing bytes.
			view := xdr.ScValView(topic)
			raw, err := view.Raw()
			if err != nil || len(raw) != len(topic) {
				return fmt.Errorf(
					"events: filter[%d].Topics[%d] is not one ScVal", fi, pos)
			}
		}
		if f.TopicCount.Count < 0 {
			return fmt.Errorf(
				"events: filter[%d].TopicCount.Count must be non-negative, got %d",
				fi, f.TopicCount.Count)
		}
		// Above MaxTopicCount the index cannot answer a count exactly:
		// every such count shares the overflow bucket. No getEvents
		// filter shape can name that many topics, and the cursor codec
		// carries the count in one byte.
		if f.TopicCount.Count > protocol.MaxTopicCount {
			return fmt.Errorf(
				"events: filter[%d].TopicCount.Count must be at most %d, got %d",
				fi, protocol.MaxTopicCount, f.TopicCount.Count)
		}
	}
	return nil
}

// CountDistinctTerms returns how many distinct value terms the filters
// name, deduped by field and value together: one contract ID in five
// filters counts once, the same bytes in two topic positions count twice.
// It is the handler's term-budget measure. The archival index's own count
// (stores/event.CountDistinctTerms) keys on the same (field, bytes) pairs.
func CountDistinctTerms(filters []Filter) int {
	type term struct {
		field int
		value string
	}
	unique := make(map[term]struct{})
	for i := range filters {
		f := &filters[i]
		if len(f.ContractID) > 0 {
			unique[term{-1, string(f.ContractID)}] = struct{}{}
		}
		if f.EventType != nil {
			unique[term{-2, string(rune(*f.EventType))}] = struct{}{}
		}
		for pos, t := range f.Topics {
			if len(t) > 0 {
				unique[term{pos, string(t)}] = struct{}{}
			}
		}
	}
	return len(unique)
}
