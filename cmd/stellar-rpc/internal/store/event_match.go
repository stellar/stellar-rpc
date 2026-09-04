package store

import (
	"bytes"
	"fmt"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// EventFilter is one clause in the union of an events query. Within a
// clause every constrained field is AND-ed against the event; zero values
// are wildcards. Shared by the sqlite (rpcv1) and chunk (rpcv2) backends.
//
// Topics[i] constrains topic position i.
type EventFilter struct {
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

// CompileV1EventFilters expands a validated v1 filter list into clauses. The
// OR dimensions within one v1 filter (contract ids, topic filters) multiply
// out, one clause per combination: at most 5 filters x 5 contract ids
// x 5 topics = 125. A combination with no constraints matches every event, so
// the whole query collapses to match-all (nil).
func CompileV1EventFilters(in []protocol.EventFilter) ([]EventFilter, error) {
	var out []EventFilter
	for i := range in {
		expanded, matchAll, err := expandV1Filter(&in[i])
		if err != nil {
			return nil, err
		}
		if matchAll {
			return nil, nil
		}
		out = append(out, expanded...)
	}
	return out, nil
}

func expandV1Filter(f *protocol.EventFilter) ([]EventFilter, bool, error) {
	// A validated type set holds only contract and system: naming both
	// constrains nothing, naming one is one term. Either way the type never
	// multiplies the expansion.
	var eventType *xdr.ContractEventType
	if len(f.EventType) == 1 {
		name := f.EventType.Keys()[0]
		typ, ok := protocol.GetEventTypeXDRFromEventType()[name]
		if !ok {
			// Valid admits only contract and system, so a name that is
			// neither is a handler bug, not client input.
			return nil, false, fmt.Errorf("unsupported event type %q", name)
		}
		eventType = &typ
	}
	contracts := [][]byte{nil}
	if len(f.ContractIDs) > 0 {
		contracts = make([][]byte, 0, len(f.ContractIDs))
		for _, id := range f.ContractIDs {
			raw, err := strkey.Decode(strkey.VersionByteContract, id)
			if err != nil {
				// Unreachable: req.Valid decoded it already. The message is
				// the v1 handler's backstop wording.
				return nil, false, fmt.Errorf("invalid contract ID: %v", id)
			}
			contracts = append(contracts, raw)
		}
	}
	shapes := []topicShape{{}}
	if len(f.Topics) > 0 {
		shapes = make([]topicShape, 0, len(f.Topics))
		for _, tf := range f.Topics {
			shape, err := topicShapeOf(tf)
			if err != nil {
				return nil, false, err
			}
			shapes = append(shapes, shape)
		}
	}

	out := make([]EventFilter, 0, len(contracts)*len(shapes))
	for _, cid := range contracts {
		for _, sh := range shapes {
			flt := EventFilter{
				ContractID: cid, EventType: eventType,
				Topics: sh.topics, TopicCount: sh.count,
			}
			if isMatchAll(&flt) {
				return nil, true, nil
			}
			out = append(out, flt)
		}
	}
	return out, false, nil
}

// topicShape is one v1 TopicFilter in clause terms: the pinned positional
// values plus the arity constraint. N segments match exactly N topics; a
// trailing "**" relaxes that to at least the prefix, and "at least zero" is
// the zero value, no constraint.
type topicShape struct {
	topics [protocol.MaxTopicCount][]byte
	count  TopicCountFilter
}

func topicShapeOf(tf protocol.TopicFilter) (topicShape, error) {
	segs := tf
	shape := topicShape{count: TopicCountFilter{Count: len(tf), Exact: true}}
	if n := len(tf); n > 0 && tf[n-1].Wildcard != nil && *tf[n-1].Wildcard == protocol.WildCardZeroOrMore {
		// "At least zero" is the wildcard, which is the zero value.
		segs = tf[:n-1]
		shape.count = TopicCountFilter{Count: len(segs)}
	}
	for i, s := range segs {
		// "*" is any value, and the position still exists via the count.
		// A segment with neither value nor wildcard is skipped rather than
		// dereferenced.
		if s.Wildcard != nil || s.ScVal == nil {
			continue
		}
		raw, err := s.ScVal.MarshalBinary()
		if err != nil {
			return topicShape{}, fmt.Errorf("failed to marshal segment: %w", err)
		}
		shape.topics[i] = raw
	}
	return shape, nil
}

func isMatchAll(f *EventFilter) bool {
	if f.EventType != nil || len(f.ContractID) > 0 || f.TopicCount != (TopicCountFilter{}) {
		return false
	}
	for i := range f.Topics {
		if len(f.Topics[i]) > 0 {
			return false
		}
	}
	return true
}

// FilterPlan caches per-query info computed once by PlanFilters and
// consumed by MatchesAnyFilterView: the topic positions any clause
// constrains and the highest constrained position (caps the topic walk).
type FilterPlan struct {
	anyTopic    bool
	maxTopicIdx int // -1 if no clause constrains any topic
	needsTopic  [protocol.MaxTopicCount]bool
}

// PlanFilters computes the FilterPlan for filters.
func PlanFilters(filters []EventFilter) FilterPlan {
	plan := FilterPlan{maxTopicIdx: -1}
	for fi := range filters {
		f := &filters[fi]
		for i, want := range f.Topics {
			if len(want) == 0 {
				continue
			}
			plan.needsTopic[i] = true
			plan.anyTopic = true
			if i > plan.maxTopicIdx {
				plan.maxTopicIdx = i
			}
		}
	}
	return plan
}

// eventFields holds the decoded fields MatchesAnyFilterView pulls out
// of one event, each resolved at most once and only when some clause
// asks for it.
type eventFields struct {
	contractID     []byte
	contractIDDone bool

	eventType     xdr.ContractEventType
	eventTypeDone bool

	topicCount     int
	topicCountDone bool

	topics     [protocol.MaxTopicCount][]byte
	topicsDone bool
}

// MatchesAnyFilterView reports whether the event behind ev satisfies at
// least one clause. It resolves each field a clause constrains via view
// navigation, byte-comparing aliased .Raw() slices against the clause
// values. Zero per-event allocation: every byte slice involved aliases
// into ev.
//
// Fields are resolved at most once and only when a clause asks for
// them, cheapest first: events that fail every clause's type or
// ContractId check never trigger the topic walk, and events that pass
// do exactly one linear walk over Topics up to the highest constrained
// position.
//
//nolint:gocognit,cyclop // linear clause loop with per-field lazy caches; helpers would fragment the invariant
func MatchesAnyFilterView(ev xdr.ContractEventView, filters []EventFilter, plan *FilterPlan) (bool, error) {
	var got eventFields

	for fi := range filters {
		f := &filters[fi]
		if f.EventType != nil {
			if !got.eventTypeDone {
				eventType, err := resolveViewEventType(ev)
				if err != nil {
					return false, err
				}
				got.eventType, got.eventTypeDone = eventType, true
			}
			if got.eventType != *f.EventType {
				continue
			}
		}
		if len(f.ContractID) > 0 {
			if !got.contractIDDone {
				cid, err := resolveViewContractID(ev)
				if err != nil {
					return false, err
				}
				got.contractID, got.contractIDDone = cid, true
			}
			if !bytes.Equal(got.contractID, f.ContractID) {
				continue
			}
		}
		if !f.TopicCount.IsWildcard() {
			if !got.topicCountDone {
				n, err := resolveViewTopicCount(ev)
				if err != nil {
					return false, err
				}
				got.topicCount, got.topicCountDone = n, true
			}
			if !f.TopicCount.Matches(got.topicCount) {
				continue
			}
		}
		matched := true
		for i, want := range f.Topics {
			if len(want) == 0 {
				continue
			}
			if !got.topicsDone {
				if err := collectTopicViewBytes(ev, plan, &got.topics); err != nil {
					return false, err
				}
				got.topicsDone = true
			}
			g := got.topics[i]
			if g == nil || !bytes.Equal(g, want) {
				matched = false
				break
			}
		}
		if matched {
			return true, nil
		}
	}
	return false, nil
}

func resolveViewEventType(ev xdr.ContractEventView) (xdr.ContractEventType, error) {
	typeView, err := ev.Type()
	if err != nil {
		return 0, fmt.Errorf("events: post-filter view Type: %w", err)
	}
	eventType, err := typeView.Value()
	if err != nil {
		return 0, fmt.Errorf("events: post-filter view Type value: %w", err)
	}
	return eventType, nil
}

// resolveViewTopics returns the event's Body.V0.Topics. ok is false for
// a body version that carries no topics at all.
func resolveViewTopics(ev xdr.ContractEventView) (xdr.ContractEventV0TopicsView, bool, error) {
	body, err := ev.Body()
	if err != nil {
		return nil, false, fmt.Errorf("events: post-filter view Body: %w", err)
	}
	bodyV, err := body.V()
	if err != nil {
		return nil, false, fmt.Errorf("events: post-filter view Body.V: %w", err)
	}
	if bodyV != 0 {
		return nil, false, nil
	}
	v0, err := body.V0()
	if err != nil {
		return nil, false, fmt.Errorf("events: post-filter view Body.V0: %w", err)
	}
	topics, err := v0.Topics()
	if err != nil {
		return nil, false, fmt.Errorf("events: post-filter view Body.V0.Topics: %w", err)
	}
	return topics, true, nil
}

// resolveViewTopicCount returns how many topics the event carries, or
// -1 when it has no V0 body, which TopicCountFilter.Matches rejects for
// every constraint.
func resolveViewTopicCount(ev xdr.ContractEventView) (int, error) {
	topics, ok, err := resolveViewTopics(ev)
	if err != nil || !ok {
		return -1, err
	}
	count, err := topics.Count()
	if err != nil {
		return 0, fmt.Errorf("events: post-filter view Body.V0.Topics.Count: %w", err)
	}
	return count, nil
}

// resolveViewContractID returns the event's ContractId aliased into the
// raw buffer, or nil when it has none.
func resolveViewContractID(ev xdr.ContractEventView) ([]byte, error) {
	cidOpt, err := ev.ContractId()
	if err != nil {
		return nil, fmt.Errorf("events: post-filter view ContractId opt: %w", err)
	}
	cidView, present, err := cidOpt.Unwrap()
	if err != nil {
		return nil, fmt.Errorf("events: post-filter view ContractId unwrap: %w", err)
	}
	if !present {
		return nil, nil
	}
	cid, err := cidView.Raw()
	if err != nil {
		return nil, fmt.Errorf("events: post-filter view ContractId raw: %w", err)
	}
	return cid, nil
}

// collectTopicViewBytes walks the ContractEventView's Body.V0.Topics
// once linearly and captures each constrained position's .Raw() bytes
// into topicRaw. Stops after the highest constrained position so the
// walk is O(plan.maxTopicIdx+1) rather than the O(MaxTopicCount²)
// that calling .At(j) for each j would produce (ScVecView.At is a
// prefix walk under the hood). A body version with no topics leaves
// topicRaw zero (every constrained position will mismatch downstream).
func collectTopicViewBytes(
	ev xdr.ContractEventView,
	plan *FilterPlan,
	topicRaw *[protocol.MaxTopicCount][]byte,
) error {
	if !plan.anyTopic {
		return nil
	}
	topicsArr, ok, err := resolveViewTopics(ev)
	if err != nil || !ok {
		return err
	}
	i := 0
	for topic, ierr := range topicsArr.Iter() {
		if ierr != nil {
			return fmt.Errorf("events: post-filter view topic iter: %w", ierr)
		}
		if i > plan.maxTopicIdx || i >= protocol.MaxTopicCount {
			break
		}
		if plan.needsTopic[i] {
			rawBytes, err := topic.Raw()
			if err != nil {
				return fmt.Errorf("events: post-filter view topic[%d].Raw: %w", i, err)
			}
			topicRaw[i] = rawBytes
		}
		i++
	}
	return nil
}
