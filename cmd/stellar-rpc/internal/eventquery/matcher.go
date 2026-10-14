package eventquery

import (
	"bytes"
	"fmt"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// Matcher tests raw ContractEvent bytes against a filter set: the
// post-filter step of the archival index walk, and the whole filter step of
// an engine whose store has no index. The plan is computed once per query.
type Matcher struct {
	filters []Filter
	plan    filterPlan
}

// NewMatcher builds a Matcher for filters. An empty filter set matches every event.
func NewMatcher(filters []Filter) *Matcher {
	return &Matcher{filters: filters, plan: planFilters(filters)}
}

// Matches reports whether the ContractEvent encoded in raw satisfies at
// least one filter clause.
func (m *Matcher) Matches(raw []byte) (bool, error) {
	if len(m.filters) == 0 {
		return true, nil
	}
	return matchesAnyFilterView(raw, m.filters, &m.plan)
}

// filterPlan caches per-query info computed once at postFilter entry
// and consumed by matchesAnyFilterView: the topic positions any
// clause constrains and the highest constrained position (caps the
// view-path topic walk).
type filterPlan struct {
	anyTopic    bool
	maxTopicIdx int // -1 if no clause constrains any topic
	needsTopic  [protocol.MaxTopicCount]bool
}

func planFilters(filters []Filter) filterPlan {
	plan := filterPlan{maxTopicIdx: -1}
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

// eventFields holds the decoded fields matchesAnyFilterView pulls out
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

// matchesAnyFilterView reports whether the event encoded in raw
// satisfies at least one filter clause. It resolves each field a
// clause constrains via xdr.ContractEventView navigation,
// byte-comparing aliased .Raw() slices against the filter clauses.
// Zero per-event allocation — every byte slice involved aliases into
// raw.
//
// Fields are resolved at most once and only when a clause asks for
// them, cheapest first: events that fail every clause's type or
// ContractId check never trigger the topic walk, and events that pass
// do exactly one linear walk over Topics up to the highest constrained
// position.
//
//nolint:gocognit,cyclop // linear clause loop with per-field lazy caches; helpers would fragment the invariant
func matchesAnyFilterView(raw []byte, filters []Filter, plan *filterPlan) (bool, error) {
	ev := xdr.ContractEventView(raw)
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
// -1 when it has no V0 body, which TopicCountFilter.matches rejects for
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
	plan *filterPlan,
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
