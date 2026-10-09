package methods

import (
	"bytes"
	"fmt"
	"slices"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// eventFilters is a validated GetEventsRequest.Filters with comparison values in wire form,
// matched against a view's raw bytes the way GetEventsRequest.Matches matches a decoded event.
// Empty matches every event.
type eventFilters []eventFilter

// eventFilter is one protocol.EventFilter; an empty field matches everything.
type eventFilter struct {
	types       []xdr.ContractEventType
	contractIDs [][]byte
	topics      []topicFilter
}

// topicFilter is one protocol.TopicFilter: a nil value is "*", atLeast is a trailing "**".
type topicFilter struct {
	values  [][]byte
	atLeast bool
}

func compileFilters(in []protocol.EventFilter) (eventFilters, error) {
	out := make(eventFilters, 0, len(in))
	for _, f := range in {
		c := eventFilter{}
		for _, name := range f.EventType.Keys() {
			typ, ok := protocol.GetEventTypeXDRFromEventType()[name]
			if !ok { // Valid admits only contract and system
				return nil, fmt.Errorf("unsupported event type %q", name)
			}
			c.types = append(c.types, typ)
		}
		for _, id := range f.ContractIDs {
			raw, err := strkey.Decode(strkey.VersionByteContract, id)
			if err != nil { // unreachable: Valid decoded it already
				return nil, fmt.Errorf("invalid contract ID: %v", id)
			}
			c.contractIDs = append(c.contractIDs, raw)
		}
		for _, tf := range f.Topics {
			t, err := compileTopicFilter(tf)
			if err != nil {
				return nil, err
			}
			c.topics = append(c.topics, t)
		}
		out = append(out, c)
	}
	return out, nil
}

func compileTopicFilter(tf protocol.TopicFilter) (topicFilter, error) {
	var t topicFilter
	if n := len(tf); n > 0 && tf[n-1].Wildcard != nil && *tf[n-1].Wildcard == protocol.WildCardZeroOrMore {
		tf, t.atLeast = tf[:n-1], true
	}
	t.values = make([][]byte, len(tf))
	for i, s := range tf {
		if s.ScVal == nil { // "*", or a segment Valid would have rejected
			continue
		}
		raw, err := s.ScVal.MarshalBinary()
		if err != nil {
			return topicFilter{}, fmt.Errorf("failed to marshal segment: %w", err)
		}
		t.values[i] = raw
	}
	return t, nil
}

// matchHeader reports whether some filter accepts the type and contract id. It never rejects
// an event match would accept, so a false answer skips reading the topics.
func (fs eventFilters) matchHeader(h eventHead) bool {
	return len(fs) == 0 || slices.ContainsFunc(fs, func(f eventFilter) bool { return f.matchHeader(h) })
}

// match reports whether some filter accepts the whole event.
func (fs eventFilters) match(h eventHead, topics [][]byte) bool {
	return len(fs) == 0 || slices.ContainsFunc(fs, func(f eventFilter) bool {
		return f.matchHeader(h) && (len(f.topics) == 0 ||
			slices.ContainsFunc(f.topics, func(t topicFilter) bool { return t.matches(topics) }))
	})
}

func (f eventFilter) matchHeader(h eventHead) bool {
	return (len(f.types) == 0 || slices.Contains(f.types, h.typ)) &&
		(len(f.contractIDs) == 0 || slices.ContainsFunc(f.contractIDs, func(id []byte) bool {
			return bytes.Equal(id, h.cid)
		}))
}

func (t topicFilter) matches(topics [][]byte) bool {
	if len(topics) < len(t.values) || (!t.atLeast && len(topics) != len(t.values)) {
		return false
	}
	for i, want := range t.values {
		if want != nil && !bytes.Equal(topics[i], want) {
			return false
		}
	}
	return true
}
