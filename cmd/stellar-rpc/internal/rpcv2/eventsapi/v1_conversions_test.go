package eventsapi

// Translation rules for the v1 filter model, encoded as data ahead of the
// implementation: each case pins one rule from getevents-v1-shim-brief.md
// against the exact store filters v1Filters must produce. Skipped while
// getEventsV1 is the prep stub; the parity harness cross-checks the same
// rules end to end.

import (
	"testing"

	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event"
)

//nolint:funlen // one table, one case per translation rule
func TestV1FiltersTranslation(t *testing.T) {
	t.Skip("shim core pending: v1Filters is the prep stub (see get_events_v1.go)")

	xferVal, xferRaw := symbolScVal(t, "xfer")
	mintVal, mintRaw := symbolScVal(t, "mint")
	aliceVal, aliceRaw := symbolScVal(t, "alice")
	star, dstar := "*", "**"
	seg := func(v xdr.ScVal) protocol.SegmentFilter { return protocol.SegmentFilter{ScVal: &v} }
	wild := func(w string) protocol.SegmentFilter { return protocol.SegmentFilter{Wildcard: &w} }
	contractType := protocol.EventTypeSet{protocol.EventTypeContract: nil}
	bothTypes := protocol.EventTypeSet{
		protocol.EventTypeContract: nil, protocol.EventTypeSystem: nil,
	}

	for name, tc := range map[string]struct {
		in   []protocol.EventFilter
		want []event.Filter // nil means the whole query matches every event
	}{
		"contract id alone": {
			in:   []protocol.EventFilter{{ContractIDs: []string{testContractStrkey(t, 0xAA)}}},
			want: []event.Filter{{ContractID: testContractRaw(0xAA)}},
		},
		"single type": {
			in:   []protocol.EventFilter{{EventType: contractType}},
			want: []event.Filter{{EventType: eventTypePtr(xdr.ContractEventTypeContract)}},
		},
		// A validated set holds only contract and system, so a set of both
		// constrains nothing; with nothing else in the filter, one branch
		// matches everything and the query collapses to match-all.
		"type set of both collapses to match-all": {
			in:   []protocol.EventFilter{{EventType: bothTypes}},
			want: nil,
		},
		"type set of both with a contract id keeps the contract id": {
			in: []protocol.EventFilter{{
				EventType: bothTypes, ContractIDs: []string{testContractStrkey(t, 0xAA)},
			}},
			want: []event.Filter{{ContractID: testContractRaw(0xAA)}},
		},
		// N segments without a trailing "**" match exactly N topics.
		"one-segment topic is exact arity one": {
			in: []protocol.EventFilter{{Topics: []protocol.TopicFilter{{seg(xferVal)}}}},
			want: []event.Filter{{
				Topics:     [protocol.MaxTopicCount][]byte{xferRaw},
				TopicCount: event.TopicCountFilter{Count: 1, Exact: true},
			}},
		},
		// A trailing "**" keeps the prefix constraints and relaxes the
		// arity to at-least-prefix.
		"trailing double-star is at-least arity": {
			in: []protocol.EventFilter{{Topics: []protocol.TopicFilter{
				{seg(xferVal), wild(dstar)}}}},
			want: []event.Filter{{
				Topics:     [protocol.MaxTopicCount][]byte{xferRaw},
				TopicCount: event.TopicCountFilter{Count: 1, Exact: false},
			}},
		},
		// "*" constrains no value, only the position's existence via arity.
		"star segment is positionless exact arity": {
			in:   []protocol.EventFilter{{Topics: []protocol.TopicFilter{{wild(star)}}}},
			want: []event.Filter{{TopicCount: event.TopicCountFilter{Count: 1, Exact: true}}},
		},
		"star then value pins position one": {
			in: []protocol.EventFilter{{Topics: []protocol.TopicFilter{
				{wild(star), seg(aliceVal)}}}},
			want: []event.Filter{{
				Topics:     [protocol.MaxTopicCount][]byte{nil, aliceRaw},
				TopicCount: event.TopicCountFilter{Count: 2, Exact: true},
			}},
		},
		// "**" alone is at-least-zero: no constraint at all, so the branch
		// matches everything and the query collapses to match-all.
		"double-star alone collapses to match-all": {
			in:   []protocol.EventFilter{{Topics: []protocol.TopicFilter{{wild(dstar)}}}},
			want: nil,
		},
		// OR dimensions multiply: type x contractIds x topics.
		"cross-product expansion": {
			in: []protocol.EventFilter{{
				EventType: contractType,
				ContractIDs: []string{
					testContractStrkey(t, 0xAA), testContractStrkey(t, 0xBB)},
				Topics: []protocol.TopicFilter{{seg(xferVal)}, {seg(mintVal)}},
			}},
			want: []event.Filter{
				{
					EventType:  eventTypePtr(xdr.ContractEventTypeContract),
					ContractID: testContractRaw(0xAA),
					Topics:     [protocol.MaxTopicCount][]byte{xferRaw},
					TopicCount: event.TopicCountFilter{Count: 1, Exact: true},
				},
				{
					EventType:  eventTypePtr(xdr.ContractEventTypeContract),
					ContractID: testContractRaw(0xAA),
					Topics:     [protocol.MaxTopicCount][]byte{mintRaw},
					TopicCount: event.TopicCountFilter{Count: 1, Exact: true},
				},
				{
					EventType:  eventTypePtr(xdr.ContractEventTypeContract),
					ContractID: testContractRaw(0xBB),
					Topics:     [protocol.MaxTopicCount][]byte{xferRaw},
					TopicCount: event.TopicCountFilter{Count: 1, Exact: true},
				},
				{
					EventType:  eventTypePtr(xdr.ContractEventTypeContract),
					ContractID: testContractRaw(0xBB),
					Topics:     [protocol.MaxTopicCount][]byte{mintRaw},
					TopicCount: event.TopicCountFilter{Count: 1, Exact: true},
				},
			},
		},
		"separate filters stay separate branches": {
			in: []protocol.EventFilter{
				{ContractIDs: []string{testContractStrkey(t, 0xAA)}},
				{Topics: []protocol.TopicFilter{{seg(mintVal)}}},
			},
			want: []event.Filter{
				{ContractID: testContractRaw(0xAA)},
				{
					Topics:     [protocol.MaxTopicCount][]byte{mintRaw},
					TopicCount: event.TopicCountFilter{Count: 1, Exact: true},
				},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := v1Filters(tc.in)
			require.NoError(t, err)
			require.ElementsMatch(t, tc.want, got)
		})
	}
}
