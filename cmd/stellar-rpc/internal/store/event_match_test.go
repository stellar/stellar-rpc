package store

// Translation rules for the v1 filter model and the matcher checks that only
// the compiled clauses enforce. eventsapi's parity harness cross-checks the
// same rules end to end against the shared v1 handler.

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
)

func testContractRaw(b byte) []byte { return bytes.Repeat([]byte{b}, 32) }

func testContractStrkey(t *testing.T, b byte) string {
	t.Helper()
	s, err := strkey.Encode(strkey.VersionByteContract, testContractRaw(b))
	require.NoError(t, err)
	return s
}

// symbolScVal returns an ScVal symbol and its canonical XDR bytes, the form clauses carry.
func symbolScVal(t *testing.T, s string) (xdr.ScVal, []byte) {
	t.Helper()
	val := xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: (*xdr.ScSymbol)(&s)}
	raw, err := val.MarshalBinary()
	require.NoError(t, err)
	return val, raw
}

func eventTypePtr(v xdr.ContractEventType) *xdr.ContractEventType { return new(v) }

// contractEventBytes marshals a V0 ContractEvent with the given contract id, type and topics.
func contractEventBytes(t *testing.T, cid []byte, typ xdr.ContractEventType, topics ...xdr.ScVal) []byte {
	t.Helper()
	sym := xdr.ScSymbol("data")
	ev := xdr.ContractEvent{
		Type: typ,
		Body: xdr.ContractEventBody{V: 0, V0: &xdr.ContractEventV0{
			Topics: topics,
			Data:   xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym},
		}},
	}
	if cid != nil {
		var id xdr.ContractId
		copy(id[:], cid)
		ev.ContractId = &id
	}
	raw, err := ev.MarshalBinary()
	require.NoError(t, err)
	return raw
}

//nolint:funlen // one table, one case per translation rule
func TestCompileV1EventFilters(t *testing.T) {
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
		want []EventFilter // nil means the whole query matches every event
	}{
		"contract id alone": {
			in:   []protocol.EventFilter{{ContractIDs: []string{testContractStrkey(t, 0xAA)}}},
			want: []EventFilter{{ContractID: testContractRaw(0xAA)}},
		},
		"single type": {
			in:   []protocol.EventFilter{{EventType: contractType}},
			want: []EventFilter{{EventType: eventTypePtr(xdr.ContractEventTypeContract)}},
		},
		// Valid admits only contract and system, the only types either backend
		// ingests, so a set of both constrains nothing.
		"type set of both is a wildcard": {
			in:   []protocol.EventFilter{{EventType: bothTypes}},
			want: nil,
		},
		"type set of both with a contract id keeps only the contract id": {
			in: []protocol.EventFilter{{
				EventType: bothTypes, ContractIDs: []string{testContractStrkey(t, 0xAA)},
			}},
			want: []EventFilter{{ContractID: testContractRaw(0xAA)}},
		},
		// N segments without a trailing "**" match exactly N topics.
		"one-segment topic is exact arity one": {
			in: []protocol.EventFilter{{Topics: []protocol.TopicFilter{{seg(xferVal)}}}},
			want: []EventFilter{{
				Topics:     [protocol.MaxTopicCount][]byte{xferRaw},
				TopicCount: TopicCountFilter{Count: 1, Exact: true},
			}},
		},
		// A trailing "**" keeps the prefix constraints and relaxes the
		// arity to at-least-prefix.
		"trailing double-star is at-least arity": {
			in: []protocol.EventFilter{{Topics: []protocol.TopicFilter{
				{seg(xferVal), wild(dstar)},
			}}},
			want: []EventFilter{{
				Topics:     [protocol.MaxTopicCount][]byte{xferRaw},
				TopicCount: TopicCountFilter{Count: 1, Exact: false},
			}},
		},
		// "*" constrains no value, only the position's existence via arity.
		"star segment is positionless exact arity": {
			in:   []protocol.EventFilter{{Topics: []protocol.TopicFilter{{wild(star)}}}},
			want: []EventFilter{{TopicCount: TopicCountFilter{Count: 1, Exact: true}}},
		},
		"star then value pins position one": {
			in: []protocol.EventFilter{{Topics: []protocol.TopicFilter{
				{wild(star), seg(aliceVal)},
			}}},
			want: []EventFilter{{
				Topics:     [protocol.MaxTopicCount][]byte{nil, aliceRaw},
				TopicCount: TopicCountFilter{Count: 2, Exact: true},
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
					testContractStrkey(t, 0xAA), testContractStrkey(t, 0xBB),
				},
				Topics: []protocol.TopicFilter{{seg(xferVal)}, {seg(mintVal)}},
			}},
			want: []EventFilter{
				{
					EventType:  eventTypePtr(xdr.ContractEventTypeContract),
					ContractID: testContractRaw(0xAA),
					Topics:     [protocol.MaxTopicCount][]byte{xferRaw},
					TopicCount: TopicCountFilter{Count: 1, Exact: true},
				},
				{
					EventType:  eventTypePtr(xdr.ContractEventTypeContract),
					ContractID: testContractRaw(0xAA),
					Topics:     [protocol.MaxTopicCount][]byte{mintRaw},
					TopicCount: TopicCountFilter{Count: 1, Exact: true},
				},
				{
					EventType:  eventTypePtr(xdr.ContractEventTypeContract),
					ContractID: testContractRaw(0xBB),
					Topics:     [protocol.MaxTopicCount][]byte{xferRaw},
					TopicCount: TopicCountFilter{Count: 1, Exact: true},
				},
				{
					EventType:  eventTypePtr(xdr.ContractEventTypeContract),
					ContractID: testContractRaw(0xBB),
					Topics:     [protocol.MaxTopicCount][]byte{mintRaw},
					TopicCount: TopicCountFilter{Count: 1, Exact: true},
				},
			},
		},
		"separate filters stay separate branches": {
			in: []protocol.EventFilter{
				{ContractIDs: []string{testContractStrkey(t, 0xAA)}},
				{Topics: []protocol.TopicFilter{{seg(mintVal)}}},
			},
			want: []EventFilter{
				{ContractID: testContractRaw(0xAA)},
				{
					Topics:     [protocol.MaxTopicCount][]byte{mintRaw},
					TopicCount: TopicCountFilter{Count: 1, Exact: true},
				},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := CompileV1EventFilters(tc.in)
			require.NoError(t, err)
			require.ElementsMatch(t, tc.want, got)
		})
	}
}

// TestMatchesAnyFilterView_TypeAndCount covers the matcher's type and
// topic-count checks. rpcv2's exact indexes only reach them when a union
// clause falls through or a term hash collides, so nothing else covers them.
func TestMatchesAnyFilterView_TypeAndCount(t *testing.T) {
	// Only arity matters here, so both events carry the same topic value.
	cid := testContractRaw(0x00)
	topic, _ := symbolScVal(t, "alpha")
	oneTopic := contractEventBytes(t, cid, xdr.ContractEventTypeContract, topic)
	twoTopics := contractEventBytes(t, cid, xdr.ContractEventTypeContract, topic, topic)

	system, contract := xdr.ContractEventTypeSystem, xdr.ContractEventTypeContract
	exactly1 := EventFilter{TopicCount: TopicCountFilter{Count: 1, Exact: true}}
	exactly2 := EventFilter{TopicCount: TopicCountFilter{Count: 2, Exact: true}}
	atLeast2 := EventFilter{TopicCount: TopicCountFilter{Count: 2}}

	for name, tc := range map[string]struct {
		raw    []byte
		filter EventFilter
		want   bool
	}{
		"wrong type rejected":     {oneTopic, EventFilter{EventType: &system}, false},
		"right type accepted":     {oneTopic, EventFilter{EventType: &contract}, true},
		"count above exact":       {twoTopics, exactly1, false},
		"count below exact":       {oneTopic, exactly2, false},
		"exact count accepted":    {twoTopics, exactly2, true},
		"count below the minimum": {oneTopic, atLeast2, false},
		"at least count accepted": {twoTopics, atLeast2, true},
	} {
		t.Run(name, func(t *testing.T) {
			filters := []EventFilter{tc.filter}
			plan := PlanFilters(filters)
			got, err := MatchesAnyFilterView(xdr.ContractEventView(tc.raw), filters, &plan)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}
