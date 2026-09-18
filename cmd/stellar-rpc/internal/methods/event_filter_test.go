package methods

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// TestEventFilters pins the port of GetEventsRequest.Matches: filters OR, fields AND,
// "*" is any one topic, a trailing "**" relaxes the arity, and matchHeader never rejects
// an event match accepts.
func TestEventFilters(t *testing.T) {
	sym := func(s string) xdr.ScVal { return xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: (*xdr.ScSymbol)(&s)} }
	raw := func(v xdr.ScVal) []byte { b, err := v.MarshalBinary(); require.NoError(t, err); return b }
	seg := func(s string) protocol.SegmentFilter { v := sym(s); return protocol.SegmentFilter{ScVal: &v} }
	wild := func(w string) protocol.SegmentFilter { return protocol.SegmentFilter{Wildcard: &w} }
	cidA, cidB := bytes.Repeat([]byte{0xAA}, 32), bytes.Repeat([]byte{0xBB}, 32)
	strkeyA := strkey.MustEncode(strkey.VersionByteContract, cidA)
	strkeyB := strkey.MustEncode(strkey.VersionByteContract, cidB)
	topics := func(tf ...protocol.SegmentFilter) []protocol.TopicFilter { return []protocol.TopicFilter{tf} }
	contract := protocol.EventTypeSet{protocol.EventTypeContract: nil}

	xferAlice := [][]byte{raw(sym("xfer")), raw(sym("alice"))}
	headA := eventHead{typ: xdr.ContractEventTypeContract, cid: cidA}
	headB := eventHead{typ: xdr.ContractEventTypeSystem, cid: cidB}

	for name, tc := range map[string]struct {
		in     []protocol.EventFilter
		head   eventHead
		topics [][]byte
		want   bool
	}{
		"no filters match everything": {nil, headB, nil, true},
		"contract id":                 {[]protocol.EventFilter{{ContractIDs: []string{strkeyA}}}, headA, nil, true},
		"contract id mismatch":        {[]protocol.EventFilter{{ContractIDs: []string{strkeyA}}}, headB, nil, false},
		"type":                        {[]protocol.EventFilter{{EventType: contract}}, headB, nil, false},
		"fields AND within a filter": {
			[]protocol.EventFilter{{
				EventType: contract, ContractIDs: []string{strkeyA},
				Topics: topics(seg("xfer"), wild("*")),
			}}, headA, xferAlice, true,
		},
		"filters OR": {
			[]protocol.EventFilter{
				{ContractIDs: []string{strkeyA}, Topics: topics(seg("mint"))},
				{Topics: topics(seg("xfer"), seg("alice"))},
			}, headA, xferAlice, true,
		},
		"header and topics must come from one filter": {
			[]protocol.EventFilter{
				{ContractIDs: []string{strkeyA}, Topics: topics(seg("mint"))},
				{ContractIDs: []string{strkeyB}, Topics: topics(seg("xfer"), wild("**"))},
			},
			headA, xferAlice, false,
		},
		"exact arity":         {[]protocol.EventFilter{{Topics: topics(seg("xfer"))}}, headA, xferAlice, false},
		"trailing ** relaxes": {[]protocol.EventFilter{{Topics: topics(seg("xfer"), wild("**"))}}, headA, xferAlice, true},
		"** needs the prefix": {
			[]protocol.EventFilter{{Topics: topics(seg("a"), seg("b"), seg("c"), wild("**"))}}, headA, xferAlice, false,
		},
	} {
		t.Run(name, func(t *testing.T) {
			fs, err := compileFilters(tc.in)
			require.NoError(t, err)
			got := fs.match(tc.head, tc.topics)
			assert.Equal(t, tc.want, got)
			if got {
				assert.True(t, fs.matchHeader(tc.head), "matchHeader must not reject a matching event")
			}
		})
	}
}
