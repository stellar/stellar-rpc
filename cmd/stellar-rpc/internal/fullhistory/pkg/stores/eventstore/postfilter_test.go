package eventstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/events"
)

// makeTestEvent constructs a Payload carrying a ContractEvent with the
// supplied contract id and topic symbols. Returns the Payload in TWO
// shapes (struct-decoded and view-decoded) so equivalence tests can
// drive postFilter through both arms on identical wire bytes.
func makeTestEvent(t *testing.T, cidByte byte, topicSyms ...string) (events.Payload, events.Payload) {
	t.Helper()
	var cid xdr.ContractId
	cid[0] = cidByte
	topics := make([]xdr.ScVal, len(topicSyms))
	for i, s := range topicSyms {
		sym := xdr.ScSymbol(s)
		topics[i] = xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym}
	}
	data := xdr.ScSymbol("data-" + string(rune('A'+cidByte)))
	src := events.Payload{
		TxHash: xdr.Hash{0xde, 0xad},
		ContractEvent: xdr.ContractEvent{
			ContractId: &cid,
			Type:       xdr.ContractEventTypeContract,
			Body: xdr.ContractEventBody{
				V: 0,
				V0: &xdr.ContractEventV0{
					Topics: topics,
					Data:   xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &data},
				},
			},
		},
	}
	wire, err := src.Marshal()
	require.NoError(t, err)

	var structForm, viewForm events.Payload
	require.NoError(t, structForm.Unmarshal(wire))
	// Fresh copy of the wire so the view path doesn't share state
	// with the struct path; UnmarshalView aliases this slice.
	wireForView := append([]byte(nil), wire...)
	require.NoError(t, viewForm.UnmarshalView(wireForView))
	return structForm, viewForm
}

func mustMarshalScVal(t *testing.T, sym string) []byte {
	t.Helper()
	s := xdr.ScSymbol(sym)
	v := xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &s}
	b, err := v.MarshalBinary()
	require.NoError(t, err)
	return b
}

// TestPostFilter_StructAndViewPathAgree pins the central invariant:
// for any wire-form Payload, struct-decoded and view-decoded shapes
// produce bit-identical postFilter outputs. Without this, the bench
// is comparing apples to oranges.
func TestPostFilter_StructAndViewPathAgree(t *testing.T) {
	// Three events with distinct contract IDs and topic shapes.
	pStructA, pViewA := makeTestEvent(t, 1, "alpha", "beta")
	pStructB, pViewB := makeTestEvent(t, 2, "alpha", "gamma")
	pStructC, pViewC := makeTestEvent(t, 1, "delta")

	alphaBytes := mustMarshalScVal(t, "alpha")
	betaBytes := mustMarshalScVal(t, "beta")
	gammaBytes := mustMarshalScVal(t, "gamma")
	deltaBytes := mustMarshalScVal(t, "delta")
	cid1Full := make([]byte, 32)
	cid1Full[0] = 1
	cid2Full := make([]byte, 32)
	cid2Full[0] = 2

	cases := []struct {
		name        string
		filters     []Filter
		wantIndices []int // indices into the [A, B, C] event slice
	}{
		{
			name:        "single contract filter matches A and C",
			filters:     []Filter{{ContractID: cid1Full}},
			wantIndices: []int{0, 2},
		},
		{
			name:        "single topic[0]=alpha matches A and B",
			filters:     []Filter{{Topics: [protocol.MaxTopicCount][]byte{alphaBytes}}},
			wantIndices: []int{0, 1},
		},
		{
			name: "contract=1 AND topic[0]=alpha matches A only",
			filters: []Filter{{
				ContractID: cid1Full,
				Topics:     [protocol.MaxTopicCount][]byte{alphaBytes},
			}},
			wantIndices: []int{0},
		},
		{
			name: "topic[1]=gamma constrains position 1 — B only",
			filters: []Filter{{
				Topics: [protocol.MaxTopicCount][]byte{nil, gammaBytes},
			}},
			wantIndices: []int{1},
		},
		{
			name: "topic[1]=beta requires position 1 — C lacks it",
			filters: []Filter{{
				Topics: [protocol.MaxTopicCount][]byte{nil, betaBytes},
			}},
			wantIndices: []int{0},
		},
		{
			name: "cross-clause OR: contract=2 OR topic[0]=delta — B and C",
			filters: []Filter{
				{ContractID: cid2Full},
				{Topics: [protocol.MaxTopicCount][]byte{deltaBytes}},
			},
			wantIndices: []int{1, 2},
		},
		{
			name:        "no clause matches",
			filters:     []Filter{{ContractID: make([]byte, 32) /* all-zero */}},
			wantIndices: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name+" [struct]", func(t *testing.T) {
			payloads := []events.Payload{clone(pStructA), clone(pStructB), clone(pStructC)}
			got, err := postFilter(payloads, tc.filters)
			require.NoError(t, err)
			assertIndices(t, got, []events.Payload{pStructA, pStructB, pStructC}, tc.wantIndices)
		})
		t.Run(tc.name+" [view]", func(t *testing.T) {
			payloads := []events.Payload{clone(pViewA), clone(pViewB), clone(pViewC)}
			got, err := postFilter(payloads, tc.filters)
			require.NoError(t, err)
			assertIndices(t, got, []events.Payload{pViewA, pViewB, pViewC}, tc.wantIndices)
		})
	}
}

// TestPostFilter_DiscardsCollisionInjection simulates a collision
// (or a buggy index): the candidate payload list contains an event
// whose actual ContractId / Topics don't match the filter. The
// post-filter must drop it. This is the defense-in-depth property the
// whole feature exists for — verify both paths.
func TestPostFilter_DiscardsCollisionInjection(t *testing.T) {
	// Filter wants contract=99 + topic[0]="real".
	wantCID := make([]byte, 32)
	wantCID[0] = 99
	filters := []Filter{{
		ContractID: wantCID,
		Topics:     [protocol.MaxTopicCount][]byte{mustMarshalScVal(t, "real")},
	}}

	// The "candidate" set returned by the index includes the legit
	// match AND a collision-injected event with the wrong contract
	// and topic.
	legitStruct, legitView := makeTestEvent(t, 99, "real")
	badStruct, badView := makeTestEvent(t, 7, "fake")

	t.Run("struct path", func(t *testing.T) {
		got, err := postFilter(
			[]events.Payload{clone(badStruct), clone(legitStruct)},
			filters,
		)
		require.NoError(t, err)
		require.Len(t, got, 1)
		require.NotNil(t, got[0].ContractEvent.ContractId)
		assert.Equal(t, uint8(99), got[0].ContractEvent.ContractId[0])
	})

	t.Run("view path", func(t *testing.T) {
		got, err := postFilter(
			[]events.Payload{clone(badView), clone(legitView)},
			filters,
		)
		require.NoError(t, err)
		require.Len(t, got, 1)
		// View path doesn't populate ContractEvent — verify via the
		// XDR view directly that the surviving payload is the legit
		// one (contract id byte = 99 means the contract-id-present
		// flag is followed by [99, 0, 0, ...]).
		require.NotNil(t, got[0].ContractEventBytes)
		// First 4 bytes after the optional-prefix flag are the
		// extension-point (0); not easy to extract without view
		// machinery — but ContractEventBytes equals the wire form
		// for contract 99 by construction, so byte-comparing the
		// payload bytes against the legitView source is sufficient.
		assert.Equal(t, legitView.ContractEventBytes, got[0].ContractEventBytes)
	})
}

// TestPostFilter_MissingTopicPositionFails covers the edge case where
// the event has fewer topics than the filter constrains. The struct
// path returns nil for the missing slot via the v0.Topics index
// check; the view path returns nil via the linear walk hitting end
// of the topics array. Both must drop the event.
func TestPostFilter_MissingTopicPositionFails(t *testing.T) {
	pStruct, pView := makeTestEvent(t, 1, "only") // single topic
	filters := []Filter{{
		Topics: [protocol.MaxTopicCount][]byte{
			nil, mustMarshalScVal(t, "missing"),
		},
	}}

	t.Run("struct", func(t *testing.T) {
		got, err := postFilter([]events.Payload{clone(pStruct)}, filters)
		require.NoError(t, err)
		assert.Empty(t, got)
	})
	t.Run("view", func(t *testing.T) {
		got, err := postFilter([]events.Payload{clone(pView)}, filters)
		require.NoError(t, err)
		assert.Empty(t, got)
	})
}

// TestPostFilter_NilContractIdEventVsContractFilter pins behavior
// when the event has no ContractId but the filter constrains it.
// Crafted by Marshal'ing a Payload whose ContractEvent has
// ContractId=nil.
func TestPostFilter_NilContractIdEventVsContractFilter(t *testing.T) {
	sym := xdr.ScSymbol("alpha")
	src := events.Payload{
		ContractEvent: xdr.ContractEvent{
			ContractId: nil,
			Type:       xdr.ContractEventTypeContract,
			Body: xdr.ContractEventBody{
				V: 0,
				V0: &xdr.ContractEventV0{
					Topics: []xdr.ScVal{{Type: xdr.ScValTypeScvSymbol, Sym: &sym}},
					Data:   xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym},
				},
			},
		},
	}
	wire, err := src.Marshal()
	require.NoError(t, err)
	var pStruct events.Payload
	require.NoError(t, pStruct.Unmarshal(wire))
	var pView events.Payload
	require.NoError(t, pView.UnmarshalView(append([]byte(nil), wire...)))

	wantCID := make([]byte, 32)
	wantCID[0] = 1
	filters := []Filter{{ContractID: wantCID}}

	t.Run("struct", func(t *testing.T) {
		got, err := postFilter([]events.Payload{clone(pStruct)}, filters)
		require.NoError(t, err)
		assert.Empty(t, got)
	})
	t.Run("view", func(t *testing.T) {
		got, err := postFilter([]events.Payload{clone(pView)}, filters)
		require.NoError(t, err)
		assert.Empty(t, got)
	})
}

// TestPostFilter_EmptyFiltersShortCircuits guards the documented
// invariant that Query never reaches postFilter with len(filters)==0
// (the match-all path handles it). The function itself must still
// return the input unchanged as a defensive measure.
func TestPostFilter_EmptyFiltersShortCircuits(t *testing.T) {
	pStruct, _ := makeTestEvent(t, 1, "x")
	in := []events.Payload{clone(pStruct), clone(pStruct)}
	got, err := postFilter(in, nil)
	require.NoError(t, err)
	assert.Len(t, got, 2)
}

// TestPostFilter_FourTopicEventAtCap covers an event with exactly
// protocol.MaxTopicCount topics — the boundary of the lazy walk in
// both paths.
func TestPostFilter_FourTopicEventAtCap(t *testing.T) {
	pStruct, pView := makeTestEvent(t, 1, "t0", "t1", "t2", "t3")
	filters := []Filter{{
		Topics: [protocol.MaxTopicCount][]byte{
			mustMarshalScVal(t, "t0"),
			mustMarshalScVal(t, "t1"),
			mustMarshalScVal(t, "t2"),
			mustMarshalScVal(t, "t3"),
		},
	}}

	t.Run("struct", func(t *testing.T) {
		got, err := postFilter([]events.Payload{pStruct}, filters)
		require.NoError(t, err)
		require.Len(t, got, 1)
	})
	t.Run("view", func(t *testing.T) {
		got, err := postFilter([]events.Payload{pView}, filters)
		require.NoError(t, err)
		require.Len(t, got, 1)
	})

	// Topic[3] mismatch — both paths must reject.
	filters[0].Topics[3] = mustMarshalScVal(t, "wrong")
	t.Run("struct mismatch at last position", func(t *testing.T) {
		got, err := postFilter([]events.Payload{pStruct}, filters)
		require.NoError(t, err)
		assert.Empty(t, got)
	})
	t.Run("view mismatch at last position", func(t *testing.T) {
		got, err := postFilter([]events.Payload{pView}, filters)
		require.NoError(t, err)
		assert.Empty(t, got)
	})
}

// TestPostFilter_NonV0BodyArm constructs a synthetic Payload whose
// Body.V is something other than 0 (no V0 topics array at all). The
// struct path takes hasV0=false; the view path takes
// collectTopicViewBytes's bodyVVal != 0 branch. Both must consistently
// fail any clause with topic constraints, and pass clauses with only
// ContractID constraints.
//
// Today's protocol only defines V=0, so we craft this by Marshaling a
// V=0 event and then flipping the body discriminant in the wire form
// — exercising the defensive branch without needing an SDK-side
// feature.
func TestPostFilter_NonV0BodyArm(t *testing.T) {
	// Start from a real V=0 event, then flip the body discriminant
	// in the wire form. The on-disk format is:
	//   header || ext(4 bytes) || cid(opt 4+32 bytes) || type(4) || body.V(4) || body.V0(...)
	// After the V=0 baseline, locate the body.V uint32 and write 1.
	pStruct, _ := makeTestEvent(t, 1, "alpha")
	wire, err := pStruct.Marshal()
	require.NoError(t, err)

	// Skip the events.Payload fixed-width header (61 bytes — see
	// payload.go format comment) plus the ContractEvent.Ext (4 bytes
	// for the V=0 ExtensionPoint), ContractId optional (4 bytes
	// presence flag + 32 bytes Hash since the fixture sets
	// ContractId), and Type (4 bytes enum). That puts us at body.V.
	const (
		payloadHeaderLen = 61
		ceExtLen         = 4
		optPresentLen    = 4
		hashLen          = 32
		ceTypeLen        = 4
	)
	bodyVOffset := payloadHeaderLen + ceExtLen + optPresentLen + hashLen + ceTypeLen
	require.Less(t, bodyVOffset+4, len(wire))
	wire[bodyVOffset+0] = 0
	wire[bodyVOffset+1] = 0
	wire[bodyVOffset+2] = 0
	wire[bodyVOffset+3] = 1 // flip V from 0 to 1 (big-endian)

	// View path: UnmarshalView aliases the modified bytes.
	var pView events.Payload
	require.NoError(t, pView.UnmarshalView(append([]byte(nil), wire...)))

	// Struct path: rebuild the ContractEvent in-memory with V != 0
	// directly (rather than fighting the SDK's strict Unmarshal).
	pStructV1 := pStruct
	pStructV1.ContractEvent.Body = xdr.ContractEventBody{V: 1}

	cid1Full := make([]byte, 32)
	cid1Full[0] = 1

	t.Run("contract-only filter matches", func(t *testing.T) {
		filters := []Filter{{ContractID: cid1Full}}
		gotStruct, err := postFilter([]events.Payload{pStructV1}, filters)
		require.NoError(t, err)
		assert.Len(t, gotStruct, 1, "struct: V!=0 still has ContractId for matching")

		gotView, err := postFilter([]events.Payload{pView}, filters)
		require.NoError(t, err)
		assert.Len(t, gotView, 1, "view: V!=0 still has ContractId for matching")
	})

	t.Run("topic-constrained filter mismatches", func(t *testing.T) {
		filters := []Filter{{
			Topics: [protocol.MaxTopicCount][]byte{
				mustMarshalScVal(t, "alpha"),
			},
		}}
		gotStruct, err := postFilter([]events.Payload{pStructV1}, filters)
		require.NoError(t, err)
		assert.Empty(t, gotStruct, "struct: V!=0 has no V0.Topics")

		gotView, err := postFilter([]events.Payload{pView}, filters)
		require.NoError(t, err)
		assert.Empty(t, gotView, "view: V!=0 has no V0.Topics")
	})
}

// TestPostFilter_MultiClauseSameTopicPosition exercises the
// per-position cache: two clauses constrain topic[0] with different
// values; only the matching value's clause should pass. The struct
// path resolves the topic once via topicResolved[0]; the view path
// resolves all constrained positions once via topicsWalked.
func TestPostFilter_MultiClauseSameTopicPosition(t *testing.T) {
	pStruct, pView := makeTestEvent(t, 1, "beta", "gamma")
	filters := []Filter{
		{Topics: [protocol.MaxTopicCount][]byte{mustMarshalScVal(t, "alpha")}}, // no match
		{Topics: [protocol.MaxTopicCount][]byte{mustMarshalScVal(t, "beta")}},  // matches
		{Topics: [protocol.MaxTopicCount][]byte{mustMarshalScVal(t, "delta")}}, // no match
	}

	t.Run("struct", func(t *testing.T) {
		got, err := postFilter([]events.Payload{pStruct}, filters)
		require.NoError(t, err)
		require.Len(t, got, 1)
	})
	t.Run("view", func(t *testing.T) {
		got, err := postFilter([]events.Payload{pView}, filters)
		require.NoError(t, err)
		require.Len(t, got, 1)
	})
}

// TestPostFilter_ValidateFiltersRejectsShortContractID pins the
// length validation added to Query at entry. A short ContractID is
// rejected at Query() before any storage I/O fires. postFilter
// itself doesn't run validateFilters (Query does), so this is an
// integration-level check — exercised through Query in
// query_test.go's existing fixtures would be ideal but here we
// directly verify the helper.
func TestPostFilter_ValidateFiltersRejectsShortContractID(t *testing.T) {
	short := []byte{1, 2, 3}
	err := validateFilters([]Filter{{ContractID: short}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be 0 or 32 bytes")

	// 32-byte and empty are both fine.
	require.NoError(t, validateFilters([]Filter{
		{ContractID: make([]byte, 32)},
		{ContractID: nil},
		{},
	}))
}

// makeTestEventScVal is a generalised makeTestEvent that takes
// already-constructed ScVals (any union arm — Symbol, Bytes, Vec,
// Map, Address, etc.). Returns struct + view shapes of the same
// wire bytes so cross-path equivalence tests can exercise any
// ScVal type.
func makeTestEventScVal(t *testing.T, cidByte byte, topics ...xdr.ScVal) (events.Payload, events.Payload) {
	t.Helper()
	var cid xdr.ContractId
	cid[0] = cidByte
	data := xdr.ScSymbol("data")
	src := events.Payload{
		TxHash: xdr.Hash{0xde, 0xad},
		ContractEvent: xdr.ContractEvent{
			ContractId: &cid,
			Type:       xdr.ContractEventTypeContract,
			Body: xdr.ContractEventBody{
				V: 0,
				V0: &xdr.ContractEventV0{
					Topics: topics,
					Data:   xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &data},
				},
			},
		},
	}
	wire, err := src.Marshal()
	require.NoError(t, err)

	var structForm, viewForm events.Payload
	require.NoError(t, structForm.Unmarshal(wire))
	wireForView := append([]byte(nil), wire...)
	require.NoError(t, viewForm.UnmarshalView(wireForView))
	return structForm, viewForm
}

// mustMarshal canonicalises an arbitrary ScVal so the test can use it
// as a Filter.Topics[i] value (same canonical form that the bench
// picker emits via TermsFor).
func mustMarshal(t *testing.T, v xdr.ScVal) []byte {
	t.Helper()
	b, err := v.MarshalBinary()
	require.NoError(t, err)
	return b
}

// TestPostFilter_StructAndViewPathAgreeAcrossScValTypes pins the
// load-bearing invariant for the lazy view-path code: for EVERY
// ScVal union arm we put through both paths, struct's MarshalBinary
// must produce the same canonical bytes as view's Raw() returns from
// the wire form. If the SDK ever drifts (e.g. canonical encoding
// changes between struct path and view path for Vec/Map), this test
// catches it — TestPostFilter_StructAndViewPathAgree only covered
// Symbol topics, which is a single union arm.
func TestPostFilter_StructAndViewPathAgreeAcrossScValTypes(t *testing.T) {
	// Symbol topic (control — already covered elsewhere).
	sym := xdr.ScSymbol("alpha")
	symV := xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym}

	// Bytes topic.
	bytesV := xdr.ScBytes{0x01, 0x02, 0x03, 0x04}
	scvBytes := xdr.ScVal{Type: xdr.ScValTypeScvBytes, Bytes: &bytesV}

	// String topic.
	scStr := xdr.ScString("hello")
	scvStr := xdr.ScVal{Type: xdr.ScValTypeScvString, Str: &scStr}

	// U64 topic (small scalar arm).
	u64 := xdr.Uint64(42)
	scvU64 := xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: &u64}

	// I128 topic (composite scalar — parts).
	i128 := xdr.Int128Parts{Hi: 1, Lo: 2}
	scvI128 := xdr.ScVal{Type: xdr.ScValTypeScvI128, I128: &i128}

	// Vec of two Symbols. ScVal.Vec is **ScVec (optional ptr-to-Vec).
	innerA := xdr.ScSymbol("a")
	innerB := xdr.ScSymbol("b")
	vec := xdr.ScVec{
		{Type: xdr.ScValTypeScvSymbol, Sym: &innerA},
		{Type: xdr.ScValTypeScvSymbol, Sym: &innerB},
	}
	vecPtr := &vec
	scvVec := xdr.ScVal{Type: xdr.ScValTypeScvVec, Vec: &vecPtr}

	// Map of one (Symbol→U64) pair. ScVal.Map is **ScMap.
	mapKey := xdr.ScSymbol("k")
	mapVal := xdr.Uint64(7)
	mapBody := xdr.ScMap{
		{
			Key: xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &mapKey},
			Val: xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: &mapVal},
		},
	}
	mapPtr := &mapBody
	scvMap := xdr.ScVal{Type: xdr.ScValTypeScvMap, Map: &mapPtr}

	// Address topic (account variant).
	var accID xdr.AccountId
	accID.Type = xdr.PublicKeyTypePublicKeyTypeEd25519
	ed := xdr.Uint256{1, 2, 3, 4}
	accID.Ed25519 = &ed
	scAddr := xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeAccount, AccountId: &accID}
	scvAddr := xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &scAddr}

	cases := []struct {
		name  string
		topic xdr.ScVal
	}{
		{"Symbol", symV},
		{"Bytes", scvBytes},
		{"String", scvStr},
		{"U64", scvU64},
		{"I128", scvI128},
		{"Vec", scvVec},
		{"Map", scvMap},
		{"Address", scvAddr},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pStruct, pView := makeTestEventScVal(t, 1, tc.topic)
			filterBytes := mustMarshal(t, tc.topic)
			filters := []Filter{{
				Topics: [protocol.MaxTopicCount][]byte{filterBytes},
			}}
			gotStruct, err := postFilter([]events.Payload{pStruct}, filters)
			require.NoError(t, err)
			require.Len(t, gotStruct, 1, "%s: struct path should match", tc.name)

			gotView, err := postFilter([]events.Payload{pView}, filters)
			require.NoError(t, err)
			require.Len(t, gotView, 1, "%s: view path should match", tc.name)

			// And a deliberately-mismatched filter must reject in both.
			otherSym := xdr.ScSymbol("nope")
			otherFilters := []Filter{{
				Topics: [protocol.MaxTopicCount][]byte{
					mustMarshal(t, xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &otherSym}),
				},
			}}
			gotStructMiss, err := postFilter([]events.Payload{pStruct}, otherFilters)
			require.NoError(t, err)
			assert.Empty(t, gotStructMiss, "%s: struct mismatch", tc.name)

			gotViewMiss, err := postFilter([]events.Payload{pView}, otherFilters)
			require.NoError(t, err)
			assert.Empty(t, gotViewMiss, "%s: view mismatch", tc.name)
		})
	}
}

// TestPostFilter_MixedModeDispatchPrefersView pins the dispatch
// invariant: when a Payload carries BOTH ContractEvent (struct) and
// ContractEventBytes (view), matchesAnyFilter routes to the view
// path via `len(p.ContractEventBytes) > 0`. If a future refactor
// flips the precedence (e.g. checks ContractEvent.ContractId != nil
// first), this test surfaces the regression.
//
// Construction: take a struct-only Payload that matches filter F1,
// graft on a ContractEventBytes for a DIFFERENT event that does not
// match F1. View path (bytes) → no match; struct path → would match.
// Observed behavior must be "no match" → view wins.
func TestPostFilter_MixedModeDispatchPrefersView(t *testing.T) {
	pStruct, _ := makeTestEvent(t, 1, "alpha") // event-A: cid=1, topic[0]="alpha"
	_, pViewB := makeTestEvent(t, 2, "beta")   // event-B: cid=2, topic[0]="beta"

	// Graft B's view-bytes onto A's struct. ContractEvent says
	// (cid=1, topic[0]="alpha"); ContractEventBytes encodes
	// (cid=2, topic[0]="beta").
	mixed := pStruct
	mixed.ContractEventBytes = pViewB.ContractEventBytes

	// Filter for cid=1 + topic[0]="alpha". Struct says match; view says no.
	cid1Full := make([]byte, 32)
	cid1Full[0] = 1
	filters := []Filter{{
		ContractID: cid1Full,
		Topics:     [protocol.MaxTopicCount][]byte{mustMarshalScVal(t, "alpha")},
	}}
	got, err := postFilter([]events.Payload{mixed}, filters)
	require.NoError(t, err)
	assert.Empty(t, got, "view path must take precedence — ContractEventBytes wins")

	// And the dual: filter for B's content should match because view
	// resolves to B's bytes.
	cid2Full := make([]byte, 32)
	cid2Full[0] = 2
	filtersB := []Filter{{
		ContractID: cid2Full,
		Topics:     [protocol.MaxTopicCount][]byte{mustMarshalScVal(t, "beta")},
	}}
	got, err = postFilter([]events.Payload{mixed}, filtersB)
	require.NoError(t, err)
	assert.Len(t, got, 1, "view path matches B's wire content even with A's struct attached")
}

// FuzzPostFilterStructVsView throws random topic-symbol vectors at
// both paths and asserts result equality. Catches any future SDK
// divergence between struct MarshalBinary and view .Raw() canonical
// encoding that a fixed-input test would miss.
//
// Input encoding: the fuzz []byte is partitioned into up to 4 topic
// values; each value's bytes form a Symbol's text. Symbol-only keeps
// the input simple but exercises the full lazy / dispatch / cache
// machinery on both paths.
func FuzzPostFilterStructVsView(f *testing.F) {
	// Seed corpus: a few canonical inputs.
	f.Add([]byte("alpha"), []byte("beta"), []byte("gamma"), []byte("delta"))
	f.Add([]byte("x"), []byte(""), []byte(""), []byte(""))
	f.Add([]byte(""), []byte(""), []byte(""), []byte(""))

	f.Fuzz(func(t *testing.T, t0, t1, t2, t3 []byte) {
		// Sanitize: Symbol allows only [a-zA-Z0-9_] up to 32 bytes —
		// reject malformed inputs by skipping rather than failing.
		topics := []xdr.ScVal{}
		for _, b := range [][]byte{t0, t1, t2, t3} {
			if !isValidScSymbol(b) {
				continue
			}
			s := xdr.ScSymbol(b)
			topics = append(topics, xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &s})
		}
		if len(topics) == 0 {
			t.Skip()
		}

		pStruct, pView := makeTestEventScVal(t, 1, topics...)

		// Filter matching topic[0] (first non-empty). Sanity check.
		filter := Filter{}
		filter.Topics[0] = mustMarshal(t, topics[0])
		filters := []Filter{filter}

		gotStruct, errS := postFilter([]events.Payload{pStruct}, filters)
		gotView, errV := postFilter([]events.Payload{pView}, filters)
		if (errS == nil) != (errV == nil) {
			t.Fatalf("error divergence: struct=%v view=%v", errS, errV)
		}
		if errS != nil {
			return
		}
		if len(gotStruct) != len(gotView) {
			t.Fatalf("len divergence: struct=%d view=%d", len(gotStruct), len(gotView))
		}
	})
}

// isValidScSymbol mirrors stellar-core's symbol validation (32-byte
// max, alnum + underscore). Fuzz inputs that violate cause Marshal
// to error long before postFilter sees them — skip rather than
// failing.
func isValidScSymbol(b []byte) bool {
	if len(b) == 0 || len(b) > 32 {
		return false
	}
	for _, c := range b {
		alnum := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
		if !alnum && c != '_' {
			return false
		}
	}
	return true
}

// clone returns a shallow copy of p. ContractEventBytes and Terms
// slice headers are copied but their backing arrays are aliased —
// safe in postFilter tests because the function never mutates those
// bytes.
func clone(p events.Payload) events.Payload { return p }

func assertIndices(t *testing.T, got []events.Payload, all []events.Payload, want []int) {
	t.Helper()
	require.Len(t, got, len(want))
	for i, idx := range want {
		// Match by ContractEventBytes (view) or via Marshal (struct);
		// both compare canonical XDR bytes for the same logical event.
		wantBytes := all[idx].ContractEventBytes
		if wantBytes == nil {
			b, err := all[idx].ContractEvent.MarshalBinary()
			require.NoError(t, err)
			wantBytes = b
		}
		gotBytes := got[i].ContractEventBytes
		if gotBytes == nil {
			b, err := got[i].ContractEvent.MarshalBinary()
			require.NoError(t, err)
			gotBytes = b
		}
		assert.Equal(t, wantBytes, gotBytes, "result[%d] mismatch", i)
	}
}
