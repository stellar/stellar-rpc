package event

// Byte-level goldens for the term keys under the fixed test secret. The keys
// are on-disk format: a change here means bump TermSchemaVersion, or revert.

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/xdr"
)

func hexKey(t *testing.T, k TermKey) string {
	t.Helper()
	return hex.EncodeToString(k[:])
}

func TestTermKey_Golden(t *testing.T) {
	val := []byte("stellar-rpc-term-golden")
	want := map[Field]string{
		FieldContractID: "25d1fc0d0be846045b47c7e4e1cb7130",
		FieldTopic0:     "d4ae2f1e1317b65b14e7117899ffbbd3",
		FieldTopic1:     "db67c52c51441a8df08e7c4ca38af2ff",
		FieldTopic2:     "c9cec2fb9cb2a6fd2ac34334697a6f9d",
		FieldTopic3:     "488208b25e5badb441083b73dfd3b184",
		FieldEventType:  "e11fa5b83906e7a157297d990d1d34a1",
		FieldTopicCount: "104581f4da1b727bf11507a0aef29287",
	}
	require.Len(t, want, len(allFields), "extend the golden map when adding a Field")
	for _, f := range allFields {
		w, pinned := want[f]
		require.True(t, pinned, "field %d has no golden pin; add one when adding a Field", f)
		assert.Equal(t, w, hexKey(t, NewTerm(f, val).Key(testIndexSecret)), "field %d", f)
	}
}

func TestFieldTermKeys_Golden(t *testing.T) {
	assert.Equal(t, "79be05c26f39249bb53e27ad703e2547",
		hexKey(t, EventTypeTerm(xdr.ContractEventTypeContract).Key(testIndexSecret)))

	wantCounts := []string{
		"3a9d24f15cb6d30e39d854b4c1cd0183",
		"51f37bb33de8a2be7914b5f7d36acc5f",
		"9cfc15110981972872a04fcaa48b21b6",
		"d6f277a0ddf565a8832c8112915adf1f",
		"4d0a53430a32c3b298e94759486add79",
		"541d1a45bee7502a70bf40bdb606bdfd", // the overflow bucket
	}
	for n, w := range wantCounts {
		assert.Equal(t, w, hexKey(t, TopicCountTerm(n).Key(testIndexSecret)), "topic count %d", n)
	}
	assert.Equal(t, TopicCountTerm(topicCountOverflowBucket), TopicCountTerm(99),
		"counts past the overflow bucket clamp into it")
}

// goldenContractEventBytes builds the fixed marshaled ContractEvent the
// TermsForBytes golden runs over: a contract ID of bytes 0..31 and four
// topics exercising every indexed topic position.
func goldenContractEventBytes(t *testing.T) []byte {
	t.Helper()
	var cid xdr.ContractId
	for i := range cid {
		cid[i] = byte(i)
	}
	sym0, sym2 := xdr.ScSymbol("transfer"), xdr.ScSymbol("to")
	u1, u3 := xdr.Uint32(7), xdr.Uint32(9)
	ev := xdr.ContractEvent{
		ContractId: &cid,
		Type:       xdr.ContractEventTypeContract,
		Body: xdr.ContractEventBody{
			V: 0,
			V0: &xdr.ContractEventV0{
				Topics: []xdr.ScVal{
					{Type: xdr.ScValTypeScvSymbol, Sym: &sym0},
					{Type: xdr.ScValTypeScvU32, U32: &u1},
					{Type: xdr.ScValTypeScvSymbol, Sym: &sym2},
					{Type: xdr.ScValTypeScvU32, U32: &u3},
				},
				Data: xdr.ScVal{Type: xdr.ScValTypeScvU32, U32: &u1},
			},
		},
	}
	b, err := ev.MarshalBinary()
	require.NoError(t, err)
	return b
}

// TestTermsForBytes_Golden pins the full derivation from a marshaled event:
// not just the hash and field prefix (TestTermKey_Golden's job) but
// the value encoding of every field TermsForBytes extracts, so a change to
// how a contract ID or topic becomes hash input cannot pass unnoticed.
// Cross-anchors: key 0 equals the eventType(contract) golden and key 2 the
// topicCount(4) golden above.
func TestTermsForBytes_Golden(t *testing.T) {
	terms, err := TermsForBytes(goldenContractEventBytes(t))
	require.NoError(t, err)
	want := []string{
		"79be05c26f39249bb53e27ad703e2547", // event type (contract)
		"3e338c99eca417d8bedb899eda51b346", // contract ID 0..31
		"4d0a53430a32c3b298e94759486add79", // topic count 4
		"4d3a94c9d897819e375e2ce31b46173c", // topic 0: symbol "transfer"
		"3db0bc0cf1012b296c09670dd89f461a", // topic 1: u32 7
		"6ac66407822877be0bff3d89b4c9ffbd", // topic 2: symbol "to"
		"65515cacf29c2fa9ec988f7a75e28850", // topic 3: u32 9
	}
	require.Len(t, terms, len(want))
	for i, term := range terms {
		assert.Equal(t, want[i], hexKey(t, term.Key(testIndexSecret)), "key %d", i)
	}
}
