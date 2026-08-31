package event

// Byte-level goldens for the term-key derivation. The term keys are on-disk
// format: they are the identities every frozen index.pack/index.hash is built
// over, so any change to the hash function, the field-byte prefix, or a
// field's value encoding silently invalidates every existing index. These
// fixtures make such a change fail CI with "bytes changed: bump
// TermSchemaVersion (and the artifact format) or revert" instead of relying
// on review to notice. (The shared blinding primitives are pinned at their
// own level, in stores/blind_test.go.)

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

func TestComputeTermKey_Golden(t *testing.T) {
	val := []byte("stellar-rpc-term-golden")
	want := map[Field]string{
		FieldContractID: "68828a4d63340ec7a182c7bacec54f0a",
		FieldTopic0:     "fe2646f2447419a4c3b5899d18f19517",
		FieldTopic1:     "5d3c6660724f4267f75f0314a8bd99bc",
		FieldTopic2:     "924913135dfd660d50dd75352d8b4bb8",
		FieldTopic3:     "9a981d29f52a36df9c3d9fa2d19ffc91",
		FieldEventType:  "b8cba0b1d3c82ec9ba18a11d780d0d23",
		FieldTopicCount: "8188dbfd6145c54e6cb442dc02b3c324",
	}
	require.Len(t, want, len(allFields), "extend the golden map when adding a Field")
	for _, f := range allFields {
		w, pinned := want[f]
		require.True(t, pinned, "field %d has no golden pin; add one when adding a Field", f)
		assert.Equal(t, w, hexKey(t, ComputeTermKey(val, f)), "field %d", f)
	}
}

func TestFieldTermKeys_Golden(t *testing.T) {
	assert.Equal(t, "092967d65ec25b057c6a54e627dec4e6",
		hexKey(t, EventTypeTermKey(xdr.ContractEventTypeContract)))

	wantCounts := []string{
		"5a21d2e3bd688e548ac939e4babaeb9b",
		"8e389795ce9351bb0abbe6c00fff6052",
		"a68396e0abe112008816c89412c9113b",
		"291796ea36f7a80a1d7515faf2a22ec7",
		"db3d5df76fa2f6cd611dde2b96f16401",
		"70c2b104db4b2ba115eeb4954daac675", // the overflow bucket
	}
	for n, w := range wantCounts {
		assert.Equal(t, w, hexKey(t, TopicCountTermKey(n)), "topic count %d", n)
	}
	assert.Equal(t, TopicCountTermKey(topicCountOverflowBucket), TopicCountTermKey(99),
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
// not just the hash and field prefix (TestComputeTermKey_Golden's job) but
// the value encoding of every field TermsForBytes extracts, so a change to
// how a contract ID or topic becomes hash input cannot pass unnoticed.
// Cross-anchors: key 0 equals the eventType(contract) golden and key 2 the
// topicCount(4) golden above.
func TestTermsForBytes_Golden(t *testing.T) {
	keys, err := TermsForBytes(goldenContractEventBytes(t))
	require.NoError(t, err)
	want := []string{
		"092967d65ec25b057c6a54e627dec4e6", // event type (contract)
		"f96521b095e9249b0af1546bfbd9a80f", // contract ID 0..31
		"db3d5df76fa2f6cd611dde2b96f16401", // topic count 4
		"8b9390866f0062b43022a0896fe6b606", // topic 0: symbol "transfer"
		"de4348720f97c94df5621c5cc5304a3d", // topic 1: u32 7
		"9ea0933498a23b8f6c3d867631e945c8", // topic 2: symbol "to"
		"dacc6ffd833f6932052ef2cd55f75844", // topic 3: u32 9
	}
	require.Len(t, keys, len(want))
	for i, k := range keys {
		assert.Equal(t, want[i], hex.EncodeToString(k[:]), "key %d", i)
	}
}
