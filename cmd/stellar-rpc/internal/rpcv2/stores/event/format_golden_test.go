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
