package event

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// TestAppendTerms_GoldenAgainstTermsForBytes is the Count()+Raw() walk's
// golden gate: AppendTerms must be observably identical to the
// TopicsView.All path TermsForBytes still uses — byte-identical TermKeys on
// every accept, reject exactly when the old path rejects. The sweep
// truncates each fixture's raw XDR at EVERY length, which covers the
// mandated cases by construction: truncation inside an in-cap topic,
// truncation inside an over-cap topic (the overCap fixture's topics 4 and 5
// — Raw sizes every element, so over-cap truncation must still reject), and
// harmless truncation past the topics array (both paths accept).
func TestAppendTerms_GoldenAgainstTermsForBytes(t *testing.T) {
	var cid xdr.ContractId
	cid[0], cid[1] = 0xab, 0xcd
	fixtures := map[string]xdr.ContractEvent{
		"cidNoTopics":  symTopicEvent(&cid),
		"cidOneTopic":  symTopicEvent(&cid, "transfer"),
		"noCidTopics":  symTopicEvent(nil, "a", "b"),
		"cidMaxTopics": symTopicEvent(&cid, "t0", "t1", "t2", "t3"),
		"cidOverCap":   symTopicEvent(&cid, "t0", "t1", "t2", "t3", "over", "flow"),
	}
	for name, ev := range fixtures {
		t.Run(name, func(t *testing.T) {
			raw := marshaledEvent(t, ev)
			sawReject := false
			for n := 0; n <= len(raw); n++ {
				prefix := raw[:n]
				want, wantErr := TermsForBytes(prefix)
				got, gotErr := AppendTerms(nil, prefix)
				if wantErr != nil {
					sawReject = true
					require.Error(t, gotErr, "prefix of %d bytes: old path rejected, new must too", n)
					continue
				}
				require.NoError(t, gotErr, "prefix of %d bytes: old path accepted, new must too", n)
				assert.Equal(t, want, got, "keys diverged at prefix of %d bytes", n)
			}
			require.True(t, sawReject, "sweep must hit at least one truncation reject")
		})
	}
}

// TestAppendTerms_UnsupportedBodyVersionHardFails mirrors the TermsForBytes
// pin: a future ContractEvent body version is a hard error on the arena
// path too — never a silent contractID-only index.
func TestAppendTerms_UnsupportedBodyVersionHardFails(t *testing.T) {
	// Layout without a contract ID:
	// ext.V (4) || contractId flag (4, =0) || type (4) || body.V (4).
	raw := marshaledEvent(t, symTopicEvent(nil, "transfer"))
	binary.BigEndian.PutUint32(raw[12:16], 1)

	_, err := AppendTerms(nil, raw)
	require.ErrorContains(t, err, "unsupported ContractEvent body version 1")
}

// TestAppendTerms_ArenaAppendAndBound pins the arena contract: AppendTerms
// APPENDS (existing dst content untouched, backing array reused within
// capacity) and contributes at most MaxTermsPerEvent keys per event — the
// protocol bound (contract ID + protocol.MaxTopicCount topics) per-ledger
// arenas are sized around.
func TestAppendTerms_ArenaAppendAndBound(t *testing.T) {
	var cid xdr.ContractId
	cid[0] = 0x77
	raw := marshaledEvent(t, symTopicEvent(&cid, "t0", "t1", "t2", "t3", "extra"))

	dst := make([]TermKey, 0, 2*MaxTermsPerEvent)
	dst, err := AppendTerms(dst, raw)
	require.NoError(t, err)
	require.Len(t, dst, MaxTermsPerEvent,
		"contract ID + MaxTopicCount topics is the per-event maximum (extras not indexed)")
	first := append([]TermKey(nil), dst...)

	// A second event appends AFTER the first's keys, in the same array.
	base := &dst[0]
	dst, err = AppendTerms(dst, raw)
	require.NoError(t, err)
	require.Len(t, dst, 2*MaxTermsPerEvent)
	assert.Equal(t, first, dst[:MaxTermsPerEvent], "existing arena content must be untouched")
	assert.Equal(t, first, dst[MaxTermsPerEvent:], "same event must yield the same keys")
	assert.Same(t, base, &dst[0], "within capacity the arena must not reallocate")
}
