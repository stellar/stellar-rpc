package events

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// termsViaScan is the golden sweep's INDEPENDENT reference derivation: the
// same cid/body navigation as AppendTerms, but with the topics walked
// through the generated Scan() iterator over EVERY element — not just the
// indexed prefix — so its whole-vec validation matches the production
// Len()+Raw() walk (Raw on the vec sizes every element) while sharing none
// of its offset arithmetic. Cur views are exact-extent, so MustRaw is a
// slice operation and the hashed bytes are exactly the topic's wire XDR.
func termsViaScan(eventBytes []byte) ([]TermKey, error) {
	ev := xdr.NewContractEventView(eventBytes)
	var keys []TermKey
	cidOpt, err := ev.ContractId()
	if err != nil {
		return nil, err
	}
	cidView, present, err := cidOpt.Unwrap()
	if err != nil {
		return nil, err
	}
	if present {
		cid, cerr := cidView.Value()
		if cerr != nil {
			return nil, cerr
		}
		keys = append(keys, ComputeTermKey(cid[:], FieldContractID))
	}
	body, err := ev.Body()
	if err != nil {
		return nil, err
	}
	bodyV, err := body.V()
	if err != nil {
		return nil, err
	}
	if bodyV != 0 {
		return nil, fmt.Errorf("unsupported ContractEvent body version %d", bodyV)
	}
	v0, err := body.ArmV0()
	if err != nil {
		return nil, err
	}
	topics, err := v0.Topics()
	if err != nil {
		return nil, err
	}
	sc := topics.Scan()
	for i := 0; sc.Next(); i++ {
		if i < protocol.MaxTopicCount {
			keys = append(keys, ComputeTermKey(sc.Cur().MustRaw(), topicField(i)))
		}
	}
	if serr := sc.Err(); serr != nil {
		return nil, serr
	}
	return keys, nil
}

// TestAppendTerms_GoldenAgainstScanReference is the Len()+Raw() walk's
// golden gate: AppendTerms (and with it TermsForBytes, which delegates)
// must be observably identical to the Scan()-based reference —
// byte-identical TermKeys on
// every accept, reject exactly when the reference rejects. The sweep
// truncates each fixture's raw XDR at EVERY length, which covers the
// mandated cases by construction: truncation inside an in-cap topic,
// truncation inside an over-cap topic (the overCap fixture's topics 4 and 5
// — Raw sizes every element, so over-cap truncation must still reject), and
// harmless truncation past the topics array (both paths accept).
func TestAppendTerms_GoldenAgainstScanReference(t *testing.T) {
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
				want, wantErr := termsViaScan(prefix)
				got, gotErr := AppendTerms(nil, prefix)
				if wantErr != nil {
					sawReject = true
					require.Error(t, gotErr, "prefix of %d bytes: reference rejected, production must too", n)
					continue
				}
				require.NoError(t, gotErr, "prefix of %d bytes: reference accepted, production must too", n)
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
