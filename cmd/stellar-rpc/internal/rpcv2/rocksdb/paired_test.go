package rocksdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetPinnedPair_LendsBothValuesAtOnce pins the reason the pair read
// exists: two values from two families, both live inside one callback, under
// one lock — the shape a nested GetPinned cannot have.
func TestGetPinnedPair_LendsBothValuesAtOnce(t *testing.T) {
	s := openTestStore(t, []string{"a", "b"})
	require.NoError(t, s.Put("a", []byte("k"), []byte("value-a")))
	require.NoError(t, s.Put("b", []byte("k"), []byte("value-b")))

	called := false
	foundA, foundB, err := s.GetPinnedPair("a", []byte("k"), "b", []byte("k"), func(a, b []byte) error {
		called = true
		assert.Equal(t, "value-a", string(a))
		assert.Equal(t, "value-b", string(b))
		return nil
	})
	require.NoError(t, err)
	assert.True(t, foundA)
	assert.True(t, foundB)
	assert.True(t, called)
}

// TestGetPinnedPair_MissesNameWhichSideIsGone pins that fn runs only with both
// values in hand, and the caller learns which key was missing.
func TestGetPinnedPair_MissesNameWhichSideIsGone(t *testing.T) {
	s := openTestStore(t, []string{"a", "b"})
	require.NoError(t, s.Put("a", []byte("k"), []byte("value-a")))

	never := func([]byte, []byte) error {
		t.Fatal("fn must not run when a key is missing")
		return nil
	}
	foundA, foundB, err := s.GetPinnedPair("a", []byte("k"), "b", []byte("k"), never)
	require.NoError(t, err)
	assert.True(t, foundA)
	assert.False(t, foundB)

	foundA, foundB, err = s.GetPinnedPair("a", []byte("nope"), "b", []byte("k"), never)
	require.NoError(t, err)
	assert.False(t, foundA)
	assert.False(t, foundB)
}

// TestGetPinnedPair_CarriesTheCallbackError pins that fn's error reaches the
// caller rather than being swallowed by the pinned read.
func TestGetPinnedPair_CarriesTheCallbackError(t *testing.T) {
	s := openTestStore(t, []string{"a", "b"})
	require.NoError(t, s.Put("a", []byte("k"), []byte("x")))
	require.NoError(t, s.Put("b", []byte("k"), []byte("y")))

	sentinel := assertAnError{}
	_, _, err := s.GetPinnedPair("a", []byte("k"), "b", []byte("k"), func([]byte, []byte) error {
		return sentinel
	})
	require.ErrorIs(t, err, sentinel)
}

type assertAnError struct{}

func (assertAnError) Error() string { return "callback said no" }

// TestIterateRangePaired_PairsByKey pins the lockstep walk: every primary key
// in range is yielded with the secondary family's value under the same key,
// and nil where the secondary has none.
func TestIterateRangePaired_PairsByKey(t *testing.T) {
	s := openTestStore(t, []string{"main", "side"})
	for i := range uint32(6) {
		require.NoError(t, s.Put("main", EncodeUint32(i), []byte{byte(i)}))
	}
	// The secondary holds a sparse subset, including a key outside the range.
	for _, i := range []uint32{1, 3, 4, 9} {
		require.NoError(t, s.Put("side", EncodeUint32(i), []byte{byte(i), 0xAA}))
	}

	got := map[uint32]string{}
	for e, err := range s.IterateRangePaired("main", "side", EncodeUint32(1), EncodeUint32(4)) {
		require.NoError(t, err)
		got[DecodeUint32(e.Key)] = string(e.Paired)
	}
	assert.Equal(t, map[uint32]string{
		1: string([]byte{1, 0xAA}),
		2: "",
		3: string([]byte{3, 0xAA}),
		4: string([]byte{4, 0xAA}),
	}, got)
}

// TestIterateRangePaired_MissingSecondaryFamilyIsAnError pins the answer a
// walk gives for a secondary family the store was not opened with: it fails,
// before any entry. Pairing every key with nil would say "the family holds
// nothing for these keys" — the same shape as a real answer — and a caller
// that acts on it (the freeze writes each ledger with whatever it was paired
// with) would silently produce an artifact missing everything that family
// holds.
func TestIterateRangePaired_MissingSecondaryFamilyIsAnError(t *testing.T) {
	s := openTestStore(t, []string{"main"})
	for i := range uint32(3) {
		require.NoError(t, s.Put("main", EncodeUint32(i), []byte{byte(i)}))
	}

	seen, sawErr := 0, error(nil)
	for e, err := range s.IterateRangePaired("main", "absent", nil, nil) {
		if err != nil {
			sawErr = err
			continue
		}
		_ = e
		seen++
	}
	require.ErrorIs(t, sawErr, ErrCFNotFound)
	assert.ErrorContains(t, sawErr, "absent")
	assert.Zero(t, seen, "no entry may be yielded for a walk that cannot pair")

	// The same walk over a family the store DOES have still pairs, so the
	// refusal is about the family's absence and nothing else.
	require.NoError(t, s.Put("main", EncodeUint32(9), []byte{9}))
	for _, err := range s.IterateRangePaired("main", "main", nil, nil) {
		require.NoError(t, err)
	}
}
