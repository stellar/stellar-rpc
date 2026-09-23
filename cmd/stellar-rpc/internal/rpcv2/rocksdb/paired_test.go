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
