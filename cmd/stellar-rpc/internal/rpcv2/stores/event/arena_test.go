package event

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

// A returned copy never moves or changes, however much is copied after it.
func TestByteArenaCopiesAreStable(t *testing.T) {
	var a byteArena
	src := make([]byte, 300)
	got := make([][]byte, 0, 3000)
	want := make([][]byte, 0, 3000)
	for i := range 3000 { // ~900KB total: crosses many 64KB chunks
		for j := range src {
			src[j] = byte(i + j)
		}
		c := a.copy(src)
		got = append(got, c)
		want = append(want, bytes.Clone(src))
	}
	huge := bytes.Repeat([]byte{0xAB}, 3*arenaChunkSize)
	hugeCopy := a.copy(huge)
	huge[0] = 0xCD // mutate the source; the copy must not see it
	require.Equal(t, byte(0xAB), hugeCopy[0])
	require.Len(t, hugeCopy, 3*arenaChunkSize)
	for i := range got {
		require.True(t, bytes.Equal(got[i], want[i]), "copy %d changed", i)
	}
	// Appending to a returned copy must not scribble into the arena.
	c := a.copy([]byte{1, 2, 3})
	next := a.copy([]byte{9, 9, 9})
	_ = append(c, 7) // the append must copy, not extend in place
	require.Equal(t, []byte{9, 9, 9}, next)
}
