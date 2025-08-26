package infrastructure

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestArrayWriter(t *testing.T) {
	writer := arrayWriter{}
	write := func(s string) {
		_, err := writer.Write([]byte(s))
		require.NoError(t, err)
	}

	write("hello\nworld")
	require.Len(t, writer.Lines, 2)
	writer.Reset()

	write("hello\nworld")
	write("!\ngoodbye")
	require.Len(t, writer.Lines, 3)
	writer.Reset()

	write("hello\nworld\n")
	write("goodbye\n")
	require.Len(t, writer.Lines, 3)
	writer.Reset()

	write("hello\nworld")
	write("!\ngoodbye")
	require.Len(t, writer.Lines, 3)
	writer.Reset()
}
