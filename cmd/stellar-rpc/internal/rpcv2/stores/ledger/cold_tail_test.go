package ledger

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/packfile"
)

// TestColdReader_OpensInOneReadWhenTheTailFits pins both sides of the tuned
// tail: a pack whose index and app data fit inside coldTailRead opens with the
// single speculative read, and one whose do not opens anyway, paying the
// follow-up read the packfile falls back to. The first is the reason to tune
// the size down; the second is why tuning it down is safe.
func TestColdReader_OpensInOneReadWhenTheTailFits(t *testing.T) {
	// Small pack: a handful of records, so the tail is a few dozen bytes.
	small := filepath.Join(t.TempDir(), "small.pack")
	w, err := NewColdWriter(small, 1, ColdWriterOptions{})
	require.NoError(t, err)
	for seq := uint32(1); seq <= 8; seq++ {
		require.NoError(t, w.AppendLedger(seq, fillerLedger(seq, 0)))
	}
	require.NoError(t, w.Commit())
	require.NoError(t, w.Close())

	reads, refills := packfile.TailReads(), packfile.TailRefills()
	r := newTestColdReader(t, small)
	h, err := r.init()
	require.NoError(t, err)
	require.EqualValues(t, 8, h.lastSeq)
	assert.Equal(t, reads+1, packfile.TailReads())
	assert.Equal(t, refills, packfile.TailRefills(),
		"a tail that covers the index must open in one read")
	assert.Less(t, tailSizeOf(t, r), int64(coldTailRead),
		"premise: this pack's tail really does fit")

	// The same pack read with a tail smaller than its own index: the open has
	// to go back for the index region, and still succeeds.
	tiny, err := openColdReaderWithTail(small, 64)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tiny.Close() })
	reads, refills = packfile.TailReads(), packfile.TailRefills()
	th, err := tiny.init()
	require.NoError(t, err)
	assert.Equal(t, h, th, "the second read must produce the same header")
	assert.Equal(t, reads+1, packfile.TailReads())
	assert.Equal(t, refills+1, packfile.TailRefills(),
		"a tail shorter than the index must be followed by the index read")
}

// tailSizeOf is the pack's index + app data + trailer, which is what the
// speculative tail read is trying to cover.
func tailSizeOf(t *testing.T, r *ColdReader) int64 {
	t.Helper()
	tr, err := r.r.Trailer()
	require.NoError(t, err)
	return int64(tr.IndexSize) + int64(tr.AppDataSize) + packfile.TrailerSize
}
