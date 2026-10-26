package ledger

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
)

// The freeze copies only a complete chunk with no ledger missing.
func TestFreezeColdFromStore_RefusesAGapAndAShortChunk(t *testing.T) {
	c := chunk.ID(0)
	for _, tc := range []struct {
		seqs []uint32
		want string
	}{
		{seqs: []uint32{c.FirstLedger(), c.FirstLedger() + 2}, want: "expected seq"},
		{seqs: []uint32{c.FirstLedger(), c.FirstLedger() + 1}, want: "want 10000"},
	} {
		h, store := openTestHotStoreAt(t, t.TempDir())
		for _, seq := range tc.seqs {
			require.NoError(t, addLedgers(h, Entry{Seq: seq, Bytes: []byte("v")}))
		}
		path := filepath.Join(t.TempDir(), "x.pack")
		_, err := FreezeColdFromStore(context.Background(), c, store, path, ColdWriterOptions{})
		require.ErrorContains(t, err, tc.want)
	}
}
