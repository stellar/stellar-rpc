package ledger

import (
	"iter"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

// TestWholeLedgerReadsCheckTheHeaderSequence pins the assertion on BOTH tiers
// and on both whole-ledger reads. The fixture is a ledger stored under the
// wrong sequence — the cold tier's characteristic failure, since it resolves a
// sequence positionally and would otherwise answer with a neighbor, and a
// shape the hot tier can reach too if anything ever writes the wrong value
// under a key.
//
// Both reads must refuse it and name both sequences, so the error says which
// ledger was asked for and which one the bytes are.
func TestWholeLedgerReadsCheckTheHeaderSequence(t *testing.T) {
	const asked, stored = uint32(4_100), uint32(4_101)
	mismatched := fillerLedger(stored, 0)

	hot := openTestHotStore(t)
	require.NoError(t, addLedgers(hot, Entry{Seq: asked, Bytes: mismatched}))

	coldPath := filepath.Join(t.TempDir(), "mismatched.pack")
	w, err := NewColdWriter(coldPath, asked, ColdWriterOptions{})
	require.NoError(t, err)
	require.NoError(t, w.AppendLedger(asked, mismatched))
	require.NoError(t, w.Commit())
	require.NoError(t, w.Close())

	for name, tier := range map[string]wholeLedgerReader{
		"hot":  hot,
		"cold": newTestColdReader(t, coldPath),
	} {
		t.Run(name, func(t *testing.T) {
			called := false
			err := tier.WithLedger(asked, func([]byte) error { called = true; return nil })
			require.ErrorIs(t, err, stores.ErrCorrupt)
			assert.ErrorContains(t, err, "4100", "the error must name the sequence asked for")
			assert.ErrorContains(t, err, "4101", "the error must name the sequence stored")
			assert.False(t, called, "a mismatched ledger must never reach the caller")

			seen := 0
			var iterErr error
			for _, ierr := range tier.IterateLedgers(asked, asked) {
				if ierr != nil {
					iterErr = ierr
					break
				}
				seen++
			}
			require.ErrorIs(t, iterErr, stores.ErrCorrupt)
			assert.ErrorContains(t, iterErr, "4101")
			assert.Zero(t, seen, "the walk must stop rather than yield the wrong ledger")
		})
	}
}

// TestWholeLedgerReadsAcceptTheMatchingHeader is the control: the same fixture
// stored under its own sequence reads back on both tiers, so the check refuses
// mismatches rather than ledgers.
func TestWholeLedgerReadsAcceptTheMatchingHeader(t *testing.T) {
	const seq = uint32(4_200)
	raw := fillerLedger(seq, 0)

	hot := openTestHotStore(t)
	require.NoError(t, addLedgers(hot, Entry{Seq: seq, Bytes: raw}))
	require.NoError(t, hot.WithLedger(seq, func(got []byte) error {
		assert.Equal(t, raw, got)
		return nil
	}))

	coldPath := filepath.Join(t.TempDir(), "matching.pack")
	w, err := NewColdWriter(coldPath, seq, ColdWriterOptions{})
	require.NoError(t, err)
	require.NoError(t, w.AppendLedger(seq, raw))
	require.NoError(t, w.Commit())
	require.NoError(t, w.Close())
	require.NoError(t, newTestColdReader(t, coldPath).WithLedger(seq, func(got []byte) error {
		assert.Equal(t, raw, got)
		return nil
	}))
}

// wholeLedgerReader is the pair of reads the header check covers, which both
// tiers implement identically.
type wholeLedgerReader interface {
	WithLedger(seq uint32, fn func(raw []byte) error) error
	IterateLedgers(start, end uint32) iter.Seq2[Entry, error]
}
