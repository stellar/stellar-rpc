package ledger

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
)

// collectStream drains a LedgerStream over r, copying each borrowed slice (valid
// only until the next step) so the returned bytes are owned by the caller.
func collectStream(t *testing.T, s ledgerbackend.LedgerStream, r ledgerbackend.Range) [][]byte {
	t.Helper()
	var got [][]byte
	for raw, err := range s.RawLedgers(context.Background(), r) {
		require.NoError(t, err)
		got = append(got, append([]byte(nil), raw...))
	}
	return got
}

// TestNewPackStream_RoundTrip streams a real cold packfile back through the
// relocated packStream and asserts the bytes round-trip over a bounded range.
func TestNewPackStream_RoundTrip(t *testing.T) {
	const firstSeq uint32 = 1_000_000
	const n = 5
	path, raws := writeFixturePack(t, firstSeq, n)

	got := collectStream(t, NewPackStream(path), ledgerbackend.BoundedRange(firstSeq, firstSeq+n-1))
	require.Equal(t, raws, got)
}

// TestNewPackStream_UnboundedStreamsToLastSeq pins the unbounded arm: with no upper
// bound the stream derives the end from the pack's LastSeq and yields every ledger.
func TestNewPackStream_UnboundedStreamsToLastSeq(t *testing.T) {
	const firstSeq uint32 = 42
	const n = 4
	path, raws := writeFixturePack(t, firstSeq, n)

	got := collectStream(t, NewPackStream(path), ledgerbackend.UnboundedRange(firstSeq))
	require.Equal(t, raws, got)
}

// TestNewPackStream_ObservesCtxCancellation pins the LedgerStream cancellation
// contract: once ctx is canceled, RawLedgers must yield the cancellation error
// instead of streaming on — a drain loop relies on the stream for cancellation and
// does not poll ctx itself.
func TestNewPackStream_ObservesCtxCancellation(t *testing.T) {
	const firstSeq uint32 = 7
	path, _ := writeFixturePack(t, firstSeq, 3)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	yielded := 0
	var gotErr error
	for _, serr := range NewPackStream(path).RawLedgers(ctx, ledgerbackend.BoundedRange(firstSeq, firstSeq+2)) {
		if serr != nil {
			gotErr = serr
			break
		}
		yielded++
		cancel() // cancel after the first ledger; the next step must error
	}
	require.ErrorIs(t, gotErr, context.Canceled)
	require.Equal(t, 1, yielded, "no ledger may be yielded after cancellation")
}

// TestNewPackStream_OpenErrorSurfacesOnFirstYield verifies a missing/invalid pack
// surfaces as an error naming the path on the first yield. The cold reader opens
// lazily (no synchronous I/O), so the failure surfaces through IterateLedgers
// rather than at OpenColdReader; backfillSource is what eagerly stats the pack
// before constructing the stream, so this is the defensive fallback.
func TestNewPackStream_OpenErrorSurfacesOnFirstYield(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist.pack")

	var gotErr error
	for _, serr := range NewPackStream(missing).RawLedgers(context.Background(), ledgerbackend.BoundedRange(1, 3)) {
		gotErr = serr
		break
	}
	require.Error(t, gotErr)
	require.Contains(t, gotErr.Error(), missing)
}
