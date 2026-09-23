package event

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

// TestColdParts_RejectsAPartOutsideItsDeclaredSpan pins the check that closes
// the "wrong span exponent under a valid checksum" residual.
//
// A term's span exponent lives in the directory, which no record checksum
// covers. Forging it one wider than the truth leaves every checksum valid and
// the directory still tiling, so the reader happily reads real part records
// and assembles them — under spans they do not belong to. The ids come back
// perfectly well-formed and wrong. Reading a part and confirming its ids fall
// inside the span it was read for is what makes that an error instead.
func TestColdParts_RejectsAPartOutsideItsDeclaredSpan(t *testing.T) {
	f := densePartsFixture()
	dir := buildPartsFixture(t, f.bitmaps)

	before, err := OpenColdReader(partsChunkID, dir, ColdReaderOptions{})
	require.NoError(t, err)
	d, err := before.waitDir()
	require.NoError(t, err)
	entry, demoted := d.lookup(f.key(denseTerm))
	require.True(t, demoted, "the fixture must demote this term, or the pin proves nothing")
	require.NoError(t, before.Close())

	rewriteIndexPack(t, dir, func(a *indexArtifact) {
		a.appData[dirRowAt(t, a.appData, f.key(denseTerm))+22] = entry.k + 1
	})

	cr, err := OpenColdReader(partsChunkID, dir, ColdReaderOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close() })

	_, _, err = cr.LookupKeys(context.Background(),
		[]TermKey{f.key(denseTerm)}, IDRange{Start: 0, End: 3_500_001})
	require.ErrorIs(t, err, stores.ErrCorrupt,
		"a part holding ids outside its declared span must be rejected, not answered")
}
