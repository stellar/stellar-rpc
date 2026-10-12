package store

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// TestScanLedgersFrom_EndsAtMaxUint32 pins that an inclusive scan ending at
// MaxUint32 stops after the endpoint instead of wrapping to 0, whether or not
// the endpoint is present.
func TestScanLedgersFrom_EndsAtMaxUint32(t *testing.T) {
	const end = uint32(math.MaxUint32)
	var asked []uint32
	get := func(seq uint32) (xdr.LedgerCloseMeta, bool, error) {
		asked = append(asked, seq)
		if seq == end {
			return xdr.LedgerCloseMeta{}, false, nil
		}
		lcm := xdr.LedgerCloseMeta{V: 0, V0: &xdr.LedgerCloseMetaV0{}}
		lcm.V0.LedgerHeader.Header.LedgerSeq = xdr.Uint32(seq)
		return lcm, true, nil
	}

	var got []uint32
	for l, err := range ScanLedgersFrom(end-1, end, get) {
		require.NoError(t, err)
		got = append(got, l.Sequence)
	}
	assert.Equal(t, []uint32{end - 1, end}, asked)
	assert.Equal(t, []uint32{end - 1}, got)
}
