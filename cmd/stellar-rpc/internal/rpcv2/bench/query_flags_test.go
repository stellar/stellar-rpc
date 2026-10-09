package bench

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// plan rejects the request flags below their lower bounds and an empty
// --network-passphrase.
func TestPlanRejectsRequestFlagLowerBounds(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*queryFlags)
		want string
	}{
		{"--ledgers-span 0", func(f *queryFlags) { f.ledgersSpan = 0 }, "--ledgers-span must be in [1, "},
		{"--txpage-span 0", func(f *queryFlags) { f.txPageSpan = 0 }, "--txpage-span must be in [1, "},
		{"--txpage-limit 0", func(f *queryFlags) { f.txPageLimit = 0 }, "--txpage-limit must be >= 1, got 0"},
		{"empty --network-passphrase", func(f *queryFlags) { f.passphrase = "" }, "--network-passphrase is required"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := validQueryFlags()
			tc.edit(&f)
			_, err := f.plan()
			require.ErrorContains(t, err, tc.want)
		})
	}
}
