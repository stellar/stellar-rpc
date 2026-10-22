package bench

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// plan rejects the request flags outside their bounds, an unknown --format and
// an empty --network-passphrase.
func TestPlanRejectsRequestFlags(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*queryFlags)
		want string
	}{
		{"--ledgers-limit 0", func(f *queryFlags) { f.ledgersLimit = 0 }, "--ledgers-limit must be in [1, 20], got 0"},
		{
			"--ledgers-limit above the cap", func(f *queryFlags) { f.ledgersLimit = 21 },
			"--ledgers-limit must be in [1, 20], got 21",
		},
		{"--txpage-limit 0", func(f *queryFlags) { f.txPageLimit = 0 }, "--txpage-limit must be in [1, 200], got 0"},
		{
			"--txpage-limit above the cap", func(f *queryFlags) { f.txPageLimit = 201 },
			"--txpage-limit must be in [1, 200], got 201",
		},
		{"unknown --format", func(f *queryFlags) { f.format = "xdr" }, `--format: unknown format "xdr"`},
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
