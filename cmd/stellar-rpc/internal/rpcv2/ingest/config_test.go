package ingest

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

// zeroSecret is a right-length but all-zero secret: the shape a caller that
// forgot to derive one hands in (a zero [stores.SecretLen]byte sliced), which
// disables blinding entirely rather than failing a length check.
func zeroSecret() []byte { return make([]byte, stores.SecretLen) }

// TestConfig_Validate covers every rejection validate() owns plus the
// fully-valid config it must accept. The secret guards are the load-bearing
// ones: without them a Txhash/Events pass can key its .bin/index under a nil
// or all-zero secret — the first silently unqueryable, the second reproducible
// by anyone who can influence the indexed keys. Both used to be silent.
func TestConfig_Validate(t *testing.T) {
	for _, tc := range []struct {
		name    string
		cfg     Config
		wantErr string // "" ⇒ must validate clean
	}{
		{
			name:    "no data types enabled",
			cfg:     Config{},
			wantErr: "enables no data types",
		},
		{
			name:    "txhash without a secret",
			cfg:     Config{Txhash: true},
			wantErr: "per-index secret required",
		},
		{
			name:    "txhash with a short secret",
			cfg:     Config{Txhash: true, TxhashSecret: bytes.Repeat([]byte{0x5C}, 8)},
			wantErr: "per-index secret required",
		},
		{
			name:    "txhash with an all-zero secret",
			cfg:     Config{Txhash: true, TxhashSecret: zeroSecret()},
			wantErr: "all zero",
		},
		{
			name:    "events without a secret",
			cfg:     Config{Events: true},
			wantErr: "per-index secret required",
		},
		{
			name:    "events with an all-zero secret",
			cfg:     Config{Events: true, EventsSecret: zeroSecret()},
			wantErr: "all zero",
		},
		{
			name:    "negative encode workers",
			cfg:     Config{Ledgers: true, ZstdEncodeWorkers: -1},
			wantErr: "must be >= 0",
		},
		{
			name: "every data type with derived secrets",
			cfg: Config{
				Ledgers:           true,
				Txhash:            true,
				Events:            true,
				TxhashSecret:      testTxhashSecretBytes(),
				EventsSecret:      testEventsSecretBytes(),
				ZstdEncodeWorkers: 0,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.validate()
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}
