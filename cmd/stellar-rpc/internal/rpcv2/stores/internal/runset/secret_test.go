package runset

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// fakeKV is an in-memory KV over one CF's worth of keys, with an injectable
// read failure. Keys are compared as strings (the protocol's keys are fixed
// literals).
type fakeKV struct {
	data   map[string][]byte
	getErr error
	putErr error
	puts   int
}

func newFakeKV() *fakeKV { return &fakeKV{data: map[string][]byte{}} }

func (f *fakeKV) Get(_ string, key []byte) ([]byte, bool, error) {
	if f.getErr != nil {
		return nil, false, f.getErr
	}
	v, ok := f.data[string(key)]
	return v, ok, nil
}

func (f *fakeKV) Put(_ string, key, value []byte) error {
	if f.putErr != nil {
		return f.putErr
	}
	f.puts++
	f.data[string(key)] = append([]byte(nil), value...)
	return nil
}

var (
	testKey    = []byte("engine:secret")
	secretA    = []byte{1, 2, 3, 4}
	secretB    = []byte{1, 2, 3, 5}
	zeroSecret = []byte{0, 0, 0, 0}
)

// TestAdoptSecret is the whole adoption lifecycle in one table: the first
// open decides the key, a later open must present the same one, a
// disagreement is loud and durable-state-free, and an all-zero secret is
// refused whatever the DB holds. Both hot engines run exactly this rule —
// each keeps only a threading case of its own (TestHotStore_SecretAdoption,
// TestWarmup_SecretAdoption), because what could drift between them is which
// secret they hand in, not what happens to it.
func TestAdoptSecret(t *testing.T) {
	boom := errors.New("boom")
	for _, tc := range []struct {
		name     string
		stored   []byte // pre-existing persisted secret ("" = none)
		secret   []byte // what this open presents
		getErr   error
		putErr   error
		wantErr  string
		wantPuts int
	}{
		{name: "first open persists", secret: secretA, wantPuts: 1},
		{name: "same secret re-adopts without writing", stored: secretA, secret: secretA},
		{
			name: "different secret is loud", stored: secretA, secret: secretB,
			wantErr: "engine: hot DB is keyed under a different routing secret",
		},
		{
			name: "zero secret refused on a fresh DB", secret: zeroSecret,
			wantErr: "engine: all-zero routing secret",
		},
		{
			name: "zero secret refused on an adopted DB", stored: secretA, secret: zeroSecret,
			wantErr: "engine: all-zero routing secret",
		},
		{
			name: "read failure surfaces", secret: secretA, getErr: boom,
			wantErr: "engine: read routing secret: boom",
		},
		{
			name: "persist failure surfaces", secret: secretA, putErr: boom,
			wantErr: "engine: persist routing secret: boom",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kv := newFakeKV()
			if tc.stored != nil {
				require.NoError(t, kv.Put("", testKey, tc.stored))
				kv.puts = 0
			}
			kv.getErr, kv.putErr = tc.getErr, tc.putErr

			err := AdoptSecret(kv, testKey, "engine", tc.secret)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
			if tc.getErr == nil && tc.putErr == nil {
				require.Equal(t, tc.wantPuts, kv.puts, "unexpected durable writes")
			}
			// A rejected adoption never re-keys what is already there.
			if tc.stored != nil && tc.getErr == nil {
				got, found, gerr := kv.Get("", testKey)
				require.NoError(t, gerr)
				require.True(t, found)
				require.Equal(t, tc.stored, got)
			}
		})
	}
}

// TestRequireSecret is the freeze's read-only half: it may not adopt, it
// accepts a DB that persisted nothing (see RequireSecret's doc — the run
// magic, not this check, is what rejects a pre-release chunk), and it
// rejects a disagreement before any artifact byte is written.
func TestRequireSecret(t *testing.T) {
	boom := errors.New("boom")
	for _, tc := range []struct {
		name    string
		stored  []byte
		secret  []byte
		getErr  error
		wantErr string
	}{
		{name: "no persisted secret is accepted", secret: secretA},
		{name: "matching secret is accepted", stored: secretA, secret: secretA},
		{
			name: "mismatch is loud", stored: secretA, secret: secretB,
			wantErr: "hot DB is keyed under a different routing secret than this freeze",
		},
		{
			name: "read failure surfaces", secret: secretA, getErr: boom,
			wantErr: "read routing secret: boom",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kv := newFakeKV()
			if tc.stored != nil {
				require.NoError(t, kv.Put("", testKey, tc.stored))
				kv.puts = 0
			}
			kv.getErr = tc.getErr

			err := RequireSecret(kv, testKey, tc.secret)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
			require.Zero(t, kv.puts, "the freeze half must mint no durable state")
		})
	}
}
