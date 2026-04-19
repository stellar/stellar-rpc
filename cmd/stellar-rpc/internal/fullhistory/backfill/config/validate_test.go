package config

import (
	"errors"
	"strings"
	"testing"
)

// mockStore is the local test double for the Store interface. Kept
// separate from the package's stub so each test controls pre-seeded
// state directly.
type mockStore struct {
	data   map[string]string
	getErr error
	putErr error
}

func newMockStore() *mockStore {
	return &mockStore{data: map[string]string{}}
}

func (m *mockStore) Get(key string) (string, bool, error) {
	if m.getErr != nil {
		return "", false, m.getErr
	}
	v, ok := m.data[key]
	return v, ok, nil
}

func (m *mockStore) Put(key, value string) error {
	if m.putErr != nil {
		return m.putErr
	}
	m.data[key] = value
	return nil
}

// mockBSBProbe is the test double for BSBAvailabilityProbe — a fixed
// max-ledger value plus an optional injected error.
type mockBSBProbe struct {
	maxLedger uint32
	err       error
}

func (m mockBSBProbe) MaxAvailableLedger() (uint32, error) {
	return m.maxLedger, m.err
}

// TestValidate_RejectsBadConfigs — required-field rules each produce
// an error that names the offending TOML key.
func TestValidate_RejectsBadConfigs(t *testing.T) {
	cases := []struct {
		name       string
		toml       string
		wantErrSub string
	}{
		{
			name: "missing DEFAULT_DATA_DIR",
			toml: `
[BACKFILL.BSB]
BUCKET_PATH = "bucket"
`,
			wantErrSub: "DEFAULT_DATA_DIR",
		},
		{
			name: "missing [BACKFILL.BSB] section",
			toml: `
[SERVICE]
DEFAULT_DATA_DIR = "/data"
`,
			wantErrSub: "BACKFILL.BSB",
		},
		{
			name: "[BACKFILL.BSB] present but BUCKET_PATH empty",
			toml: `
[SERVICE]
DEFAULT_DATA_DIR = "/data"

[BACKFILL.BSB]
BUFFER_SIZE = 500
`,
			wantErrSub: "BUCKET_PATH",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := ParseConfig([]byte(tc.toml))
			if err != nil {
				t.Fatalf("ParseConfig: %v", err)
			}
			err = cfg.Validate()
			if err == nil {
				t.Fatalf("Validate succeeded; wanted error containing %q", tc.wantErrSub)
			}
			if !strings.Contains(err.Error(), tc.wantErrSub) {
				t.Errorf("error %q does not contain %q", err.Error(), tc.wantErrSub)
			}
		})
	}
}

// TestValidateFlags — out-of-range CLI inputs produce errors naming the
// offending flag. Workers=0 is a valid sentinel (resolves to GOMAXPROCS
// in ApplyFlags); only Workers<0 is rejected.
func TestValidateFlags(t *testing.T) {
	cases := []struct {
		name       string
		flags      CLIFlags
		wantErrSub string // empty → expect nil
	}{
		{
			name:       "valid inputs pass",
			flags:      CLIFlags{StartLedger: 2, EndLedger: 10_000_001, Workers: 40, MaxRetries: 3},
			wantErrSub: "",
		},
		{
			name:       "workers=0 (GOMAXPROCS sentinel) passes",
			flags:      CLIFlags{StartLedger: 2, EndLedger: 10_000_001, Workers: 0, MaxRetries: 3},
			wantErrSub: "",
		},
		{
			name:       "start < FirstLedger (2)",
			flags:      CLIFlags{StartLedger: 1, EndLedger: 100, Workers: 1},
			wantErrSub: "start-ledger",
		},
		{
			name:       "end <= start",
			flags:      CLIFlags{StartLedger: 100, EndLedger: 100, Workers: 1},
			wantErrSub: "end-ledger",
		},
		{
			name:       "workers < 0",
			flags:      CLIFlags{StartLedger: 2, EndLedger: 100, Workers: -1, MaxRetries: 3},
			wantErrSub: "workers",
		},
		{
			name:       "max-retries < 0",
			flags:      CLIFlags{StartLedger: 2, EndLedger: 100, Workers: 1, MaxRetries: -1},
			wantErrSub: "max-retries",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := ParseConfig(minimalValidTOML())
			if err != nil {
				t.Fatalf("ParseConfig: %v", err)
			}
			if err := cfg.Validate(); err != nil {
				t.Fatalf("Validate: %v", err)
			}
			err = cfg.ValidateFlags(tc.flags)
			if tc.wantErrSub == "" {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("ValidateFlags succeeded; wanted error containing %q", tc.wantErrSub)
			}
			if !strings.Contains(err.Error(), tc.wantErrSub) {
				t.Errorf("error %q does not contain %q", err.Error(), tc.wantErrSub)
			}
		})
	}
}

// TestValidateAgainstStore — CHUNKS_PER_TXHASH_INDEX immutability.
// Three branches: absent (first-run seed), match (pass), mismatch (abort).
func TestValidateAgainstStore(t *testing.T) {
	const key = "config:chunks_per_txhash_index"

	mustValidated := func(t *testing.T, cpi uint32) *Config {
		t.Helper()
		cfg, err := ParseConfig(minimalValidTOML())
		if err != nil {
			t.Fatalf("ParseConfig: %v", err)
		}
		cfg.Backfill.ChunksPerTxHashIndex = cpi
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate: %v", err)
		}
		return cfg
	}

	t.Run("absent key → first-run seed write, pass", func(t *testing.T) {
		cfg := mustValidated(t, 1000)
		store := newMockStore()
		if err := cfg.ValidateAgainstStore(store); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got := store.data[key]; got != "1000" {
			t.Errorf("seed key not persisted: store[%s] = %q", key, got)
		}
	})

	t.Run("stored matches current → pass", func(t *testing.T) {
		cfg := mustValidated(t, 1000)
		store := newMockStore()
		store.data[key] = "1000"
		if err := cfg.ValidateAgainstStore(store); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("precondition violation: Validate not called first", func(t *testing.T) {
		// ChunksPerTxHashIndex defaults to 0 on a freshly-parsed Config.
		// Skipping Validate leaves it at 0; ValidateAgainstStore must
		// refuse rather than silently seeding "0" into the store.
		cfg := &Config{}
		err := cfg.ValidateAgainstStore(newMockStore())
		if err == nil {
			t.Fatal("expected precondition error, got nil")
		}
		for _, want := range []string{"precondition", "CHUNKS_PER_TXHASH_INDEX", "Validate()"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q missing substring %q", err.Error(), want)
			}
		}
	})

	t.Run("stored differs from current → hard abort", func(t *testing.T) {
		cfg := mustValidated(t, 500)
		store := newMockStore()
		store.data[key] = "1000" // prior run seeded 1000
		err := cfg.ValidateAgainstStore(store)
		if err == nil {
			t.Fatal("expected mismatch error, got nil")
		}
		// Error must name both stored and current values and the key name.
		for _, want := range []string{"1000", "500", "CHUNKS_PER_TXHASH_INDEX"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q missing substring %q", err.Error(), want)
			}
		}
	})
}

// TestValidateAgainstBSB — three cases: probe reports enough, probe
// reports not enough, probe errors out.
func TestValidateAgainstBSB(t *testing.T) {
	setupCfg := func(t *testing.T) *Config {
		t.Helper()
		cfg, err := ParseConfig(minimalValidTOML())
		if err != nil {
			t.Fatalf("ParseConfig: %v", err)
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate: %v", err)
		}
		cfg.ApplyFlags(CLIFlags{StartLedger: 2, EndLedger: 10_000_001, Workers: 1})
		return cfg
	}

	t.Run("probe max >= effective end → pass", func(t *testing.T) {
		cfg := setupCfg(t)
		if err := cfg.ValidateAgainstBSB(mockBSBProbe{maxLedger: 20_000_001}); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("probe max < effective end → hard abort", func(t *testing.T) {
		cfg := setupCfg(t)
		err := cfg.ValidateAgainstBSB(mockBSBProbe{maxLedger: 5_000_000})
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		for _, want := range []string{"10000001", "5000000"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q missing substring %q", err.Error(), want)
			}
		}
	})

	t.Run("probe returns error → propagated", func(t *testing.T) {
		cfg := setupCfg(t)
		sentinel := errors.New("bsb: connection refused")
		err := cfg.ValidateAgainstBSB(mockBSBProbe{err: sentinel})
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !errors.Is(err, sentinel) {
			t.Errorf("error %v does not wrap sentinel %v", err, sentinel)
		}
	})
}
