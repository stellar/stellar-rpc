package config

import (
	"errors"
	"strings"
	"testing"
)

// mockConfigStore is the test double for the ConfigStore interface. Keeping
// it local to the test file (rather than reusing the package's stub) gives
// each test full control over the pre-seeded state — exactly what the
// mismatch / match / absent branch coverage requires.
type mockConfigStore struct {
	data map[string]string
	// getErr, if non-nil, is returned from Get instead of a lookup —
	// used by the "Get error propagates" branch.
	getErr error
	// putErr, if non-nil, is returned from Put — used to verify that
	// first-run persistence errors propagate.
	putErr error
}

func newMockConfigStore() *mockConfigStore {
	return &mockConfigStore{data: map[string]string{}}
}

func (m *mockConfigStore) Get(key string) (string, bool, error) {
	if m.getErr != nil {
		return "", false, m.getErr
	}
	v, ok := m.data[key]
	return v, ok, nil
}

func (m *mockConfigStore) Put(key, value string) error {
	if m.putErr != nil {
		return m.putErr
	}
	m.data[key] = value
	return nil
}

// Cycle 5 — Validate rejects malformed configs with operator-legible error
// messages. Table-driven to keep each failure mode named and isolated;
// substring match on error text lets us verify the error identifies the
// specific missing/invalid key without over-specifying exact wording.
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

// Cycle 9 — ValidateFlags rejects out-of-range CLI inputs with an error
// whose message names the offending flag. Covers four failure modes plus
// a valid pass-through. Each failure mode maps to a rule from the design
// doc's "Validation before DAG construction" enumeration.
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
			name:       "workers < 1",
			flags:      CLIFlags{StartLedger: 2, EndLedger: 100, Workers: 0, MaxRetries: 3},
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

// Cycle 10 — ValidateAgainstConfigStore enforces CHUNKS_PER_TXHASH_INDEX
// immutability. Three branches from the design doc:
//   - absent: first run; write current value and pass.
//   - matches: subsequent run with same value; pass.
//   - mismatches: hard-abort with error naming both stored and current values.
func TestValidateAgainstConfigStore(t *testing.T) {
	const key = "config:chunks_per_txhash_index"

	// Build a validated config with the caller-specified CHUNKS_PER_TXHASH_INDEX.
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
		store := newMockConfigStore()
		if err := cfg.ValidateAgainstConfigStore(store); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got := store.data[key]; got != "1000" {
			t.Errorf("seed key not persisted: store[%s] = %q", key, got)
		}
	})

	t.Run("stored matches current → pass", func(t *testing.T) {
		cfg := mustValidated(t, 1000)
		store := newMockConfigStore()
		store.data[key] = "1000"
		if err := cfg.ValidateAgainstConfigStore(store); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("stored differs from current → hard abort", func(t *testing.T) {
		cfg := mustValidated(t, 500) // operator changed the config
		store := newMockConfigStore()
		store.data[key] = "1000" // prior run seeded 1000
		err := cfg.ValidateAgainstConfigStore(store)
		if err == nil {
			t.Fatal("expected mismatch error, got nil")
		}
		// Both the stored and the current values must appear in the
		// error so the operator can tell what changed.
		for _, want := range []string{"1000", "500", "CHUNKS_PER_TXHASH_INDEX"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q missing substring %q", err.Error(), want)
			}
		}
	})
}

// mockBSBProbe is the test double for BSBAvailabilityProbe. A fixed max
// ledger value + optional injected error gives full control over the two
// happy-path branches and the error-propagation branch.
type mockBSBProbe struct {
	maxLedger uint32
	err       error
}

func (m mockBSBProbe) MaxAvailableLedger() (uint32, error) {
	return m.maxLedger, m.err
}

// Cycle 11 — ValidateAgainstBSB enforces the "expanded end ≤ max available
// ledger in BSB" rule from the design doc's BSB-Availability Validation
// section. Three cases:
//   - probe max ≥ effective end → pass;
//   - probe max < effective end → error naming both values;
//   - probe returns error        → propagate (caller decides hard vs soft abort).
//
// Pre-condition: ApplyFlags has been called so EffectiveEndLedger is
// populated. Tests construct the state explicitly to avoid depending on the
// full pipeline ordering.
func TestValidateAgainstBSB(t *testing.T) {
	t.Run("probe max >= effective end → pass", func(t *testing.T) {
		cfg, err := ParseConfig(minimalValidTOML())
		if err != nil {
			t.Fatalf("ParseConfig: %v", err)
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate: %v", err)
		}
		cfg.ApplyFlags(CLIFlags{StartLedger: 2, EndLedger: 10_000_001, Workers: 1})

		probe := mockBSBProbe{maxLedger: 20_000_001}
		if err := cfg.ValidateAgainstBSB(probe); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("probe max < effective end → hard abort", func(t *testing.T) {
		cfg, err := ParseConfig(minimalValidTOML())
		if err != nil {
			t.Fatalf("ParseConfig: %v", err)
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate: %v", err)
		}
		cfg.ApplyFlags(CLIFlags{StartLedger: 2, EndLedger: 10_000_001, Workers: 1})

		probe := mockBSBProbe{maxLedger: 5_000_000}
		err = cfg.ValidateAgainstBSB(probe)
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
		cfg, err := ParseConfig(minimalValidTOML())
		if err != nil {
			t.Fatalf("ParseConfig: %v", err)
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate: %v", err)
		}
		cfg.ApplyFlags(CLIFlags{StartLedger: 2, EndLedger: 10_000_001, Workers: 1})

		sentinel := errors.New("bsb: connection refused")
		probe := mockBSBProbe{err: sentinel}
		err = cfg.ValidateAgainstBSB(probe)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !errors.Is(err, sentinel) {
			t.Errorf("error %v does not wrap sentinel %v", err, sentinel)
		}
	})
}
