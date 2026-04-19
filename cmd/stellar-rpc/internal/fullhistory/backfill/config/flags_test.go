package config

import (
	"runtime"
	"testing"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/geometry"
)

// Cycle 6 — ApplyFlags widens the operator-supplied [--start-ledger,
// --end-ledger] range OUTWARD to the nearest chunk boundaries per the design
// doc's "Chunk Boundary Expansion" section. Never narrows. Computed from the
// geometry that BuildGeometry produces for this config (ChunkSize = 10_000,
// ChunksPerTxHashIndex defaulted to 1000).
//
// Four cases cover the combinatorial shape:
//
//	(a) both endpoints mid-chunk (canonical example from issue #684 body);
//	(b) start=FirstLedger (2) and end already chunk-aligned — no widening;
//	(c) start=FirstLedger (2), end mid-chunk — only end widens;
//	(d) start mid-chunk, end already chunk-aligned — only start widens.
func TestApplyFlags_ChunkBoundaryExpansion(t *testing.T) {
	cases := []struct {
		name         string
		startLedger  uint32
		endLedger    uint32
		wantEffStart uint32
		wantEffEnd   uint32
	}{
		{
			// Canonical example from the issue body / design doc:
			// 5_000_000 falls inside chunk 499 ([4_990_002, 5_000_001]),
			// 56_337_842 falls inside chunk 5633 ([56_330_002, 56_340_001]).
			name:         "both mid-chunk (canonical)",
			startLedger:  5_000_000,
			endLedger:    56_337_842,
			wantEffStart: 4_990_002,
			wantEffEnd:   56_340_001,
		},
		{
			// Chunk-aligned already — widening is a no-op.
			name:         "start=2, end at chunk-boundary (no widen)",
			startLedger:  2,
			endLedger:    10_000_001, // chunk 999's last ledger
			wantEffStart: 2,
			wantEffEnd:   10_000_001,
		},
		{
			// Start at absolute minimum, end falls mid-chunk 3
			// (chunk 3 spans [30_002, 40_001]).
			name:         "start=2, end mid-chunk (widen end only)",
			startLedger:  2,
			endLedger:    35_000,
			wantEffStart: 2,
			wantEffEnd:   40_001,
		},
		{
			// Start falls mid-chunk 1 (spans [10_002, 20_001]),
			// end at chunk 5's last ledger (60_001).
			name:         "start mid-chunk, end at chunk-boundary (widen start only)",
			startLedger:  15_000,
			endLedger:    60_001,
			wantEffStart: 10_002,
			wantEffEnd:   60_001,
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
			cfg.ApplyFlags(CLIFlags{
				StartLedger: tc.startLedger,
				EndLedger:   tc.endLedger,
			})
			if cfg.EffectiveStartLedger != tc.wantEffStart {
				t.Errorf("EffectiveStartLedger = %d, want %d", cfg.EffectiveStartLedger, tc.wantEffStart)
			}
			if cfg.EffectiveEndLedger != tc.wantEffEnd {
				t.Errorf("EffectiveEndLedger = %d, want %d", cfg.EffectiveEndLedger, tc.wantEffEnd)
			}
		})
	}
}

// Cycle 7 — ApplyFlags resolves the "0 sentinel" for Workers to GOMAXPROCS
// and for MaxRetries to 3, matching the help-text and design-doc contracts.
// Explicit non-zero values pass through unchanged — this test verifies both
// directions to guard against a regression where the default clobbered an
// operator-supplied value.
func TestApplyFlags_WorkerAndRetryDefaults(t *testing.T) {
	cases := []struct {
		name           string
		flagWorkers    int
		flagMaxRetries int
		wantWorkers    int
		wantMaxRetries int
	}{
		{
			name:           "zero sentinels → GOMAXPROCS and 3",
			flagWorkers:    0,
			flagMaxRetries: 0,
			wantWorkers:    runtime.GOMAXPROCS(0),
			wantMaxRetries: 3,
		},
		{
			name:           "explicit values pass through",
			flagWorkers:    40,
			flagMaxRetries: 7,
			wantWorkers:    40,
			wantMaxRetries: 7,
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
			cfg.ApplyFlags(CLIFlags{
				StartLedger: 2,
				EndLedger:   10_000_001,
				Workers:     tc.flagWorkers,
				MaxRetries:  tc.flagMaxRetries,
			})
			if cfg.Workers != tc.wantWorkers {
				t.Errorf("Workers = %d, want %d", cfg.Workers, tc.wantWorkers)
			}
			if cfg.MaxRetries != tc.wantMaxRetries {
				t.Errorf("MaxRetries = %d, want %d", cfg.MaxRetries, tc.wantMaxRetries)
			}
		})
	}
}

// Cycle 8 — ApplyFlags merges LogLevel / LogFormat from CLI into
// cfg.Logging ONLY when the flag value is non-empty. Empty means "operator
// did not pass --log-level / --log-format, keep whatever the TOML said."
// Test covers both directions (override and passthrough).
func TestApplyFlags_LoggingOverride(t *testing.T) {
	cases := []struct {
		name       string
		tomlLevel  string
		tomlFormat string
		flagLevel  string
		flagFormat string
		wantLevel  string
		wantFormat string
	}{
		{
			name:       "CLI non-empty overrides TOML",
			tomlLevel:  "info",
			tomlFormat: "text",
			flagLevel:  "debug",
			flagFormat: "json",
			wantLevel:  "debug",
			wantFormat: "json",
		},
		{
			name:       "CLI empty leaves TOML intact",
			tomlLevel:  "warn",
			tomlFormat: "json",
			flagLevel:  "",
			flagFormat: "",
			wantLevel:  "warn",
			wantFormat: "json",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &Config{
				Service: ServiceConfig{DefaultDataDir: "/data"},
				Backfill: BackfillConfig{
					BSB: &BSBConfig{BucketPath: "bucket"},
				},
				Logging: LoggingConfig{Level: tc.tomlLevel, Format: tc.tomlFormat},
			}
			if err := cfg.Validate(); err != nil {
				t.Fatalf("Validate: %v", err)
			}
			cfg.ApplyFlags(CLIFlags{
				StartLedger: 2,
				EndLedger:   10_000_001,
				LogLevel:    tc.flagLevel,
				LogFormat:   tc.flagFormat,
			})
			if cfg.Logging.Level != tc.wantLevel {
				t.Errorf("Logging.Level = %q, want %q", cfg.Logging.Level, tc.wantLevel)
			}
			if cfg.Logging.Format != tc.wantFormat {
				t.Errorf("Logging.Format = %q, want %q", cfg.Logging.Format, tc.wantFormat)
			}
		})
	}
}

// Cycle 12 — BuildGeometry threads CHUNKS_PER_TXHASH_INDEX into
// geometry.Geometry so downstream callers (ApplyFlags, the DAG engine in
// slice #687) see a self-consistent range layout. Default case uses 1000,
// custom case uses a non-round 250 to catch any accidental hard-coded
// "× 1000" reintroduction.
func TestBuildGeometry(t *testing.T) {
	cases := []struct {
		name           string
		chunksPerIndex uint32
		wantRangeSize  uint32
	}{
		{
			name:           "default CHUNKS_PER_TXHASH_INDEX (1000) → 10M range",
			chunksPerIndex: 0, // forces default
			wantRangeSize:  10_000_000,
		},
		{
			name:           "custom CHUNKS_PER_TXHASH_INDEX (250) → 2.5M range",
			chunksPerIndex: 250,
			wantRangeSize:  2_500_000,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &Config{}
			cfg.Backfill.ChunksPerTxHashIndex = tc.chunksPerIndex
			geo := cfg.BuildGeometry()
			if geo.ChunkSize != geometry.ChunkSize {
				t.Errorf("ChunkSize = %d, want %d (layout invariant)", geo.ChunkSize, geometry.ChunkSize)
			}
			if geo.RangeSize != tc.wantRangeSize {
				t.Errorf("RangeSize = %d, want %d", geo.RangeSize, tc.wantRangeSize)
			}
		})
	}
}
