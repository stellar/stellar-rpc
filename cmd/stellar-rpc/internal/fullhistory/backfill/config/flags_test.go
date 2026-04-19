package config

import (
	"runtime"
	"testing"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/geometry"
)

// TestApplyFlags_ChunkBoundaryExpansion — start widens DOWN, end widens
// UP; never narrows. Four cases cover the combinatorial shape:
//
//	(a) both endpoints mid-chunk (canonical example from issue body);
//	(b) both already chunk-aligned — no widening;
//	(c) start=2, end mid-chunk — only end widens;
//	(d) start mid-chunk, end chunk-aligned — only start widens.
func TestApplyFlags_ChunkBoundaryExpansion(t *testing.T) {
	cases := []struct {
		name         string
		startLedger  uint32
		endLedger    uint32
		wantEffStart uint32
		wantEffEnd   uint32
	}{
		{
			// 5_000_000 is inside chunk 499 ([4_990_002, 5_000_001]);
			// 56_337_842 is inside chunk 5633 ([56_330_002, 56_340_001]).
			name:         "both mid-chunk (canonical)",
			startLedger:  5_000_000,
			endLedger:    56_337_842,
			wantEffStart: 4_990_002,
			wantEffEnd:   56_340_001,
		},
		{
			name:         "start=2, end at chunk-boundary (no widen)",
			startLedger:  2,
			endLedger:    10_000_001, // chunk 999's last ledger
			wantEffStart: 2,
			wantEffEnd:   10_000_001,
		},
		{
			// Chunk 3 spans [30_002, 40_001].
			name:         "start=2, end mid-chunk (widen end only)",
			startLedger:  2,
			endLedger:    35_000,
			wantEffStart: 2,
			wantEffEnd:   40_001,
		},
		{
			// Chunk 1 spans [10_002, 20_001]; chunk 5 ends at 60_001.
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

// TestApplyFlags_WorkerAndRetryDefaults — 0 sentinels resolve to
// GOMAXPROCS / 3; explicit values pass through unchanged.
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

// TestApplyFlags_LoggingOverride — non-empty CLI values override TOML;
// empty values leave the TOML values intact.
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

// TestBuildGeometry — CHUNKS_PER_TXHASH_INDEX threads into the geometry's
// LedgersPerIndex. Default (1000 chunks) → 10M ledgers per index; a custom
// non-round value (250) → 2.5M ledgers per index.
func TestBuildGeometry(t *testing.T) {
	cases := []struct {
		name              string
		chunksPerIndex    uint32
		wantLedgersPerIdx uint32
	}{
		{
			name:              "default (1000) → 10M ledgers per index",
			chunksPerIndex:    0, // forces default
			wantLedgersPerIdx: 10_000_000,
		},
		{
			name:              "custom (250) → 2.5M ledgers per index",
			chunksPerIndex:    250,
			wantLedgersPerIdx: 2_500_000,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &Config{}
			cfg.Backfill.ChunksPerTxHashIndex = tc.chunksPerIndex
			geo := cfg.BuildGeometry()
			if geo.ChunkSize != geometry.ChunkSize {
				t.Errorf("ChunkSize = %d, want %d", geo.ChunkSize, geometry.ChunkSize)
			}
			if geo.LedgersPerIndex() != tc.wantLedgersPerIdx {
				t.Errorf("LedgersPerIndex() = %d, want %d", geo.LedgersPerIndex(), tc.wantLedgersPerIdx)
			}
		})
	}
}
