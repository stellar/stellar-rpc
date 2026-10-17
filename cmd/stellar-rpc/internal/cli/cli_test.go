package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	v1config "github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2"
	v2config "github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/config"
)

const (
	enableFlag     = "--" + EnableRPCv2Flag
	configPathFlag = "--" + ConfigPathFlag
	versionUse     = "version"
	trueValue      = "true"
)

func envOf(vars map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		v, ok := vars[key]
		return v, ok
	}
}

func TestSelectMode(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		env  map[string]string
		want Mode
	}{
		{name: "default is rpcv1", want: ModeRPCv1},
		{name: "flag selects rpcv2", args: []string{enableFlag}, want: ModeRPCv2},
		{name: "flag=true selects rpcv2", args: []string{"--enable-rpcv2=true"}, want: ModeRPCv2},
		{name: "flag=false selects rpcv1", args: []string{"--enable-rpcv2=false"}, want: ModeRPCv1},
		{name: "flag after subcommand", args: []string{versionUse, enableFlag}, want: ModeRPCv2},
		{name: "env selects rpcv2", env: map[string]string{EnableRPCv2Env: trueValue}, want: ModeRPCv2},
		{name: "env false selects rpcv1", env: map[string]string{EnableRPCv2Env: "0"}, want: ModeRPCv1},
		{name: "empty env is unset", env: map[string]string{EnableRPCv2Env: ""}, want: ModeRPCv1},
		{
			name: "flag false wins over env true",
			args: []string{"--enable-rpcv2=false"},
			env:  map[string]string{EnableRPCv2Env: trueValue},
			want: ModeRPCv1,
		},
		{name: "args after -- are not flags", args: []string{"--", enableFlag}, want: ModeRPCv1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := SelectMode(tc.args, envOf(tc.env))
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestSelectModeRejectsNonBooleans(t *testing.T) {
	_, err := SelectMode([]string{"--enable-rpcv2=yes"}, envOf(nil))
	require.ErrorContains(t, err, enableFlag)

	_, err = SelectMode(nil, envOf(map[string]string{EnableRPCv2Env: "yes"}))
	require.ErrorContains(t, err, EnableRPCv2Env)
}

// fakeEntry runs the entry with both daemons replaced by recorders.
type fakeEntry struct {
	stdout, stderr bytes.Buffer
	v1cfg          *v1config.Config
	v2path         string
	v2opts         rpcv2.Options
	v2calls        int
}

func (f *fakeEntry) run(args []string, env map[string]string, forced Mode) int {
	e := Entry{
		Args:       args,
		LookupEnv:  envOf(env),
		Stdout:     &f.stdout,
		Stderr:     &f.stderr,
		ForcedMode: forced,
		startRPCv1: func(cfg *v1config.Config, _ *supportlog.Entry) error {
			f.v1cfg = cfg
			return nil
		},
		startRPCv2: func(configPath string, opts rpcv2.Options) error {
			f.v2path = configPath
			f.v2opts = opts
			f.v2calls++
			return nil
		},
	}
	return e.Run()
}

func writeFile(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

func TestRPCv1ConfigPath(t *testing.T) {
	path := writeFile(t, "v1.toml", `NETWORK_PASSPHRASE = "from file"`+"\n")
	other := writeFile(t, "other.toml", `NETWORK_PASSPHRASE = "from other file"`+"\n")

	for _, tc := range []struct {
		name string
		args []string
		env  map[string]string
		want string
	}{
		{name: "flag", args: []string{configPathFlag, path}, want: path},
		{name: "new env var", env: map[string]string{ConfigPathEnv: path}, want: path},
		{name: "old env var", env: map[string]string{legacyConfigPathEnv: path}, want: path},
		{
			name: "new env var wins over the old one",
			env:  map[string]string{ConfigPathEnv: path, legacyConfigPathEnv: other},
			want: path,
		},
		{
			name: "flag wins over env",
			args: []string{configPathFlag, path},
			env:  map[string]string{ConfigPathEnv: other},
			want: path,
		},
		{name: "no file is fine", want: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeEntry{}
			require.Equal(t, 0, f.run(tc.args, tc.env, 0), f.stderr.String())
			require.NotNil(t, f.v1cfg, "rpcv1 did not start")
			assert.Equal(t, tc.want, f.v1cfg.ConfigPath)
			if tc.want != "" {
				assert.Equal(t, "from file", f.v1cfg.NetworkPassphrase)
			}
			assert.Equal(t, 0, f.v2calls)
			assert.Contains(t, f.stderr.String(), "rpcv1 mode")
		})
	}
}

func TestRPCv2ConfigPath(t *testing.T) {
	path := writeFile(t, "v2.toml", "[storage]\ndefault_data_dir = \"/tmp/x\"\n")

	for _, tc := range []struct {
		name string
		args []string
		env  map[string]string
	}{
		{name: "flag", args: []string{enableFlag, configPathFlag, path}},
		{name: "env", args: []string{enableFlag}, env: map[string]string{ConfigPathEnv: path}},
		{name: "mode from env", env: map[string]string{EnableRPCv2Env: trueValue, ConfigPathEnv: path}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeEntry{}
			require.Equal(t, 0, f.run(tc.args, tc.env, 0), f.stderr.String())
			assert.Equal(t, path, f.v2path)
			assert.Equal(t, 1, f.v2calls)
			assert.Nil(t, f.v1cfg)
			assert.Contains(t, f.stderr.String(), "rpcv2 mode")
			require.NotNil(t, f.v2opts.Config, "the daemon must get the config the cli loaded")
			assert.Equal(t, "/tmp/x", f.v2opts.Config.Storage.DefaultDataDir)
			assert.NotNil(t, f.v2opts.Logger, "the daemon must get the logger the cli built")
		})
	}
}

func TestRPCv2RequiresConfigFile(t *testing.T) {
	f := &fakeEntry{}
	require.Equal(t, 1, f.run([]string{enableFlag}, nil, 0))
	assert.Equal(t, 0, f.v2calls)
	assert.Contains(t, f.stderr.String(), configPathFlag)
	assert.Contains(t, f.stderr.String(), ConfigPathEnv)
}

func TestRPCv2IgnoresRPCv1EnvVars(t *testing.T) {
	path := writeFile(t, "v2.toml", "[storage]\ndefault_data_dir = \"/tmp/x\"\n")
	env := map[string]string{
		ConfigPathEnv:         path,
		legacyConfigPathEnv:   "/etc/old.toml",
		"STELLAR_CORE_URL":    "http://localhost:11626",
		"NETWORK_PASSPHRASE":  "x",
		"ENDPOINT":            "localhost:8000",
		"UNRELATED_VARIABLE":  "1",
		"STELLAR_RPC_UNKNOWN": "1",
	}

	f := &fakeEntry{}
	require.Equal(t, 0, f.run([]string{enableFlag}, env, 0), f.stderr.String())
	assert.Equal(t, path, f.v2path, "the old config path env var must not win in rpcv2 mode")

	line := findLine(t, f.stderr.String(), "ignored")
	assert.Contains(t, line, "level=info")
	assert.Contains(t, line, "ENDPOINT NETWORK_PASSPHRASE SOROBAN_RPC_CONFIG_PATH STELLAR_CORE_URL")
	assert.NotContains(t, line, "UNRELATED_VARIABLE")
	assert.NotContains(t, line, "STELLAR_RPC_UNKNOWN")
}

func TestRPCv2WithoutRPCv1EnvVarsLogsNothingAboutThem(t *testing.T) {
	path := writeFile(t, "v2.toml", "[storage]\ndefault_data_dir = \"/tmp/x\"\n")
	f := &fakeEntry{}
	require.Equal(t, 0, f.run([]string{enableFlag, configPathFlag, path}, nil, 0), f.stderr.String())
	assert.NotContains(t, f.stderr.String(), "ignored")
}

func findLine(t *testing.T, text, substr string) string {
	t.Helper()
	for line := range strings.SplitSeq(text, "\n") {
		if strings.Contains(line, substr) {
			return line
		}
	}
	t.Fatalf("no line contains %q in:\n%s", substr, text)
	return ""
}

func TestRPCv1RefusesRPCv2Config(t *testing.T) {
	for _, table := range rpcv2Tables() {
		t.Run(table, func(t *testing.T) {
			path := writeFile(t, "v2.toml", "["+table+"]\n")
			f := &fakeEntry{}
			require.Equal(t, 1, f.run([]string{configPathFlag, path}, nil, 0))
			assert.Nil(t, f.v1cfg, "rpcv1 must not start")

			line := findLine(t, f.stderr.String(), "rpcv2 config")
			assert.Contains(t, line, "level=error")
			assert.Contains(t, line, enableFlag)
			assert.Contains(t, line, table)
			assert.Contains(t, line, path)
		})
	}

	t.Run("mixed file", func(t *testing.T) {
		path := writeFile(t, "mixed.toml", "NETWORK_PASSPHRASE = \"x\"\n[service]\nendpoint = \"localhost:8000\"\n")
		f := &fakeEntry{}
		require.Equal(t, 1, f.run([]string{configPathFlag, path}, nil, 0))
		assert.Nil(t, f.v1cfg)
		assert.Contains(t, f.stderr.String(), "rpcv2 config")
	})

	t.Run("from env", func(t *testing.T) {
		path := writeFile(t, "v2.toml", "[logging]\nlevel = \"info\"\n")
		f := &fakeEntry{}
		require.Equal(t, 1, f.run(nil, map[string]string{legacyConfigPathEnv: path}, 0))
		assert.Nil(t, f.v1cfg)
		assert.Contains(t, f.stderr.String(), "rpcv2 config")
	})
}

func TestRPCv1AcceptsItsOwnLowercaseAndUppercaseKeys(t *testing.T) {
	t.Run("BACKFILL key is not the [backfill] table", func(t *testing.T) {
		path := writeFile(t, "v1.toml", "BACKFILL = true\n")
		f := &fakeEntry{}
		require.Equal(t, 0, f.run([]string{configPathFlag, path}, nil, 0), f.stderr.String())
		require.NotNil(t, f.v1cfg)
		assert.True(t, f.v1cfg.Backfill)
	})

	t.Run("lowercase rpcv1 table", func(t *testing.T) {
		path := writeFile(t, "v1.toml", "[datastore_config]\ntype = \"GCS\"\n")
		f := &fakeEntry{}
		require.Equal(t, 0, f.run([]string{configPathFlag, path}, nil, 0), f.stderr.String())
		require.NotNil(t, f.v1cfg)
		assert.Equal(t, "GCS", f.v1cfg.DataStoreConfig.Type)
	})
}

func TestForcedMode(t *testing.T) {
	path := writeFile(t, "v2.toml", "[storage]\ndefault_data_dir = \"/tmp/x\"\n")

	t.Run("rpcv2 wrapper runs rpcv2 without the flag", func(t *testing.T) {
		f := &fakeEntry{}
		require.Equal(t, 0, f.run([]string{configPathFlag, path}, nil, ModeRPCv2), f.stderr.String())
		assert.Equal(t, path, f.v2path)
	})

	t.Run("rpcv1 wrapper does not know the flag", func(t *testing.T) {
		f := &fakeEntry{}
		require.Equal(t, 1, f.run([]string{enableFlag}, nil, ModeRPCv1))
		assert.Nil(t, f.v1cfg)
		assert.Contains(t, f.stderr.String(), "unknown flag: --enable-rpcv2")
	})

	t.Run("rpcv1 wrapper ignores the env var", func(t *testing.T) {
		f := &fakeEntry{}
		require.Equal(t, 0, f.run(nil, map[string]string{EnableRPCv2Env: trueValue}, ModeRPCv1), f.stderr.String())
		assert.NotNil(t, f.v1cfg)
		assert.Equal(t, 0, f.v2calls)
	})

	t.Run("wrappers do not advise a flag they do not have", func(t *testing.T) {
		f := &fakeEntry{}
		require.Equal(t, 0, f.run([]string{versionUse}, nil, ModeRPCv2), f.stderr.String())
		assert.Contains(t, f.stdout.String(), "mode: rpcv2\n")
		assert.NotContains(t, f.stdout.String(), enableFlag)

		f = &fakeEntry{}
		require.Equal(t, 1, f.run([]string{configPathFlag, path}, nil, ModeRPCv1))
		assert.Contains(t, f.stderr.String(), "the config file is a rpcv2 config")
		assert.NotContains(t, f.stderr.String(), enableFlag)
	})
}

func TestOnlyTheSelectedModeFlagsExist(t *testing.T) {
	t.Run("rpcv2 flag in rpcv1 mode", func(t *testing.T) {
		f := &fakeEntry{}
		require.Equal(t, 1, f.run([]string{"--storage.default_data_dir=/tmp/x"}, nil, 0))
		assert.Contains(t, f.stderr.String(), "unknown flag: --storage.default_data_dir")
		assert.Nil(t, f.v1cfg)
	})

	t.Run("rpcv1 flag in rpcv2 mode", func(t *testing.T) {
		f := &fakeEntry{}
		require.Equal(t, 1, f.run([]string{enableFlag, "--stellar-core-url=http://x"}, nil, 0))
		assert.Contains(t, f.stderr.String(), "unknown flag: --stellar-core-url")
		assert.Equal(t, 0, f.v2calls)
	})
}

func TestHelpShowsOneMode(t *testing.T) {
	t.Run("rpcv1", func(t *testing.T) {
		f := &fakeEntry{}
		require.Equal(t, 0, f.run([]string{"--help"}, nil, 0), f.stderr.String())
		help := f.stdout.String()
		assert.Contains(t, help, "--stellar-core-url")
		assert.Contains(t, help, enableFlag)
		assert.Contains(t, help, genConfigFileUse)
		assert.NotContains(t, help, "--storage.default_data_dir")
		assert.NotContains(t, help, "--print-runtime-config")
		assert.NotContains(t, help, "bench")
	})

	t.Run("rpcv2", func(t *testing.T) {
		f := &fakeEntry{}
		require.Equal(t, 0, f.run([]string{enableFlag, "--help"}, nil, 0), f.stderr.String())
		help := f.stdout.String()
		assert.Contains(t, help, "--storage.default_data_dir")
		assert.Contains(t, help, "--print-runtime-config")
		assert.Contains(t, help, configPathFlag)
		assert.Contains(t, help, enableFlag)
		assert.Contains(t, help, genConfigFileUse)
		assert.Contains(t, help, "bench")
		assert.NotContains(t, help, "--stellar-core-url")
		assert.NotContains(t, help, "--config string")
	})
}

func TestVersionNamesTheMode(t *testing.T) {
	f := &fakeEntry{}
	require.Equal(t, 0, f.run([]string{versionUse}, nil, 0), f.stderr.String())
	assert.Contains(t, f.stdout.String(), "mode: rpcv1")
	assert.Contains(t, f.stdout.String(), enableFlag)

	f = &fakeEntry{}
	require.Equal(t, 0, f.run([]string{enableFlag, versionUse}, nil, 0), f.stderr.String())
	assert.Contains(t, f.stdout.String(), "mode: rpcv2")
	assert.Contains(t, f.stdout.String(), "--enable-rpcv2=false")
}

func TestGenConfigFilePrintsTheModeDefaults(t *testing.T) {
	t.Run("rpcv1", func(t *testing.T) {
		f := &fakeEntry{}
		require.Equal(t, 0, f.run([]string{genConfigFileUse}, nil, 0), f.stderr.String())
		assert.Contains(t, f.stdout.String(), "STELLAR_CORE_URL")
		assert.NotContains(t, f.stdout.String(), "[service]")
	})

	t.Run("rpcv2", func(t *testing.T) {
		f := &fakeEntry{}
		require.Equal(t, 0, f.run([]string{enableFlag, genConfigFileUse}, nil, 0), f.stderr.String())
		cfg, err := v2config.ParseConfig(f.stdout.Bytes())
		require.NoError(t, err)
		// The TOML round trip turns a nil map or slice into an empty one.
		assert.Empty(t, cfg.Backfill.DataStore.Params)
		assert.Empty(t, cfg.Ingestion.HistoryArchiveURLs)
		cfg.Backfill.DataStore.Params = nil
		cfg.Ingestion.HistoryArchiveURLs = nil
		assert.Equal(t, v2config.Config{}.WithDefaults(), cfg)
		assert.NotContains(t, f.stdout.String(), "STELLAR_CORE_URL")
	})
}
