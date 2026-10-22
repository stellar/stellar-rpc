package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1config "github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/config"
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
	disableRPCv2   bool
	stdout, stderr bytes.Buffer
	v1cfg          *v1config.Config
	v2path         string
	v2flags        v2config.FlagOverrides
	v2calls        int
}

func (f *fakeEntry) run(args []string, env map[string]string) int {
	// nil would fall through to the test binary's own os.Args, which breaks a
	// subtest run alone from an IDE or dlv.
	if args == nil {
		args = []string{}
	}
	e := Entry{
		Args:         args,
		LookupEnv:    envOf(env),
		Stdout:       &f.stdout,
		Stderr:       &f.stderr,
		DisableRPCv2: f.disableRPCv2,
		startRPCv1: func(cfg *v1config.Config) error {
			f.v1cfg = cfg
			return nil
		},
		startRPCv2: func(configPath string, flags v2config.FlagOverrides) error {
			f.v2path = configPath
			f.v2flags = flags
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
			require.Equal(t, 0, f.run(tc.args, tc.env), f.stderr.String())
			require.NotNil(t, f.v1cfg, "rpcv1 did not start")
			assert.Equal(t, tc.want, f.v1cfg.ConfigPath)
			if tc.want != "" {
				assert.Equal(t, "from file", f.v1cfg.NetworkPassphrase)
			}
			assert.Equal(t, 0, f.v2calls)
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
			require.Equal(t, 0, f.run(tc.args, tc.env), f.stderr.String())
			assert.Equal(t, path, f.v2path)
			assert.Equal(t, 1, f.v2calls)
			assert.Nil(t, f.v1cfg)
			assert.NotNil(t, f.v2flags, "the daemon must get the flag overrides")
		})
	}
}

func TestRPCv2RequiresConfigFile(t *testing.T) {
	f := &fakeEntry{}
	require.Equal(t, 1, f.run([]string{enableFlag}, nil))
	assert.Equal(t, 0, f.v2calls)
	assert.Contains(t, f.stderr.String(), configPathFlag)
	assert.Contains(t, f.stderr.String(), ConfigPathEnv)
}

func TestRPCv2IgnoresRPCv1EnvVars(t *testing.T) {
	path := writeFile(t, "v2.toml", "[storage]\ndefault_data_dir = \"/tmp/x\"\n")
	env := map[string]string{
		ConfigPathEnv:       path,
		legacyConfigPathEnv: "/etc/old.toml",
		"STELLAR_CORE_URL":  "http://localhost:11626",
		"ENDPOINT":          "localhost:8000",
	}

	f := &fakeEntry{}
	require.Equal(t, 0, f.run([]string{enableFlag}, env), f.stderr.String())
	assert.Equal(t, path, f.v2path, "the old config path env var must not win in rpcv2 mode")
}

func TestRPCv1RefusesRPCv2Config(t *testing.T) {
	for _, table := range rpcv2Tables() {
		t.Run(table, func(t *testing.T) {
			path := writeFile(t, "v2.toml", "["+table+"]\n")
			f := &fakeEntry{}
			require.Equal(t, 1, f.run([]string{configPathFlag, path}, nil))
			assert.Nil(t, f.v1cfg, "rpcv1 must not start")

			out := f.stderr.String()
			assert.Contains(t, out, "rpcv2 config")
			assert.Contains(t, out, enableFlag)
			assert.Contains(t, out, "["+table+"]")
			assert.Contains(t, out, path)
		})
	}

	t.Run("mixed file", func(t *testing.T) {
		path := writeFile(t, "mixed.toml", "NETWORK_PASSPHRASE = \"x\"\n[service]\nendpoint = \"localhost:8000\"\n")
		f := &fakeEntry{}
		require.Equal(t, 1, f.run([]string{configPathFlag, path}, nil))
		assert.Nil(t, f.v1cfg)
		assert.Contains(t, f.stderr.String(), "rpcv2 config")
	})

	t.Run("from env", func(t *testing.T) {
		path := writeFile(t, "v2.toml", "[logging]\nlevel = \"info\"\n")
		f := &fakeEntry{}
		require.Equal(t, 1, f.run(nil, map[string]string{legacyConfigPathEnv: path}))
		assert.Nil(t, f.v1cfg)
		assert.Contains(t, f.stderr.String(), "rpcv2 config")
	})
}

func TestRPCv1AcceptsItsOwnLowercaseAndUppercaseKeys(t *testing.T) {
	t.Run("BACKFILL key is not the [backfill] table", func(t *testing.T) {
		path := writeFile(t, "v1.toml", "BACKFILL = true\n")
		f := &fakeEntry{}
		require.Equal(t, 0, f.run([]string{configPathFlag, path}, nil), f.stderr.String())
		require.NotNil(t, f.v1cfg)
		assert.True(t, f.v1cfg.Backfill)
	})

	t.Run("lowercase rpcv1 table", func(t *testing.T) {
		path := writeFile(t, "v1.toml", "[datastore_config]\ntype = \"GCS\"\n")
		f := &fakeEntry{}
		require.Equal(t, 0, f.run([]string{configPathFlag, path}, nil), f.stderr.String())
		require.NotNil(t, f.v1cfg)
		assert.Equal(t, "GCS", f.v1cfg.DataStoreConfig.Type)
	})
}

// The shape check is safe only because rpcv1 never uses the six names. Its
// strict parser is the proof.
func TestRPCv1RejectsEveryRPCv2Table(t *testing.T) {
	for _, table := range rpcv2Tables() {
		path := writeFile(t, "v.toml", "STRICT = true\n["+table+"]\n")
		var cfg v1config.Config
		err := cfg.SetValues(envOf(map[string]string{legacyConfigPathEnv: path}))
		assert.ErrorContains(t, err, "unexpected entry", "rpcv1 accepts [%s]", table)
	}
}

func TestOnlyTheSelectedModeFlagsExist(t *testing.T) {
	t.Run("rpcv2 flag in rpcv1 mode", func(t *testing.T) {
		f := &fakeEntry{}
		require.Equal(t, 1, f.run([]string{"--storage.default_data_dir=/tmp/x"}, nil))
		assert.Contains(t, f.stderr.String(), "unknown flag: --storage.default_data_dir")
		assert.Nil(t, f.v1cfg)
	})

	t.Run("rpcv1 flag in rpcv2 mode", func(t *testing.T) {
		f := &fakeEntry{}
		require.Equal(t, 1, f.run([]string{enableFlag, "--stellar-core-url=http://x"}, nil))
		assert.Contains(t, f.stderr.String(), "unknown flag: --stellar-core-url")
		assert.Equal(t, 0, f.v2calls)
	})
}

func TestHelpShowsOneMode(t *testing.T) {
	t.Run("rpcv1", func(t *testing.T) {
		f := &fakeEntry{}
		require.Equal(t, 0, f.run([]string{"--help"}, nil), f.stderr.String())
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
		require.Equal(t, 0, f.run([]string{enableFlag, "--help"}, nil), f.stderr.String())
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
	require.Equal(t, 0, f.run([]string{versionUse}, nil), f.stderr.String())
	assert.Contains(t, f.stdout.String(), "mode: rpcv1")
	assert.Contains(t, f.stdout.String(), enableFlag)

	f = &fakeEntry{}
	require.Equal(t, 0, f.run([]string{enableFlag, versionUse}, nil), f.stderr.String())
	assert.Contains(t, f.stdout.String(), "mode: rpcv2")
	assert.Contains(t, f.stdout.String(), "--enable-rpcv2=false")
}

func TestDisableRPCv2(t *testing.T) {
	path := writeFile(t, "v2.toml", "[storage]\ndefault_data_dir = \"/tmp/x\"\n")

	t.Run("refuses rpcv2", func(t *testing.T) {
		f := &fakeEntry{disableRPCv2: true}
		require.Equal(t, 1, f.run([]string{enableFlag, configPathFlag, path}, nil))
		assert.Contains(t, f.stderr.String(), "rpcv2 is not available in this build")
		assert.Equal(t, 0, f.v2calls)
	})

	t.Run("runs rpcv1", func(t *testing.T) {
		f := &fakeEntry{disableRPCv2: true}
		require.Equal(t, 0, f.run([]string{"--enable-rpcv2=false"}, nil), f.stderr.String())
		assert.NotNil(t, f.v1cfg)
	})

	t.Run("help and version do not mention the flag", func(t *testing.T) {
		for _, args := range [][]string{{"--help"}, {versionUse}} {
			f := &fakeEntry{disableRPCv2: true}
			require.Equal(t, 0, f.run(args, nil), f.stderr.String())
			assert.NotContains(t, f.stdout.String(), enableFlag, args)
		}
	})

	t.Run("rpcv2 config file", func(t *testing.T) {
		f := &fakeEntry{disableRPCv2: true}
		require.Equal(t, 1, f.run([]string{configPathFlag, path}, nil))
		assert.Contains(t, f.stderr.String(), "rpcv2 config")
		assert.NotContains(t, f.stderr.String(), enableFlag)
	})
}

func TestGenConfigFilePrintsTheModeDefaults(t *testing.T) {
	t.Run("rpcv1", func(t *testing.T) {
		f := &fakeEntry{}
		require.Equal(t, 0, f.run([]string{genConfigFileUse}, nil), f.stderr.String())
		assert.Contains(t, f.stdout.String(), "STELLAR_CORE_URL")
		assert.NotContains(t, f.stdout.String(), "[service]")
	})

	t.Run("rpcv2", func(t *testing.T) {
		f := &fakeEntry{}
		require.Equal(t, 0, f.run([]string{enableFlag, genConfigFileUse}, nil), f.stderr.String())
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
