package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// binaryPath is the stellar-rpc binary TestMain builds once for the tests
// below. errBinaryBuild says why the build failed, if it did.
var (
	binaryPath     string
	errBinaryBuild error
)

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "stellar-rpc-cli-test")
	if err != nil {
		errBinaryBuild = err
		os.Exit(m.Run())
	}
	binaryPath = filepath.Join(dir, "stellar-rpc")
	build := exec.CommandContext(context.Background(), "go", "build", "-o", binaryPath,
		"github.com/stellar/stellar-rpc/cmd/stellar-rpc")
	if out, err := build.CombinedOutput(); err != nil {
		errBinaryBuild = fmt.Errorf("go build ./cmd/stellar-rpc: %w\n%s", err, out)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// runBinary runs the built binary and returns its exit code with the
// combined stdout and stderr.
func runBinary(t *testing.T, env []string, args ...string) (int, string) {
	t.Helper()
	require.NoError(t, errBinaryBuild)
	cmd := exec.CommandContext(t.Context(), binaryPath, args...)
	// Drop the mode and config variables so the developer's shell cannot pick
	// the mode or the config file for the test.
	for _, kv := range os.Environ() {
		name, _, _ := strings.Cut(kv, "=")
		if name != EnableRPCv2Env && name != ConfigPathEnv && name != legacyConfigPathEnv {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	cmd.Env = append(cmd.Env, env...)
	out, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	switch {
	case err == nil:
		return 0, string(out)
	case errors.As(err, &exitErr):
		return exitErr.ExitCode(), string(out)
	default:
		t.Fatalf("run %s %v: %v", binaryPath, args, err)
		return -1, ""
	}
}

func TestBinaryVersion(t *testing.T) {
	code, out := runBinary(t, nil, "version")
	assert.Equal(t, 0, code, out)
	assert.True(t, strings.HasPrefix(out, "stellar-rpc "), out)
	assert.Contains(t, out, "mode: rpcv1")

	code, out = runBinary(t, nil, "--enable-rpcv2", "version")
	assert.Equal(t, 0, code, out)
	assert.True(t, strings.HasPrefix(out, "stellar-rpc "), out)
	assert.Contains(t, out, "mode: rpcv2")

	code, out = runBinary(t, []string{EnableRPCv2Env + "=true"}, "version")
	assert.Equal(t, 0, code, out)
	assert.Contains(t, out, "mode: rpcv2")
}

func TestBinaryHelp(t *testing.T) {
	code, out := runBinary(t, nil, "--help")
	assert.Equal(t, 0, code, out)
	assert.Contains(t, out, "--stellar-core-url")
	assert.Contains(t, out, "--enable-rpcv2")
	assert.NotContains(t, out, "--storage.default_data_dir")

	code, out = runBinary(t, nil, "--enable-rpcv2", "--help")
	assert.Equal(t, 0, code, out)
	assert.Contains(t, out, "--storage.default_data_dir")
	assert.Contains(t, out, "--config-path")
	assert.NotContains(t, out, "--stellar-core-url")
}

func TestBinaryRefusesRPCv2ConfigInRPCv1Mode(t *testing.T) {
	path := writeFile(t, "v2.toml", "[storage]\ndefault_data_dir = \"/tmp/x\"\n")

	code, out := runBinary(t, nil, "--config-path", path)
	assert.Equal(t, 1, code, out)
	assert.Contains(t, out, "rpcv2 config")
	assert.Contains(t, out, "--enable-rpcv2")
	assert.Contains(t, out, "[storage]")
	assert.NotContains(t, out, "starting Stellar RPC", "the daemon must not start")

	code, out = runBinary(t, []string{ConfigPathEnv + "=" + path})
	assert.Equal(t, 1, code, out)
	assert.Contains(t, out, "rpcv2 config")
}

func TestBinaryRPCv2RequiresConfigFile(t *testing.T) {
	code, out := runBinary(t, nil, "--enable-rpcv2")
	assert.Equal(t, 1, code, out)
	assert.Contains(t, out, "--config-path")
	assert.Contains(t, out, ConfigPathEnv)
}
