package cli

import (
	"fmt"
	"strconv"
	"strings"
)

// Mode is the daemon the stellar-rpc binary runs.
type Mode int

const (
	// ModeRPCv1 is the SQLite daemon under internal/rpcv1. It is the default.
	ModeRPCv1 Mode = iota + 1
	// ModeRPCv2 is the full-history RocksDB daemon under internal/rpcv2.
	ModeRPCv2
)

func (m Mode) String() string {
	switch m {
	case ModeRPCv1:
		return "rpcv1"
	case ModeRPCv2:
		return "rpcv2"
	default:
		return fmt.Sprintf("Mode(%d)", int(m))
	}
}

// howToSwitch tells an operator how to select the other mode.
func (m Mode) howToSwitch() string {
	if m == ModeRPCv2 {
		return "pass --enable-rpcv2=false to run rpcv1"
	}
	return "pass --enable-rpcv2 to run rpcv2"
}

const (
	// EnableRPCv2Flag selects rpcv2. It is a flag and an environment variable,
	// never a TOML key, because the mode decides which parser reads the file.
	EnableRPCv2Flag = "enable-rpcv2"
	EnableRPCv2Env  = "STELLAR_RPC_ENABLE_RPCV2"

	// ConfigPathFlag names the config file in both modes.
	ConfigPathFlag = "config-path"
	ConfigPathEnv  = "STELLAR_RPC_CONFIG_PATH"
	// legacyConfigPathEnv is the rpcv1 name for ConfigPathEnv. It still works
	// in rpcv1 mode so no existing rpcv1 deployment breaks.
	legacyConfigPathEnv = "SOROBAN_RPC_CONFIG_PATH"
)

// SelectMode reads the mode from the raw arguments and the environment before
// any command is built. A flag wins over the environment variable, and the
// last flag wins. Arguments after "--" are not flags.
func SelectMode(args []string, lookupEnv func(string) (string, bool)) (Mode, error) {
	mode := ModeRPCv1
	if value, ok := lookupEnv(EnableRPCv2Env); ok && value != "" {
		enabled, err := strconv.ParseBool(value)
		if err != nil {
			return 0, fmt.Errorf("%s=%q: want true or false", EnableRPCv2Env, value)
		}
		mode = modeFor(enabled)
	}
	for _, arg := range args {
		if arg == "--" {
			break
		}
		if arg == "--"+EnableRPCv2Flag {
			mode = ModeRPCv2
			continue
		}
		value, ok := strings.CutPrefix(arg, "--"+EnableRPCv2Flag+"=")
		if !ok {
			continue
		}
		enabled, err := strconv.ParseBool(value)
		if err != nil {
			return 0, fmt.Errorf("--%s=%q: want true or false", EnableRPCv2Flag, value)
		}
		mode = modeFor(enabled)
	}
	return mode, nil
}

func modeFor(rpcv2 bool) Mode {
	if rpcv2 {
		return ModeRPCv2
	}
	return ModeRPCv1
}
