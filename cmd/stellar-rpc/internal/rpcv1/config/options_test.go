package config

import (
	"reflect"
	"regexp"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAllConfigKeysMustBePointers(t *testing.T) {
	// This test is to ensure we've set up all the config keys correctly.
	cfg := Config{}
	for _, option := range cfg.options() {
		kind := reflect.ValueOf(option.ConfigKey).Type().Kind()
		if kind != reflect.Pointer {
			t.Errorf("Option.ConfigKey must be a pointer, got %s for %s", kind, option.Name)
		}
	}
}

func TestAllConfigFieldsMustHaveASingleOption(t *testing.T) {
	// This test ensures we've documented all the config options, and not missed
	// any when adding new flags (or accidentally added conflicting duplicates).

	// Allow us to explicitly exclude any fields on the Config struct, which are not going to have Options.
	// e.g. "ConfigPath"
	excluded := map[string]bool{"HistoryArchiveUserAgent": true}

	cfg := Config{}
	cfgValue := reflect.ValueOf(cfg)
	cfgType := cfgValue.Type()

	options := cfg.options()
	optionsByField := map[uintptr]*Option{}
	for _, option := range options {
		key := uintptr(reflect.ValueOf(option.ConfigKey).UnsafePointer())
		if existing, ok := optionsByField[key]; ok {
			t.Errorf("Conflicting ConfigOptions %s and %s, point to the same struct field", existing.Name, option.Name)
		}
		optionsByField[key] = option
	}

	// Get the base address of the struct
	cfgPtr := uintptr(unsafe.Pointer(&cfg))
	for _, structField := range reflect.VisibleFields(cfgType) {
		if excluded[structField.Name] {
			continue
		}
		if !structField.IsExported() {
			continue
		}

		// Each field has an offset within that struct
		fieldPointer := cfgPtr + structField.Offset

		// There should be an option which points to this field
		_, ok := optionsByField[fieldPointer]
		if !ok {
			t.Errorf("Missing Option for field Config.%s", structField.Name)
		}
	}
}

// Use this regex to validate all our config toml keys.
// This is based on the simple bare key regex at: https://toml.io/en/v1.0.0#keys
// Toml, actually allows much more complex keys, via quoted keys, but we want
// to keep things simple.
//
// The one exception we make is `.` in keys, which allows us to have nested
// objects.
var keyRegex = regexp.MustCompile(`^[.A-Za-z0-9_-]+$`)

func TestAllOptionsMustHaveAUniqueValidTomlKey(t *testing.T) {
	// This test ensures we've set a toml key for all the config options, and the
	// keys are all unique & valid. Note, we don't need to check that all struct
	// fields on the config have an option, because the test above checks that.

	// Allow us to explicitly exclude any fields on the Config struct, which are
	// not going to be in the toml. This should be the "Name" field of the
	// Option we wish to exclude.
	excluded := map[string]bool{
		"config-path": true,
	}

	cfg := Config{}
	options := cfg.options()
	optionsByTomlKey := map[string]any{}
	for _, option := range options {
		key, ok := option.getTomlKey()
		if excluded[option.Name] {
			if ok {
				t.Errorf("Found unexpected toml key for excluded Option %s. Does the test need updating?", option.Name)
			}
			continue
		}
		if !ok {
			t.Errorf("Missing toml key for Option %s", option.Name)
		}
		if existing, ok := optionsByTomlKey[key]; ok {
			t.Errorf("Conflicting ConfigOptions %s and %s, have the same toml key: %s", existing, option.Name, key)
		}
		optionsByTomlKey[key] = option.Name

		// Ensure the keys are simple valid toml keys
		assert.True(t, keyRegex.MatchString(key), "Invalid toml key for Option %s: %s", option.Name, key)
	}
}

// A zero execution budget is a footgun the limiter cannot report: it arms no
// limit timer, hands the handler an already-expired context, and then waits on
// the handler with no deadline at all. v2 rejects it; v1 accepted it until
// these options gained a validator.
func TestExecutionDurationOptionsRejectZero(t *testing.T) {
	cfg := Config{}
	var checked int
	for _, option := range cfg.options() {
		if option.Validate == nil {
			continue
		}
		target, ok := option.ConfigKey.(*time.Duration)
		if !ok {
			continue
		}
		checked++
		*target = 0
		require.Error(t, option.Validate(option), "%s accepted a zero duration", option.Name)
		*target = time.Second
		require.NoError(t, option.Validate(option), "%s rejected a positive duration", option.Name)
	}
	assert.Equal(t, 13, checked, "the execution-duration options lost their validator")
}
