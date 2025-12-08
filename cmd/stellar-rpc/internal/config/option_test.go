package config

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigOptionGetTomlKey(t *testing.T) {
	// Explicitly set toml key
	key, ok := Option{TomlKey: "TOML_KEY"}.getTomlKey()
	assert.Equal(t, "TOML_KEY", key)
	assert.True(t, ok)

	// Explicitly disabled toml key via `-`
	key, ok = Option{TomlKey: "-"}.getTomlKey()
	assert.Equal(t, "", key)
	assert.False(t, ok)

	// Explicitly disabled toml key via `_`
	key, ok = Option{TomlKey: "_"}.getTomlKey()
	assert.Equal(t, "", key)
	assert.False(t, ok)

	// Fallback to env var
	key, ok = Option{EnvVar: "ENV_VAR"}.getTomlKey()
	assert.Equal(t, "ENV_VAR", key)
	assert.True(t, ok)

	// Env-var disabled, autogenerate from name
	key, ok = Option{Name: "test-flag", EnvVar: "-"}.getTomlKey()
	assert.Equal(t, "TEST_FLAG", key)
	assert.True(t, ok)

	// Env-var not set, autogenerate from name
	key, ok = Option{Name: "test-flag"}.getTomlKey()
	assert.Equal(t, "TEST_FLAG", key)
	assert.True(t, ok)
}

func TestValidateRequired(t *testing.T) {
	var strVal string
	o := &Option{
		Name:      "required-option",
		ConfigKey: &strVal,
		Validate:  required,
	}

	// unset
	require.ErrorContains(t, o.Validate(o), "required-option is required")

	// set with blank value
	require.NoError(t, o.setValue(""))
	require.ErrorContains(t, o.Validate(o), "required-option is required")

	// set with valid value
	require.NoError(t, o.setValue("not-blank"))
	require.NoError(t, o.Validate(o))
}

func TestValidatePositiveUint32(t *testing.T) {
	var val uint32
	o := &Option{
		Name:      "positive-option",
		ConfigKey: &val,
		Validate:  positive,
	}

	// unset
	require.ErrorContains(t, o.Validate(o), "positive-option must be positive")

	// set with 0 value
	require.NoError(t, o.setValue(uint32(0)))
	require.ErrorContains(t, o.Validate(o), "positive-option must be positive")

	// set with valid value
	require.NoError(t, o.setValue(uint32(1)))
	require.NoError(t, o.Validate(o))
}

func TestValidatePositiveInt(t *testing.T) {
	var val int
	o := &Option{
		Name:      "positive-option",
		ConfigKey: &val,
		Validate:  positive,
	}

	// unset
	require.ErrorContains(t, o.Validate(o), "positive-option must be positive")

	// set with 0 value
	require.NoError(t, o.setValue(0))
	require.ErrorContains(t, o.Validate(o), "positive-option must be positive")

	// set with negative value
	require.NoError(t, o.setValue(-1))
	require.ErrorContains(t, o.Validate(o), "positive-option must be positive")

	// set with valid value
	require.NoError(t, o.setValue(1))
	require.NoError(t, o.Validate(o))
}

func TestUnassignableField(t *testing.T) {
	var co Option
	var b bool
	co.Name = "mykey"
	co.ConfigKey = &b
	err := co.setValue("abc")
	require.Error(t, err)
	require.Contains(t, err.Error(), co.Name)
}

func TestNoParserForFlag(t *testing.T) {
	var co Option
	var invalidKey []time.Duration
	co.Name = "mykey"
	co.ConfigKey = &invalidKey
	err := co.setValue("abc")
	require.Error(t, err)
	require.Contains(t, err.Error(), "no parser for flag mykey")
}

func TestSetValueBool(t *testing.T) {
	var b bool
	testCases := []struct {
		name  string
		value any
		err   string
	}{
		{"valid-bool", true, ""},
		{"valid-bool-string", "true", ""},
		{"valid-bool-string-false", "false", ""},
		{"valid-bool-string-uppercase", "TRUE", ""},
		{"invalid-bool-string", "foobar", "invalid boolean value invalid-bool-string: foobar"},
	}
	runTestCases(t, &b, testCases)
}

func TestSetValueInt(t *testing.T) {
	var i int
	testCases := []struct {
		name  string
		value any
		err   string
	}{
		{"valid-int", 1, ""},
		{"valid-int-string", "1", ""},
		{"invalid-int-string", "abcd", "strconv.ParseInt: parsing \"abcd\": invalid syntax"},
	}
	runTestCases(t, &i, testCases)
}

func TestSetValueUint32(t *testing.T) {
	var u32 uint32
	testCases := []struct {
		name  string
		value any
		err   string
	}{
		{"valid-uint32", 1, ""},
		{"overflow-uint32", uint64(math.MaxUint32) + 1, "overflow-uint32 overflows uint32"},
		{"negative-uint32", -1, "negative-uint32 cannot be negative"},
	}
	runTestCases(t, &u32, testCases)
}

func TestSetValueUint64(t *testing.T) {
	var u64 uint64
	testCases := []struct {
		name  string
		value any
		err   string
	}{
		{"valid-uint", 1, ""},
		{"negative-uint", -1, "negative-uint cannot be negative"},
	}
	runTestCases(t, &u64, testCases)
}

func TestSetValueFloat64(t *testing.T) {
	var f64 float64
	testCases := []struct {
		name  string
		value any
		err   string
	}{
		{"valid-float", 1.05, ""},
		{"valid-float-int", int64(1234), ""},
		{"valid-float-string", "1.05", ""},
		{"invalid-float-string", "foobar", "strconv.ParseFloat: parsing \"foobar\": invalid syntax"},
	}
	runTestCases(t, &f64, testCases)
}

func TestSetValueString(t *testing.T) {
	var s string
	testCases := []struct {
		name  string
		value any
		err   string
	}{
		{"valid-string", "foobar", ""},
	}
	runTestCases(t, &s, testCases)
}

func runTestCases(t *testing.T, key any, testCases []struct {
	name  string
	value any
	err   string
},
) {
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			co := Option{
				Name:      tc.name,
				ConfigKey: key,
			}
			err := co.setValue(tc.value)
			if tc.err != "" {
				require.EqualError(t, err, tc.err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
