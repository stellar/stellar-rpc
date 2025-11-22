package config

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/spf13/pflag"

	"github.com/stellar/go/support/strutils"
)

// Options is a group of Options that can be for convenience
// initialized and set at the same time.
type Options []*Option

// Validate all the config options.
func (options Options) Validate() error {
	var missingOptions []missingRequiredOptionError
	for _, option := range options {
		if option.Validate != nil {
			err := option.Validate(option)
			if err == nil {
				continue
			}
			var missingOptionErr missingRequiredOptionError
			if ok := errors.As(err, &missingOptionErr); ok {
				missingOptions = append(missingOptions, missingOptionErr)
				continue
			}
			return fmt.Errorf("invalid config value for %s: %w", option.Name, err)
		}
	}
	if len(missingOptions) > 0 {
		// we had one or more missing options, combine these all into a single error.
		errString := "The following required configuration parameters are missing:"
		for _, missingOpt := range missingOptions {
			errString += "\n*\t" + missingOpt.strErr
			errString += "\n \t" + missingOpt.usage
		}
		return &missingRequiredOptionError{strErr: errString}
	}
	return nil
}

// Option is a complete description of the configuration of a command line option
type Option struct {
	// e.g. "database-url"
	Name string
	// e.g. "DATABASE_URL".Defaults to uppercase/underscore representation of name
	EnvVar string
	// e.g. "DATABASE_URL". Defaults to uppercase/underscore representation of name. - to omit from toml
	TomlKey string
	// Help text
	Usage string
	// A default if no option is provided. Omit or set to `nil` if no default
	DefaultValue any
	// Pointer to the final key in the linked Config struct
	ConfigKey any
	// Optional function for custom validation/transformation
	CustomSetValue func(*Option, any) error
	// Function called after loading all options, to validate the configuration
	Validate    func(*Option) error
	MarshalTOML func(*Option) (any, error)

	flag *pflag.Flag // The persistent flag that the config option is attached to
}

// Returns false if this option is omitted in the toml
func (o Option) getTomlKey() (string, bool) {
	if o.TomlKey == "-" || o.TomlKey == "_" {
		return "", false
	}
	if o.TomlKey != "" {
		return o.TomlKey, true
	}
	if envVar, ok := o.getEnvKey(); ok {
		return envVar, true
	}
	return strutils.KebabToConstantCase(o.Name), true
}

// Returns false if this option is omitted in the env
func (o Option) getEnvKey() (string, bool) {
	if o.EnvVar == "-" || o.EnvVar == "_" {
		return "", false
	}
	if o.EnvVar != "" {
		return o.EnvVar, true
	}
	return strutils.KebabToConstantCase(o.Name), true
}

// TODO: See if we can remove CustomSetValue into just SetValue/ParseValue
func (o *Option) setValue(i any) (err error) {
	if o.CustomSetValue != nil {
		return o.CustomSetValue(o, i)
	}
	defer func() {
		if recoverRes := recover(); recoverRes != nil {
			var ok bool
			if err, ok = recoverRes.(error); ok {
				return
			}

			err = fmt.Errorf("config option setting error ('%s') %v", o.Name, recoverRes)
		}
	}()
	parser := func(_ *Option, _ any) error {
		return fmt.Errorf("no parser for flag %s", o.Name)
	}
	switch o.ConfigKey.(type) {
	case *bool:
		parser = parseBool
	case *int, *int8, *int16, *int32, *int64:
		parser = parseInt
	case *uint, *uint8, *uint16, *uint32:
		parser = parseUint32
	case *uint64:
		parser = parseUint
	case *float32, *float64:
		parser = parseFloat
	case *string:
		parser = parseString
	case *[]string:
		parser = parseStringSlice
	case *time.Duration:
		parser = parseDuration
	}

	return parser(o, i)
}

func (o *Option) marshalTOML() (any, error) {
	if o.MarshalTOML != nil {
		return o.MarshalTOML(o)
	}
	// go-toml doesn't handle ints other than `int`, so we have to do that ourselves.
	switch v := o.ConfigKey.(type) {
	case *int, *int8, *int16, *int32, *int64:
		return []byte(strconv.FormatInt(reflect.ValueOf(v).Elem().Int(), 10)), nil
	case *uint, *uint8, *uint16, *uint32, *uint64:
		return []byte(strconv.FormatUint(reflect.ValueOf(v).Elem().Uint(), 10)), nil
	case *time.Duration:
		return v.String(), nil
	default:
		// Unknown, hopefully go-toml knows what to do with it! :crossed_fingers:
		return reflect.ValueOf(o.ConfigKey).Elem().Interface(), nil
	}
}
