//nolint:forcetypeassert // this file uses several unchecked assertions
package config

import (
	"fmt"
	"net"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// AddFlags Init adds the CLI flags to the command. This lets the command output the
// flags as part of the --help output.
func (cfg *Config) AddFlags(cmd *cobra.Command) error {
	cfg.flagset = cmd.PersistentFlags()
	for _, option := range cfg.options() {
		if err := option.AddFlag(cfg.flagset); err != nil {
			return err
		}
	}
	return nil
}

// AddFlag adds a CLI flag for this option to the given flagset.
//
//nolint:funlen,cyclop
func (o *Option) AddFlag(flagset *pflag.FlagSet) error {
	// config options that has no names do not represent a valid flag.
	if len(o.Name) == 0 {
		return nil
	}
	// Treat any option with a custom parser as a string option.
	if o.CustomSetValue != nil {
		if o.DefaultValue == nil {
			o.DefaultValue = ""
		}
		flagset.String(o.Name, fmt.Sprint(o.DefaultValue), o.UsageText())
		o.flag = flagset.Lookup(o.Name)
		return nil
	}

	// Infer the type of the flag based on the type of the ConfigKey. This list
	// of options is based on the available flag types from pflags
	switch o.ConfigKey.(type) {
	case *bool:
		flagset.Bool(o.Name, o.DefaultValue.(bool), o.UsageText())
	case *time.Duration:
		flagset.Duration(o.Name, o.DefaultValue.(time.Duration), o.UsageText())
	case *float32:
		flagset.Float32(o.Name, o.DefaultValue.(float32), o.UsageText())
	case *float64:
		flagset.Float64(o.Name, o.DefaultValue.(float64), o.UsageText())
	case *net.IP:
		flagset.IP(o.Name, o.DefaultValue.(net.IP), o.UsageText())
	case *net.IPNet:
		flagset.IPNet(o.Name, o.DefaultValue.(net.IPNet), o.UsageText())
	case *int:
		flagset.Int(o.Name, o.DefaultValue.(int), o.UsageText())
	case *int8:
		flagset.Int8(o.Name, o.DefaultValue.(int8), o.UsageText())
	case *int16:
		flagset.Int16(o.Name, o.DefaultValue.(int16), o.UsageText())
	case *int32:
		flagset.Int32(o.Name, o.DefaultValue.(int32), o.UsageText())
	case *int64:
		flagset.Int64(o.Name, o.DefaultValue.(int64), o.UsageText())
	case *[]int:
		flagset.IntSlice(o.Name, o.DefaultValue.([]int), o.UsageText())
	case *[]int32:
		flagset.Int32Slice(o.Name, o.DefaultValue.([]int32), o.UsageText())
	case *[]int64:
		flagset.Int64Slice(o.Name, o.DefaultValue.([]int64), o.UsageText())
	case *string:
		// Set an empty string if no default was provided, since some value is always required for pflags
		if o.DefaultValue == nil {
			o.DefaultValue = ""
		}
		flagset.String(o.Name, o.DefaultValue.(string), o.UsageText())
	case *[]string:
		// Set an empty string if no default was provided, since some value is always required for pflags
		if o.DefaultValue == nil {
			o.DefaultValue = []string{}
		}
		flagset.StringSlice(o.Name, o.DefaultValue.([]string), o.UsageText())
	case *uint:
		flagset.Uint(o.Name, o.DefaultValue.(uint), o.UsageText())
	case *uint8:
		flagset.Uint8(o.Name, o.DefaultValue.(uint8), o.UsageText())
	case *uint16:
		flagset.Uint16(o.Name, o.DefaultValue.(uint16), o.UsageText())
	case *uint32:
		flagset.Uint32(o.Name, o.DefaultValue.(uint32), o.UsageText())
	case *uint64:
		flagset.Uint64(o.Name, o.DefaultValue.(uint64), o.UsageText())
	case *[]uint:
		flagset.UintSlice(o.Name, o.DefaultValue.([]uint), o.UsageText())
	default:
		return fmt.Errorf("unexpected option type: %T", o.ConfigKey)
	}

	o.flag = flagset.Lookup(o.Name)
	return nil
}

//nolint:cyclop
func (o *Option) GetFlag(flagset *pflag.FlagSet) (any, error) {
	// Treat any option with a custom parser as a string option.
	if o.CustomSetValue != nil {
		return flagset.GetString(o.Name)
	}

	// Infer the type of the flag based on the type of the ConfigKey. This list
	// of options is based on the available flag types from pflags, and must
	// match the above in `AddFlag`.
	switch o.ConfigKey.(type) {
	case *bool:
		return flagset.GetBool(o.Name)
	case *time.Duration:
		return flagset.GetDuration(o.Name)
	case *float32:
		return flagset.GetFloat32(o.Name)
	case *float64:
		return flagset.GetFloat64(o.Name)
	case *net.IP:
		return flagset.GetIP(o.Name)
	case *net.IPNet:
		return flagset.GetIPNet(o.Name)
	case *int:
		return flagset.GetInt(o.Name)
	case *int8:
		return flagset.GetInt8(o.Name)
	case *int16:
		return flagset.GetInt16(o.Name)
	case *int32:
		return flagset.GetInt32(o.Name)
	case *int64:
		return flagset.GetInt64(o.Name)
	case *[]int:
		return flagset.GetIntSlice(o.Name)
	case *[]int32:
		return flagset.GetInt32Slice(o.Name)
	case *[]int64:
		return flagset.GetInt64Slice(o.Name)
	case *string:
		return flagset.GetString(o.Name)
	case *[]string:
		return flagset.GetStringSlice(o.Name)
	case *uint:
		return flagset.GetUint(o.Name)
	case *uint8:
		return flagset.GetUint8(o.Name)
	case *uint16:
		return flagset.GetUint16(o.Name)
	case *uint32:
		return flagset.GetUint32(o.Name)
	case *uint64:
		return flagset.GetUint64(o.Name)
	case *[]uint:
		return flagset.GetUintSlice(o.Name)
	default:
		return nil, fmt.Errorf("unexpected option type: %T", o.ConfigKey)
	}
}

// UsageText returns the string to use for the usage text of the option. The
// string returned will be the Usage defined on the Option, along with
// the environment variable.
func (o *Option) UsageText() string {
	envVar, hasEnvVar := o.getEnvKey()
	if hasEnvVar {
		return fmt.Sprintf("%s (%s)", o.Usage, envVar)
	}
	return o.Usage
}
