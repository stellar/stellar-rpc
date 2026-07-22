package config

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"
)

func parseBool(option *Option, i interface{}) error {
	switch v := i.(type) {
	case nil:
		return nil
	case bool:
		//nolint:forcetypeassert
		*option.ConfigKey.(*bool) = v
	case string:
		lower := strings.ToLower(v)
		b, err := strconv.ParseBool(lower)
		if err != nil {
			return fmt.Errorf("invalid boolean value %s: %s", option.Name, v)
		}
		//nolint:forcetypeassert
		*option.ConfigKey.(*bool) = b
	default:
		return fmt.Errorf("could not parse boolean %s: %v", option.Name, i)
	}
	return nil
}

func parseInt(option *Option, i interface{}) error {
	switch v := i.(type) {
	case nil:
		return nil
	case string:
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return err
		}
		reflect.ValueOf(option.ConfigKey).Elem().SetInt(parsed)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return parseInt(option, fmt.Sprint(v))
	default:
		return fmt.Errorf("could not parse int %s: %v", option.Name, i)
	}
	return nil
}

func parseUint(option *Option, i interface{}) error {
	switch v := i.(type) {
	case nil:
		return nil
	case string:
		parsed, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return err
		}
		reflect.ValueOf(option.ConfigKey).Elem().SetUint(parsed)
	case int, int8, int16, int32, int64:
		if reflect.ValueOf(v).Int() < 0 {
			return fmt.Errorf("%s cannot be negative", option.Name)
		}
		return parseUint(option, fmt.Sprint(v))
	case uint, uint8, uint16, uint32, uint64:
		return parseUint(option, fmt.Sprint(v))
	default:
		return fmt.Errorf("could not parse uint %s: %v", option.Name, i)
	}
	return nil
}

func parseFloat(option *Option, i interface{}) error {
	switch v := i.(type) {
	case nil:
		return nil
	case string:
		parsed, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return err
		}
		reflect.ValueOf(option.ConfigKey).Elem().SetFloat(parsed)
	case uint, uint8, uint16, uint32, uint64, int, int8, int16, int32, int64, float32, float64:
		return parseFloat(option, fmt.Sprint(v))
	default:
		return fmt.Errorf("could not parse float %s: %v", option.Name, i)
	}
	return nil
}

func parseString(option *Option, i interface{}) error {
	switch v := i.(type) {
	case nil:
		return nil
	case string:
		if strPtr, ok := option.ConfigKey.(*string); ok {
			*strPtr = v
		} else {
			return fmt.Errorf("invalid type for %s: expected *string", option.Name)
		}
	default:
		return fmt.Errorf("could not parse string %s: %v", option.Name, i)
	}
	return nil
}

func parseUint32(option *Option, i interface{}) error {
	switch v := i.(type) {
	case nil:
		return nil
	case string:
		parsed, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return err
		}
		if parsed > math.MaxUint32 {
			return fmt.Errorf("%s overflows uint32", option.Name)
		}
		reflect.ValueOf(option.ConfigKey).Elem().SetUint(parsed)
	case int, int8, int16, int32, int64:
		if reflect.ValueOf(v).Int() < 0 {
			return fmt.Errorf("%s cannot be negative", option.Name)
		}
		return parseUint32(option, fmt.Sprint(v))
	case uint, uint8, uint16, uint32, uint64:
		return parseUint32(option, fmt.Sprint(v))
	default:
		return fmt.Errorf("could not parse uint32 %s: %v", option.Name, i)
	}
	return nil
}

func parseDuration(option *Option, i interface{}) error {
	durationPtr, ok := option.ConfigKey.(*time.Duration)
	if !ok {
		return fmt.Errorf("invalid type for %s: expected *time.Duration", option.Name)
	}

	switch v := i.(type) {
	case nil:
		return nil
	case string:
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("could not parse duration: %q: %w", v, err)
		}
		*durationPtr = d
	case time.Duration:
		*durationPtr = v
	case *time.Duration:
		*durationPtr = *v
	default:
		return fmt.Errorf("%s is not a duration", option.Name)
	}
	return nil
}

func parseStringSlice(option *Option, i interface{}) error {
	stringSlicePtr, ok := option.ConfigKey.(*[]string)
	if !ok {
		return fmt.Errorf("invalid type for %s: expected *[]string", option.Name)
	}

	switch v := i.(type) {
	case nil:
		return nil
	case string:
		if v == "" {
			*stringSlicePtr = nil
		} else {
			*stringSlicePtr = strings.Split(v, ",")
		}
	case []string:
		*stringSlicePtr = v
	case []interface{}:
		result := make([]string, len(v))
		for i, s := range v {
			str, ok := s.(string)
			if !ok {
				return fmt.Errorf("could not parse %s: element %d is not a string", option.Name, i)
			}
			result[i] = str
		}
		*stringSlicePtr = result
	default:
		return fmt.Errorf("could not parse %s: %v", option.Name, v)
	}
	return nil
}
