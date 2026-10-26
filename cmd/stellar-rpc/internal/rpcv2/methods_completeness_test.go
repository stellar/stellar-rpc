package rpcv2

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/config"
)

// configMethodNames maps each MethodsConfig method field's Go name to its wire
// name (the toml tag). The two methods-wide default-tier fields are pointers,
// not structs, so the kind check skips them.
func configMethodNames(t *testing.T) map[string]string {
	t.Helper()
	typ := reflect.TypeFor[config.MethodsConfig]()
	names := map[string]string{}
	for field := range typ.Fields() {
		if field.Type.Kind() != reflect.Struct {
			continue
		}
		wireName, _, _ := strings.Cut(field.Tag.Get("toml"), ",")
		require.NotEmpty(t, wireName, "method field %s needs a toml tag", field.Name)
		names[field.Name] = wireName
	}
	return names
}

func TestLimitsByMethod_CoversEveryConfiguredMethod(t *testing.T) {
	limits := limitsByMethod(validCfg(1, 1, "genesis").Service.Methods)

	wireNames := configMethodNames(t)
	for _, name := range wireNames {
		assert.Contains(t, limits, name)
	}
	assert.Len(t, limits, len(wireNames),
		"limitsByMethod has a key with no MethodsConfig field behind it")
}

func TestValidateService_ChecksEveryConfiguredMethod(t *testing.T) {
	for fieldName, wireName := range configMethodNames(t) {
		t.Run(wireName, func(t *testing.T) {
			cfg := validCfg(1, 1, "genesis")
			methodField := reflect.ValueOf(&cfg.Service.Methods).Elem().FieldByName(fieldName)
			zero := uint(0)
			methodField.FieldByName("QueueLimit").Set(reflect.ValueOf(&zero))

			err := validateService(cfg.Service)
			require.Error(t, err,
				"validateService's method list is missing %s", wireName)
			assert.Contains(t, err.Error(), wireName)
		})
	}
}
