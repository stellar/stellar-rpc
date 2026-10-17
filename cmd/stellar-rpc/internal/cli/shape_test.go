package cli

import (
	"reflect"
	"strings"
	"testing"

	"github.com/pelletier/go-toml"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v2config "github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/config"
)

func TestRPCv2TablesMatchTheConfigStruct(t *testing.T) {
	var fromStruct []string
	for field := range reflect.TypeFor[v2config.Config]().Fields() {
		tag, _, _ := strings.Cut(field.Tag.Get("toml"), ",")
		require.NotEmpty(t, tag, "field %s has no toml tag", field.Name)
		fromStruct = append(fromStruct, tag)
	}
	assert.Equal(t, fromStruct, rpcv2Tables())
}

func TestRPCv2TableIn(t *testing.T) {
	for _, tc := range []struct {
		name    string
		content string
		want    string
	}{
		{name: "empty file", content: "", want: ""},
		{name: "rpcv1 keys", content: "NETWORK_PASSPHRASE = \"x\"\nBACKFILL = true\n", want: ""},
		{name: "rpcv1 lowercase table", content: "[datastore_config]\ntype = \"GCS\"\n", want: ""},
		{name: "rpcv2 table", content: "[backfill]\nworkers = 4\n", want: "backfill"},
		{name: "rpcv2 nested table only", content: "[service.methods.getLedgers]\nqueue_limit = 1\n", want: "service"},
		{name: "rpcv2 key without a table header", content: "logging.level = \"info\"\n", want: "logging"},
		{name: "uppercase is not an rpcv2 table", content: "[SERVICE]\nendpoint = \"x\"\n", want: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tree, err := toml.Load(tc.content)
			require.NoError(t, err)
			assert.Equal(t, tc.want, rpcv2TableIn(tree))
		})
	}
}
