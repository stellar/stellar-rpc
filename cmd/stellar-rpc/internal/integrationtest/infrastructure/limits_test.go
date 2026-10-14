package infrastructure

import (
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveLimitFile(t *testing.T) {
	fsys := fstest.MapFS{}
	for _, name := range []string{
		"enable.xdr",
		"unlimited.p23.xdr",
		"unlimited.p28.xdr",
		"unlimited.p30.xdr",
		"unlimited.p100.xdr",
		"unlimited.p28.json",
		"testnet.p25.xdr",
		"testnet-like.p27.xdr",
	} {
		fsys[upgradesDir+"/"+name] = &fstest.MapFile{}
	}

	for _, tc := range []struct {
		name     string
		version  int32
		expected string
	}{
		{"unlimited", 28, "unlimited.p28.xdr"},   // exact match
		{"unlimited", 29, "unlimited.p28.xdr"},   // falls back to older file
		{"unlimited", 30, "unlimited.p30.xdr"},   // newer file wins once reached
		{"unlimited", 99, "unlimited.p30.xdr"},   // p100 is numerically, not lexically, greater
		{"unlimited", 100, "unlimited.p100.xdr"}, // multi-digit versions
		{"testnet", 29, "testnet.p25.xdr"},       // prefix of another name is not confused
	} {
		got, err := resolveLimitFile(fsys, tc.name, tc.version)
		require.NoError(t, err)
		assert.Equal(t, tc.expected, got, "%s at p%d", tc.name, tc.version)
	}

	_, err := resolveLimitFile(fsys, "unlimited", 22)
	require.ErrorContains(t, err, "N <= 22")
	_, err = resolveLimitFile(fsys, "missing", 29)
	require.Error(t, err)
}

func TestResolveLimitFileEmbedded(t *testing.T) {
	for _, name := range []string{"unlimited", "testnet"} {
		got, err := resolveLimitFile(upgradeFiles, name, MaxSupportedProtocolVersion)
		require.NoError(t, err)
		_, err = upgradeFiles.ReadFile(upgradesDir + "/" + got)
		require.NoError(t, err)
	}
}
