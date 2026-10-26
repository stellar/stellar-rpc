package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExcludeFromBench(t *testing.T) {
	const (
		zstd     = "github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/zstd"
		grocksdb = "github.com/linxGnu/grocksdb"
	)
	for _, tc := range []struct {
		name string
		pkg  string
		deps []string
		want bool
	}{
		{name: "rpcv2", pkg: zstd, want: true},
		{name: "rpcv2 child", pkg: zstd + "/internal", want: true},
		{name: "native dependency", pkg: "example.com/pkg", deps: []string{grocksdb}, want: true},
		{name: "rpcv1", pkg: "github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1"},
		{name: "similar prefix", pkg: "github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2extra"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, excludeFromBench(tc.pkg, tc.deps))
		})
	}
}
