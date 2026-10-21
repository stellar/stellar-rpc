package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseFailedPkgsReportsScannerErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bench.out")
	require.NoError(t, os.WriteFile(path, []byte(strings.Repeat("x", 1024*1024+1)), 0o600))

	failed, err := parseFailedPkgs(path)
	require.Error(t, err)
	require.Empty(t, failed)
}
