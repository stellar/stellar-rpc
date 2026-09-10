package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// These checks read workflow text only. They never execute infrastructure steps.
func TestLegSeedingContract(t *testing.T) {
	root, err := filepath.Abs(".")
	require.NoError(t, err)
	for {
		if _, err := os.Stat(filepath.Join(root, "go.mod")); err == nil {
			break
		}
		parent := filepath.Dir(root)
		require.NotEqual(t, root, parent, "repository root not found")
		root = parent
	}
	data, err := os.ReadFile(filepath.Join(root, ".github/workflows/ec2-leg.yml"))
	require.NoError(t, err)
	workflow := string(data)
	previous := -1
	for _, step := range []string{
		"Checkout target ref", "Configure AWS via OIDC", "Seed the pending result marker",
		"Render user-data", "Launch EC2 instance", "Poll for results",
	} {
		index := strings.Index(workflow, "- name: "+step)
		require.Greater(t, index, previous, step)
		previous = index
	}
	seedStart := strings.Index(workflow, "- name: Seed the pending result marker")
	seedEnd := strings.Index(workflow, "- name: Render user-data")
	seed := workflow[seedStart:seedEnd]
	require.Contains(t, seed, "if: ${{ hashFiles('")
	require.Contains(t, seed, "/harness/poller.go') != '' }}")
	require.Contains(t, seed, "RUN_ID: ${{ github.run_id }}-${{ github.run_attempt }}")
	require.Contains(t, seed, `verdict: "pending"`)
	require.Contains(t, seed, "--key \"$RESULT_KEY\"")
	require.Contains(t, workflow, "RUN_ID=${{ github.run_id }}-${{ github.run_attempt }}")
	require.Contains(t, workflow, "RESULT_KEY: runs/${{ github.run_id }}/${{ inputs.run_label }}/result.json")
	require.Contains(t, workflow, `steps.results.outputs.found }}" != "true"`)
	require.Contains(t, workflow, `steps.results.outputs.passed }}" != "true"`)
	require.Contains(t, workflow, `"$KEEP_INSTANCE" = "true" ] && [ "$LEG_PASSED" = "true"`)
	require.Contains(t, workflow, `-n "$ADOPT_RUN_LABEL" ] && [ "$LEG_PASSED" = "true"`)
}
