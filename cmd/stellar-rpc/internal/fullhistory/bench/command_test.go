package bench

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

// TestNewCommand builds the full command tree — executing every markRequired
// call, whose panic on a bad flag name this test exists to catch (main.go
// calls NewCommand unconditionally at startup) — and pins each subcommand's
// required flags.
func TestNewCommand(t *testing.T) {
	cmd := NewCommand()
	require.Equal(t, "bench-ingest", cmd.Use)

	requiredBySubcommand := map[string][]string{
		"cold": {"types", "chunk", "cold-out-dir"},
		"hot":  {"chunk", "hot-dir"},
	}
	subs := make(map[string]*cobra.Command, len(cmd.Commands()))
	for _, sub := range cmd.Commands() {
		subs[sub.Use] = sub
	}
	for name, flags := range requiredBySubcommand {
		sub := subs[name]
		require.NotNil(t, sub, "subcommand %q missing", name)
		for _, fn := range flags {
			f := sub.Flags().Lookup(fn)
			require.NotNil(t, f, "%s: flag --%s missing", name, fn)
			require.Contains(t, f.Annotations, cobra.BashCompOneRequiredFlag,
				"%s: flag --%s not marked required", name, fn)
		}
	}
}
