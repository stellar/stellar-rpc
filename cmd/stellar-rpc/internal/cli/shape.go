package cli

import (
	"fmt"

	"github.com/pelletier/go-toml"
)

// rpcv2Tables are the top-level tables of a rpcv2 config file, in the order
// of the fields of the rpcv2 Config struct. A reflection test pins them.
//
// The match is case-sensitive on purpose. rpcv1 has the key BACKFILL and
// rpcv2 has the table [backfill]. Casing alone cannot decide the shape
// either, because rpcv1 also has lowercase tables such as [datastore_config].
func rpcv2Tables() []string {
	return []string{"service", "retention", "storage", "backfill", "ingestion", "logging"}
}

// refuseRPCv2File returns an error when the TOML file at path has a rpcv2
// table. An empty path means no file and is fine.
func refuseRPCv2File(path string) error {
	if path == "" {
		return nil
	}
	tree, err := toml.LoadFile(path)
	if err != nil {
		return err
	}
	if table := rpcv2TableIn(tree); table != "" {
		return fmt.Errorf("the config file %s is a rpcv2 config (it has a [%s] table); pass --%s to run rpcv2",
			path, table, EnableRPCv2Flag)
	}
	return nil
}

// rpcv2TableIn returns the first rpcv2 table found among the top-level keys
// of a loaded TOML tree, or "" when it has none. rpcv1 ignores unknown keys
// unless strict parsing is on, so without this check a rpcv2 file starts
// rpcv1 with every value at its default.
func rpcv2TableIn(tree *toml.Tree) string {
	for _, table := range rpcv2Tables() {
		if tree.HasPath([]string{table}) {
			return table
		}
	}
	return ""
}
