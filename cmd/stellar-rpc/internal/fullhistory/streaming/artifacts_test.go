package streaming

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// TestArtifactPaths_EveryKindMapped guards the two separate sources of truth —
// the kind registry (allKinds, keys.go) and the file mapping
// (Layout.ArtifactPaths, paths.go) — against silent drift. Once a kind is in
// allKinds, parseChunkKey accepts it and ChunkArtifactKeys returns refs for it,
// so SweepChunkArtifacts calls ArtifactPaths(chunk, kind) on it. If that kind
// has no ArtifactPaths case, the default returns nil: the sweep unlinks nothing,
// deletes the key, and leaves the artifact files on disk with no catalog key —
// the one orphan class the key-driven sweep is designed to prevent. Adding a
// kind without its path mapping fails HERE, at CI, rather than orphaning files
// at runtime. (This test naturally extends as later slices add kinds, since it
// iterates allKinds.)
func TestArtifactPaths_EveryKindMapped(t *testing.T) {
	layout := NewLayout(t.TempDir())
	for _, k := range allKinds {
		assert.NotEmpty(t, layout.ArtifactPaths(chunk.ID(0), k),
			"kind %q is in allKinds but ArtifactPaths returns no path — the sweep would orphan its files", k)
	}
}
