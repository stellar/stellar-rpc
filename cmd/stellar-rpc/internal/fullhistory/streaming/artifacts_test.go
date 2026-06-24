package streaming

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// TestArtifactPaths_EveryKindMapped guards the two sources of truth — the kind
// registry (allKinds, keys.go) and the file mapping (Layout.ArtifactPaths,
// paths.go) — against drift. A kind in allKinds with no ArtifactPaths case
// makes the sweep unlink nothing, delete the key, and orphan the files on disk
// — the one orphan class the key-driven sweep prevents. Adding a kind without
// its path mapping fails HERE at CI rather than orphaning files at runtime.
func TestArtifactPaths_EveryKindMapped(t *testing.T) {
	layout := NewLayout(t.TempDir())
	for _, k := range allKinds {
		assert.NotEmpty(t, layout.ArtifactPaths(chunk.ID(0), k),
			"kind %q is in allKinds but ArtifactPaths returns no path — the sweep would orphan its files", k)
	}
}
