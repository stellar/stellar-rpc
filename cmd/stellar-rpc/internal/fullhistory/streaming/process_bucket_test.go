package streaming

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// A freeze must fsync the grandparent dirent for any bucket directory it
// creates — otherwise a crash can orphan a "frozen" pack whose bucket dirent was
// never made durable. The "this freeze creates the dir" signal must be real dir
// existence, NOT chunkID % ChunksPerBucket: under frontfill-at-tip or
// out-of-order backfill/recovery, the first chunk materialized in a bucket need
// not be the one whose id is a multiple of ChunksPerBucket.
func TestCreatedParentDirs_FlagsNewBucketRegardlessOfChunkNumber(t *testing.T) {
	root := t.TempDir()
	layout := NewLayout(root)

	// chunk 1500 is in bucket 1 (1500/1000) but 1500 % 1000 == 500 != 0, so the
	// old arithmetic gate would NOT have fsynced the grandparent — yet 1500 can be
	// the first chunk materialized in bucket 00001.
	const midBucket = chunk.ID(1500)
	require.NotEqual(t, uint32(0), uint32(midBucket)%chunk.ChunksPerBucket,
		"precondition: 1500 is not a bucket-aligned chunk")

	bucketDir := filepath.Dir(layout.LedgerPackPath(midBucket))

	// Bucket dir absent → the freeze's MkdirAll will create it → must be flagged.
	created := createdParentDirs(layout, midBucket, AllKinds())
	require.True(t, created[bucketDir],
		"absent bucket dir must be flagged new so its grandparent dirent is fsynced")

	// Once the dir exists, a later chunk in the same bucket must NOT re-flag it.
	require.NoError(t, os.MkdirAll(bucketDir, 0o755))
	created = createdParentDirs(layout, midBucket, AllKinds())
	require.False(t, created[bucketDir],
		"existing bucket dir must not be flagged new")
}
