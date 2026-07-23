package ledger

import (
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
)

// PackName returns the cold ledger packfile filename for chunkID
// (`<chunkID:08d>.pack`). Bucket-directory composition
// ({bucketID:05d}/) is the orchestrator's job — this package owns the
// per-chunk filename so the writer side (cold ingest) and the future
// cold-ledger read path share one definition, mirroring the
// eventstore cold-format split (EventsPackName and friends).
func PackName(chunkID chunk.ID) string {
	return chunkID.String() + ".pack"
}
