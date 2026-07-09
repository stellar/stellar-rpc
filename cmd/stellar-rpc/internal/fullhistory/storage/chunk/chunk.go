// Package chunk provides chunk ID math for the full-history
// pipeline.
//
// A Chunk is a fixed-size, contiguous range of ledgers and is the
// atomic unit of full-history processing. Chunk IDs are exposed as a
// distinct type to prevent accidentally mixing them up with ledger
// sequence numbers — both are uint32 and would otherwise silently
// interchange at call sites.
//
// Subsystem-specific concerns (column-family names, on-disk layout,
// hot/cold lifecycle) live in each subsystem package under
// fullhistory/storage/stores/ — so adding a new subsystem doesn't grow
// this package.
package chunk

import "fmt"

const (
	// LedgersPerChunk is the fixed number of ledgers per Chunk.
	// Typed uint32 so arithmetic against ledger sequences (uint32)
	// stays in the right space without per-call casts.
	LedgersPerChunk uint32 = 10_000

	// FirstLedgerSeq is the first ledger sequence used by the chain.
	FirstLedgerSeq = 2

	// ChunksPerBucket is the on-disk subdirectory grouping for
	// chunk-level files: bucket_id = chunk_id / ChunksPerBucket.
	ChunksPerBucket = 1000
)

// ID is a typed chunk identifier.
type ID uint32

// IDFromLedger returns the Chunk that contains the given ledger.
//
// Panics if ledgerSeq is below FirstLedgerSeq (genesis). The uint32
// subtraction would otherwise wrap to a huge value and divide to a
// garbage chunk ID — silent miscalibration. Callers receive ledger
// sequences from the chain (always ≥ FirstLedgerSeq); a sub-genesis
// input is an upstream programmer error worth surfacing loudly.
func IDFromLedger(ledgerSeq uint32) ID {
	if ledgerSeq < FirstLedgerSeq {
		panic(fmt.Sprintf("chunk: IDFromLedger called with ledgerSeq %d < FirstLedgerSeq %d",
			ledgerSeq, FirstLedgerSeq))
	}
	return ID((ledgerSeq - FirstLedgerSeq) / LedgersPerChunk)
}

// FirstLedger is the first ledger sequence in this Chunk.
func (c ID) FirstLedger() uint32 {
	return uint32(c)*LedgersPerChunk + FirstLedgerSeq
}

// LastLedger is the last ledger sequence in this Chunk.
//
// Valid for chunk IDs in [0, MaxUint32/LedgersPerChunk - 1]
// (≈429,496 chunks ≈ 4.3 billion ledgers — well past realistic
// deployments). The (c+1)*LedgersPerChunk arithmetic overflows
// past that bound.
func (c ID) LastLedger() uint32 {
	return (uint32(c)+1)*LedgersPerChunk + FirstLedgerSeq - 1
}

// String formats the chunk ID as a zero-padded 8-digit decimal.
func (c ID) String() string {
	return fmt.Sprintf("%08d", uint32(c))
}

// BucketID is the zero-padded 5-digit subdirectory name that groups
// chunk-level files on disk.
func (c ID) BucketID() string {
	return fmt.Sprintf("%05d", uint32(c)/ChunksPerBucket)
}
