package backfill

// =============================================================================
// Column Family (CF) Routing
// =============================================================================
//
// Transaction hashes are partitioned into 16 column families (CFs) based on the
// high nibble (top 4 bits) of the first byte of the txhash. This provides
// uniform distribution for parallel RecSplit index building — each CF gets
// approximately 1/16th of all transaction hashes.
//
// Routing formula: CF index = txHash[0] >> 4
//
// Examples:
//   txHash[0] = 0x00 → CF "0" (index 0)
//   txHash[0] = 0x3F → CF "3" (index 3)
//   txHash[0] = 0xAB → CF "a" (index 10)
//   txHash[0] = 0xFF → CF "f" (index 15)

const (
	// CFCount is the number of column families (hash nibble partitions).
	// Each CF gets its own RecSplit index file (cf-{nibble}.idx).
	CFCount = 16
)

// CFNames maps CF index (0-15) to the lowercase hex nibble string.
// Used for constructing file paths like "cf-a.idx" and log messages.
var CFNames = [CFCount]string{
	"0", "1", "2", "3", "4", "5", "6", "7",
	"8", "9", "a", "b", "c", "d", "e", "f",
}

// GetCFIndex returns the CF index (0-15) for a transaction hash.
// The routing is based on the high nibble of the first byte.
//
// This function is called for every transaction hash during both ingestion
// (to partition .bin file reads) and RecSplit building (to filter entries).
func GetCFIndex(txHash []byte) int {
	return int(txHash[0] >> 4)
}

// GetCFName returns the CF name (hex nibble "0"-"f") for a transaction hash.
func GetCFName(txHash []byte) string {
	return CFNames[txHash[0]>>4]
}
