// =============================================================================
// pkg/cf/cf.go - Column Family Routing and Helpers
// =============================================================================
//
// This package provides column family routing and helper functions.
// Column families partition data by the first hex character of the txHash.
//
// =============================================================================

package cf

// =============================================================================
// Column Family Names
// =============================================================================

// Names contains the names of all 16 column families.
// Each column family is named by the hex character it handles (0-9, a-f).
//
// PARTITIONING SCHEME:
//
//	txHash[0] >> 4 gives the CF index (0-15)
//	This distributes data roughly evenly (assuming uniform hash distribution)
var Names = []string{
	"0", "1", "2", "3", "4", "5", "6", "7",
	"8", "9", "a", "b", "c", "d", "e", "f",
}

// Count is the number of column families.
const Count = 16

// =============================================================================
// Column Family Routing
// =============================================================================

// GetIndex returns the column family index (0-15) for a transaction hash.
func GetIndex(txHash []byte) int {
	if len(txHash) < 1 {
		return 0
	}
	return int(txHash[0] >> 4)
}

// GetName returns the column family name for a transaction hash.
func GetName(txHash []byte) string {
	idx := GetIndex(txHash)
	if idx < 0 || idx > 15 {
		return "0"
	}
	return Names[idx]
}

// GetNameByIndex returns the column family name for a given index.
func GetNameByIndex(idx int) string {
	if idx < 0 || idx >= len(Names) {
		return "0"
	}
	return Names[idx]
}

// GetIndexByName returns the column family index for a given name.
// Returns -1 if the name is not a valid column family name.
func GetIndexByName(name string) int {
	for i, cfName := range Names {
		if cfName == name {
			return i
		}
	}
	return -1
}

// IsValidName returns true if the given string is a valid column family name.
func IsValidName(name string) bool {
	return GetIndexByName(name) >= 0
}
