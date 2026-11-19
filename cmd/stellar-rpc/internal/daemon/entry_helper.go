package daemon

import (
	"encoding/json"
	"fmt"
)

// Exported LedgerEntryDeserializer helper (#919 Fix)
type LedgerEntryDeserializer struct{}

func (d *LedgerEntryDeserializer) DeserializeEntry(rawB64 string) (map[string]interface{}, error) {
	if rawB64 == "" {
		return nil, fmt.Errorf("empty raw ledger entry base64")
	}
	var res map[string]interface{}
	if err := json.Unmarshal([]byte(rawB64), &res); err != nil {
		return nil, fmt.Errorf("failed to deserialize entry: %w", err)
	}
	return res, nil
}
