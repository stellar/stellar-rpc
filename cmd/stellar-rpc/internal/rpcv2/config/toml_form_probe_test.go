package config

import "testing"

// Probe: how does the strict decoder treat a BARE INTEGER earliest_ledger
// (the doc types the key "uint32 | genesis | now")?
func TestProbe_BareIntegerEarliestLedger(t *testing.T) {
	_, err := ParseConfig([]byte("[retention]\nearliest_ledger = 20002\n"))
	t.Logf("bare integer: err=%v", err)

	cfg, err2 := ParseConfig([]byte("[retention]\nearliest_ledger = \"20002\"\n"))
	t.Logf("quoted string: val=%q err=%v", cfg.Retention.EarliestLedger, err2)
}
