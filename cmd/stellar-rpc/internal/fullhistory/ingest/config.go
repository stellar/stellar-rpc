package ingest

import "errors"

// Config selects which data types the ingest drivers write. At least one of
// Ledgers/Txhash/Events must be enabled. Per-ledger hot fan-out is always
// parallel; that is not configurable.
//
// The view-based event path derives payloads from the LedgerCloseMetaView and
// needs no network passphrase, so Config carries no passphrase.
type Config struct {
	Ledgers bool
	Txhash  bool
	Events  bool
}

// validate rejects a Config with no enabled data types.
func (c Config) validate() error {
	if !c.Ledgers && !c.Txhash && !c.Events {
		return errors.New("ingest: Config enables no data types (set at least one of Ledgers/Txhash/Events)")
	}
	return nil
}
