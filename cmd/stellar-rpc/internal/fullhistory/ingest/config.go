package ingest

import "errors"

// Config selects which data types the ingest drivers write. At least one of
// Ledgers/Txhash/Events must be enabled. Each ledger commits to the per-chunk
// hot DB as one atomic synced WriteBatch across the enabled CFs (decision (a)) —
// there is no per-data-type fan-out.
//
// Txhash and Events are forward-declared here but only honored where a slice has
// wired their cold/hot ingesters (Events in slice 2, Txhash in slice 3);
// selecting an unwired type is accepted by validate but writes nothing.
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
