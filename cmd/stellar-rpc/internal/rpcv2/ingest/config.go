package ingest

import (
	"errors"
)

// Config selects which data types the ingest drivers write — for either
// materializer (WriteColdChunk's raw walk or FreezeColdChunk's CF scans; the
// hot-DB-vs-stream choice is the ENTRY POINT, not a config knob). At least
// one of Ledgers/Txhash/Events must be enabled.
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
