package ingest

import "errors"

// Config selects which data types the ingest drivers write. At least one of
// Ledgers/Txhash/Events must be enabled. Decode is always parsed and per-ledger
// fan-out is always parallel; those are not configurable.
//
// Passphrase is the network passphrase used to derive event payloads from a
// LedgerCloseMeta. It is required iff Events is enabled.
type Config struct {
	Ledgers bool
	Txhash  bool
	Events  bool

	Passphrase string
}

// validate rejects a Config with no enabled data types, or one that enables
// Events without supplying a network passphrase.
func (c Config) validate() error {
	if !c.Ledgers && !c.Txhash && !c.Events {
		return errors.New("ingest: Config enables no data types (set at least one of Ledgers/Txhash/Events)")
	}
	if c.Events && c.Passphrase == "" {
		return errors.New("ingest: Config.Passphrase is required when Events is enabled")
	}
	return nil
}

// needsLCM reports whether the driver must decode each ledger's raw bytes into
// an xdr.LedgerCloseMeta. The ledgers ingesters consume raw bytes verbatim, so
// the decode is only needed when events or txhash are enabled.
func (c Config) needsLCM() bool { return c.Events || c.Txhash }
