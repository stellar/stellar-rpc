package ingest

import (
	"errors"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/hotchunk"
)

// Config selects which data types the ingest drivers write. At least one of
// Ledgers/Txhash/Events must be enabled.
//
// The view-based event path derives payloads from the LedgerCloseMetaView and
// needs no network passphrase, so Config carries no passphrase.
type Config struct {
	Ledgers bool
	Txhash  bool
	Events  bool

	// EventsFreezeDB, when non-nil, switches the events writer to
	// freeze-by-merge: cold events artifacts are built directly from this
	// (complete, read-only) hot chunk DB's CFs at finalize instead of from
	// the raw-ledger walk — no per-ledger events shaping, no term memory.
	// Only the freeze path sets it (its source IS this hot DB, so the CFs
	// cover exactly the chunk); backfill leaves it nil. Ignored unless
	// Events is enabled.
	EventsFreezeDB *hotchunk.DB
}

// validate rejects a Config with no enabled data types.
func (c Config) validate() error {
	if !c.Ledgers && !c.Txhash && !c.Events {
		return errors.New("ingest: Config enables no data types (set at least one of Ledgers/Txhash/Events)")
	}
	return nil
}
