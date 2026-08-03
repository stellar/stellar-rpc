package ingest

import (
	"errors"
	"fmt"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
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

	// TxhashSecret is the resolved per-index secret that blinds the txhash .bin
	// keys — the caller derives it (txhash.ColdIndexSecret) so ingest keys match
	// the index build. Required when Txhash is set — no unkeyed fallback.
	TxhashSecret []byte

	// EventsSecret is the resolved per-chunk secret that keys the events cold
	// index's routing — the caller derives it (event.ColdIndexSecret) so the
	// build is deterministic. Required when Events is set — no unkeyed fallback.
	EventsSecret []byte

	// ZstdEncodeWorkers is the walk materializer's ledger-frame encode
	// parallelism (0 = single-threaded). FORMAT-AFFECTING: it must equal the
	// hot tier's hotchunk.Tuning value, because the freeze copies hot frames
	// into the cold pack verbatim while the walk re-encodes the same ledgers
	// — the same chunk's pack must be byte-identical whichever materializer
	// built it (the freeze-vs-walk identity gates arbitrate). One resolved
	// daemon-config value (storage.zstd_encode_workers) feeds both. Ignored
	// by FreezeColdChunk (verbatim copy, no encoder).
	ZstdEncodeWorkers int
}

// validate rejects a Config with no enabled data types, a txhash/events
// config missing its cold-index secret, or a negative encode-workers count.
func (c Config) validate() error {
	if !c.Ledgers && !c.Txhash && !c.Events {
		return errors.New("ingest: Config enables no data types (set at least one of Ledgers/Txhash/Events)")
	}
	if c.Txhash && len(c.TxhashSecret) != stores.SecretLen {
		return fmt.Errorf("ingest: Txhash enabled but TxhashSecret is %d bytes, want %d (per-index secret required)",
			len(c.TxhashSecret), stores.SecretLen)
	}
	if c.Events && len(c.EventsSecret) != stores.SecretLen {
		return fmt.Errorf("ingest: Events enabled but EventsSecret is %d bytes, want %d (per-index secret required)",
			len(c.EventsSecret), stores.SecretLen)
	}
	// An all-zero secret is the right length but disables blinding: an attacker
	// can reproduce every blinded key and route them into one block. Reject it
	// so a caller that forgot to derive a secret fails loudly, not silently.
	if c.Txhash && isZeroSecret(c.TxhashSecret) {
		return errors.New("ingest: TxhashSecret is all zero (blinding disabled — derive a per-index secret)")
	}
	if c.Events && isZeroSecret(c.EventsSecret) {
		return errors.New("ingest: EventsSecret is all zero (blinding disabled — derive a per-index secret)")
	}
	if c.ZstdEncodeWorkers < 0 {
		return errors.New("ingest: Config.ZstdEncodeWorkers must be >= 0 (0 = single-threaded)")
	}
	return nil
}

// isZeroSecret reports whether b is all zero.
func isZeroSecret(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}
