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

// validate rejects a Config with no enabled data types or a negative
// encode-workers count.
func (c Config) validate() error {
	if !c.Ledgers && !c.Txhash && !c.Events {
		return errors.New("ingest: Config enables no data types (set at least one of Ledgers/Txhash/Events)")
	}
	if c.ZstdEncodeWorkers < 0 {
		return errors.New("ingest: Config.ZstdEncodeWorkers must be >= 0 (0 = single-threaded)")
	}
	return nil
}
