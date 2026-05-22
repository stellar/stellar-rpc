package main

import (
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
)

// coldAdapter wraps *ledger.ColdReader so cold-ledgers can pass
// an `iterateRange`-capable type into rangeWorkload's closure. The
// adapter is the only surviving piece of the previous tier-dispatch
// abstraction — query benches now split per-tier (cold-tx-*, hot-tx-*)
// and don't need a polymorphic reader.
type coldAdapter struct{ *ledger.ColdReader }

func (c coldAdapter) iterateRange(start, end uint32, fn func(uint32, []byte) bool) error {
	for entry, err := range c.IterateLedgers(start, end) {
		if err != nil {
			return err
		}
		if !fn(entry.Seq, entry.Bytes) {
			return nil
		}
	}
	return nil
}
