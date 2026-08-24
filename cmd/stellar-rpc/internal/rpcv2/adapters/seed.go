package adapters

import (
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
)

// SeedCloseTimes stamps the close times of both servable-window edges on the
// registry before serving begins — OpenRegistry seeds the latest ledger with
// close time 0 (the catalog has no close times), and the oldest cache starts
// empty. One point read per edge here spares the first requests those reads;
// the fallbacks in getLedgerRange stay as backstops. No-op on an empty
// catalog (nothing committed yet).
func SeedCloseTimes(registry *query.Registry) error {
	view, err := registry.NewReadView()
	if err != nil {
		return err
	}
	defer view.Release()
	oldest, latest := view.OldestLedger(), view.LatestLedger()
	if oldest > latest {
		return nil
	}
	firstCT, err := readCloseTime(view, oldest, "oldest")
	if err != nil {
		return err
	}
	view.RecordOldestCloseTime(firstCT)
	lastCT, err := readCloseTime(view, latest, "latest")
	if err != nil {
		return err
	}
	registry.SetLatestLedger(latest, lastCT)
	return nil
}
