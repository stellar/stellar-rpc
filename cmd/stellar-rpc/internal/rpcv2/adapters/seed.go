package adapters

import (
	"errors"

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
	// The edges live in independent stores (cold pack vs hot DB), so attempt
	// both even when one fails: the joined error shows every broken edge in
	// the caller's one failure instead of one edge per restart.
	var errs []error
	if firstCT, err := readCloseTime(view, oldest, "oldest"); err != nil {
		errs = append(errs, err)
	} else {
		view.RecordOldestCloseTime(firstCT)
	}
	if lastCT, err := readCloseTime(view, latest, "latest"); err != nil {
		errs = append(errs, err)
	} else {
		registry.SetLatestLedger(latest, lastCT)
	}
	return errors.Join(errs...)
}
