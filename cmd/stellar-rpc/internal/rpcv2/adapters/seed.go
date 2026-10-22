package adapters

import (
	"errors"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
)

// SeedCloseTimes stamps the close times of both servable-window edges on the
// registry before serving begins — OpenRegistry publishes the latest ledger with
// no close time (the catalog records sequences, not timestamps) and the oldest
// stamp starts empty. One point read per edge here, once, is what keeps every
// served request off that read: the fallbacks in getLedgerRange stay only as
// backstops for the boot window and for the read after the retention floor
// moves. No-op on an empty catalog (nothing committed yet).
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
		registry.SetLatestLedger(latest, query.CloseTimeAt(lastCT))
	}
	return errors.Join(errs...)
}
